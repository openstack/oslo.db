# Copyright (c) 2013 Rackspace Hosting
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""
=================================
Multiple DB API backend support.
=================================

A DB backend module should implement a method named 'get_backend' which
takes no arguments. The method can return any object that implements DB
API methods.
"""

import logging
import threading
import time

from oslo_utils import excutils
from oslo_utils import importutils
import six

from oslo_db._i18n import _LE
from oslo_db import exception
from oslo_db import options


LOG = logging.getLogger(__name__)


def safe_for_db_retry(f):
    """Indicate api method as safe for re-connection to database.

    Database connection retries will be enabled for the decorated api method.
    Database connection failure can have many causes, which can be temporary.
    In such cases retry may increase the likelihood of connection.

    Usage::

        @safe_for_db_retry
        def api_method(self):
            self.engine.connect()


    :param f: database api method.
    :type f: function.
    """
    f.__dict__['enable_retry_on_disconnect'] = True
    return f


def retry_on_deadlock(f):
    """Retry a DB API call if Deadlock was received.

    wrap_db_entry will be applied to all db.api functions marked with this
    decorator.
    """
    f.__dict__['enable_retry_on_deadlock'] = True
    return f


def retry_on_request(f):
    """Retry a DB API call if RetryRequest exception was received.

    wrap_db_entry will be applied to all db.api functions marked with this
    decorator.
    """
    f.__dict__['enable_retry_on_request'] = True
    return f


class wrap_db_retry(object):
    """Retry db.api methods, if db_error raised

    Retry decorated db.api methods. This decorator catches db_error and retries
    function in a loop until it succeeds, or until maximum retries count
    will be reached.

    Keyword arguments:

    :param retry_interval: seconds between transaction retries
    :type retry_interval: int

    :param max_retries: max number of retries before an error is raised
    :type max_retries: int

    :param inc_retry_interval: determine increase retry interval or not
    :type inc_retry_interval: bool

    :param max_retry_interval: max interval value between retries
    :type max_retry_interval: int

    :param exception_checker: checks if an exception should trigger a retry
    :type exception_checker: callable
    """

    def __init__(self, retry_interval=0, max_retries=0, inc_retry_interval=0,
                 max_retry_interval=0, retry_on_disconnect=False,
                 retry_on_deadlock=False, retry_on_request=False,
                 exception_checker=lambda exc: False):
        super(wrap_db_retry, self).__init__()

        self.db_error = ()
        # default is that we re-raise anything unexpected
        self.exception_checker = exception_checker
        if retry_on_disconnect:
            self.db_error += (exception.DBConnectionError, )
        if retry_on_deadlock:
            self.db_error += (exception.DBDeadlock, )
        if retry_on_request:
            self.db_error += (exception.RetryRequest, )
        self.retry_interval = retry_interval
        self.max_retries = max_retries
        self.inc_retry_interval = inc_retry_interval
        self.max_retry_interval = max_retry_interval

    def __call__(self, f):
        @six.wraps(f)
        def wrapper(*args, **kwargs):
            next_interval = self.retry_interval
            remaining = self.max_retries

            while True:
                try:
                    return f(*args, **kwargs)
                except Exception as e:
                    with excutils.save_and_reraise_exception() as ectxt:
                        if remaining > 0:
                            ectxt.reraise = not self._is_exception_expected(e)
                        else:
                            LOG.exception(_LE('DB exceeded retry limit.'))
                            # if it's a RetryRequest, we need to unpack it
                            if isinstance(e, exception.RetryRequest):
                                ectxt.type_ = type(e.inner_exc)
                                ectxt.value = e.inner_exc
                    # NOTE(vsergeyev): We are using patched time module, so
                    #                  this effectively yields the execution
                    #                  context to another green thread.
                    time.sleep(next_interval)
                    if self.inc_retry_interval:
                        next_interval = min(
                            next_interval * 2,
                            self.max_retry_interval
                        )
                    remaining -= 1

        return wrapper

    def _is_exception_expected(self, exc):
        if isinstance(exc, self.db_error):
            # RetryRequest is application-initated exception
            # and not an error condition in case retries are
            # not exceeded
            if not isinstance(exc, exception.RetryRequest):
                LOG.exception(_LE('DB error.'))
            return True
        return self.exception_checker(exc)


class DBAPI(object):
    """Initialize the chosen DB API backend.

    After initialization API methods is available as normal attributes of
    ``DBAPI`` subclass. Database API methods are supposed to be called as
    DBAPI instance methods.

    :param backend_name: name of the backend to load
    :type backend_name: str

    :param backend_mapping: backend name -> module/class to load mapping
    :type backend_mapping: dict
    :default backend_mapping: None

    :param lazy: load the DB backend lazily on the first DB API method call
    :type lazy: bool
    :default lazy: False

    :keyword use_db_reconnect: retry DB transactions on disconnect or not
    :type use_db_reconnect: bool

    :keyword retry_interval: seconds between transaction retries
    :type retry_interval: int

    :keyword inc_retry_interval: increase retry interval or not
    :type inc_retry_interval: bool

    :keyword max_retry_interval: max interval value between retries
    :type max_retry_interval: int

    :keyword max_retries: max number of retries before an error is raised
    :type max_retries: int
    """

    def __init__(self, backend_name, backend_mapping=None, lazy=False,
                 **kwargs):

        self._backend = None
        self._backend_name = backend_name
        self._backend_mapping = backend_mapping or {}
        self._lock = threading.Lock()

        if not lazy:
            self._load_backend()

        self.use_db_reconnect = kwargs.get('use_db_reconnect', False)
        self.retry_interval = kwargs.get('retry_interval', 1)
        self.inc_retry_interval = kwargs.get('inc_retry_interval', True)
        self.max_retry_interval = kwargs.get('max_retry_interval', 10)
        self.max_retries = kwargs.get('max_retries', 20)

    def _load_backend(self):
        with self._lock:
            if not self._backend:
                # Import the untranslated name if we don't have a mapping
                backend_path = self._backend_mapping.get(self._backend_name,
                                                         self._backend_name)
                LOG.debug('Loading backend %(name)r from %(path)r',
                          {'name': self._backend_name,
                           'path': backend_path})
                backend_mod = importutils.import_module(backend_path)
                self._backend = backend_mod.get_backend()

    def __getattr__(self, key):
        if not self._backend:
            self._load_backend()

        attr = getattr(self._backend, key)
        if not hasattr(attr, '__call__'):
            return attr
        # NOTE(vsergeyev): If `use_db_reconnect` option is set to True, retry
        #                  DB API methods, decorated with @safe_for_db_retry
        #                  on disconnect.
        retry_on_disconnect = self.use_db_reconnect and attr.__dict__.get(
            'enable_retry_on_disconnect', False)
        retry_on_deadlock = attr.__dict__.get('enable_retry_on_deadlock',
                                              False)
        retry_on_request = attr.__dict__.get('enable_retry_on_request', False)

        if retry_on_disconnect or retry_on_deadlock or retry_on_request:
            attr = wrap_db_retry(
                retry_interval=self.retry_interval,
                max_retries=self.max_retries,
                inc_retry_interval=self.inc_retry_interval,
                max_retry_interval=self.max_retry_interval,
                retry_on_disconnect=retry_on_disconnect,
                retry_on_deadlock=retry_on_deadlock,
                retry_on_request=retry_on_request)(attr)

        return attr

    @classmethod
    def from_config(cls, conf, backend_mapping=None, lazy=False):
        """Initialize DBAPI instance given a config instance.

        :param conf: oslo.config config instance
        :type conf: oslo.config.cfg.ConfigOpts

        :param backend_mapping: backend name -> module/class to load mapping
        :type backend_mapping: dict

        :param lazy: load the DB backend lazily on the first DB API method call
        :type lazy: bool

        """

        conf.register_opts(options.database_opts, 'database')

        return cls(backend_name=conf.database.backend,
                   backend_mapping=backend_mapping,
                   lazy=lazy,
                   use_db_reconnect=conf.database.use_db_reconnect,
                   retry_interval=conf.database.db_retry_interval,
                   inc_retry_interval=conf.database.db_inc_retry_interval,
                   max_retry_interval=conf.database.db_max_retry_interval,
                   max_retries=conf.database.db_max_retries)
