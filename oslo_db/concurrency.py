# Copyright 2014 Mirantis.inc
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

import copy
import logging
import threading

from debtcollector import removals
from oslo_config import cfg

from oslo_db import api

LOG = logging.getLogger(__name__)

tpool_opts = [
    cfg.BoolOpt(
        'use_tpool',
        default=False,
        deprecated_name='dbapi_use_tpool',
        deprecated_group='DEFAULT',
        deprecated_for_removal=True,
        deprecated_since='10.0.0',
        deprecated_reason=(
            'This feature has never graduated from experimental status and is '
            'now being removed due to lack of maintenance and test coverage'
        ),
        help=(
            'Enable the experimental use of thread pooling for '
            'all DB API calls'
        ),
    ),
]

_removed_msg = (
    'Thread pool support in oslo_db is deprecated; you should use '
    'oslo_db.api.DBAPI.from_config directly'
)


@removals.removed_class(
    'TpoolDbapiWrapper', message=_removed_msg, version='10.0.0')
class TpoolDbapiWrapper(object):
    """DB API wrapper class.

    This wraps the oslo DB API with an option to be able to use eventlet's
    thread pooling. Since the CONF variable may not be loaded at the time
    this class is instantiated, we must look at it on the first DB API call.
    """

    def __init__(self, conf, backend_mapping):
        self._db_api = None
        self._backend_mapping = backend_mapping
        self._conf = conf
        self._conf.register_opts(tpool_opts, 'database')
        self._lock = threading.Lock()

    @property
    def _api(self):
        if not self._db_api:
            with self._lock:
                if not self._db_api:
                    db_api = api.DBAPI.from_config(
                        conf=self._conf, backend_mapping=self._backend_mapping)
                    if self._conf.database.use_tpool:
                        try:
                            from eventlet import tpool
                        except ImportError:
                            LOG.exception("'eventlet' is required for "
                                          "TpoolDbapiWrapper.")
                            raise
                        self._db_api = tpool.Proxy(db_api)
                    else:
                        self._db_api = db_api
        return self._db_api

    def __getattr__(self, key):
        return getattr(self._api, key)


def list_opts():
    """Returns a list of oslo.config options available in this module.

    :returns: a list of (group_name, opts) tuples
    """
    return [('database', copy.deepcopy(tpool_opts))]
