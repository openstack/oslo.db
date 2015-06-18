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

"""Unit tests for DB API."""

import mock
from oslo_config import cfg
from oslo_utils import importutils

from oslo.db import api
from oslo.db import exception
from oslo_db.tests.old_import_api import utils as test_utils

sqla = importutils.try_import('sqlalchemy')
if not sqla:
    raise ImportError("Unable to import module 'sqlalchemy'.")


def get_backend():
    return DBAPI()


class DBAPI(object):
    def _api_raise(self, exception_to_raise, *args, **kwargs):
        """Simulate raising a database error

        This method creates a fake OperationalError with an ID matching
        a valid MySQL "database has gone away" situation. It also decrements
        the error_counter so that we can artificially keep track of
        how many times this function is called by the wrapper. When
        error_counter reaches zero, this function returns True, simulating
        the query succeeding.
        """

        if self.error_counter > 0:
            self.error_counter -= 1
            orig = sqla.exc.DBAPIError(False, False, False)
            orig.args = [2006, 'Test raise operational error']
            exception_type = type(exception_to_raise)
            e = exception_type(orig)
            raise e
        else:
            return True

    def api_raise_conn_err_default(self, *args, **kwargs):
        return self._api_raise(exception.DBConnectionError(), *args, **kwargs)

    @api.safe_for_db_retry
    def api_raise_conn_err_enable_retry(self, *args, **kwargs):
        return self._api_raise(exception.DBConnectionError(), *args, **kwargs)

    def api_raise_deadlock_err_default(self, *args, **kwargs):
        return self._api_raise(exception.DBDeadlock(), *args, **kwargs)

    @api.retry_on_deadlock
    def api_raise_deadlock_err_decorated(self, *args, **kwargs):
        return self._api_raise(exception.DBDeadlock(), *args, **kwargs)

    @api.safe_for_db_retry
    def api_raise_deadlock_safe_db_retry_decorated(self, *args, **kwargs):
        return self._api_raise(exception.DBDeadlock(), *args, **kwargs)

    @api.safe_for_db_retry
    @api.retry_on_deadlock
    def api_raise_deadlock_err_two_decorators(self, *args, **kwargs):
        if self.error_counter > 2:
            return False
        if self.error_counter == 2:
            self.error_counter -= 1
            orig = sqla.exc.DBAPIError(False, False, False)
            orig.args = [2006, 'Test raise operational error']
            raise exception.DBConnectionError(orig)
        if self.error_counter == 1:
            self.error_counter -= 1
            raise exception.DBDeadlock()
        else:
            return True

    def api_class_call1(_self, *args, **kwargs):
        return args, kwargs


class DBAPITestCase(test_utils.BaseTestCase):
    def test_dbapi_full_path_module_method(self):
        dbapi = api.DBAPI('oslo_db.tests.test_api')
        result = dbapi.api_class_call1(1, 2, kwarg1='meow')
        expected = ((1, 2), {'kwarg1': 'meow'})
        self.assertEqual(expected, result)

    def test_dbapi_unknown_invalid_backend(self):
        self.assertRaises(ImportError, api.DBAPI, 'tests.unit.db.not_existent')

    def test_dbapi_lazy_loading(self):
        dbapi = api.DBAPI('oslo_db.tests.test_api', lazy=True)

        self.assertIsNone(dbapi._backend)
        dbapi.api_class_call1(1, 'abc')
        self.assertIsNotNone(dbapi._backend)

    def test_dbapi_from_config(self):
        conf = cfg.ConfigOpts()

        dbapi = api.DBAPI.from_config(conf,
                                      backend_mapping={'sqlalchemy': __name__})
        self.assertIsNotNone(dbapi._backend)


class DBReconnectTestCase(DBAPITestCase):
    def setUp(self):
        super(DBReconnectTestCase, self).setUp()

        self.test_db_api = DBAPI()
        patcher = mock.patch(__name__ + '.get_backend',
                             return_value=self.test_db_api)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_raise_connection_error(self):
        self.dbapi = api.DBAPI('sqlalchemy', {'sqlalchemy': __name__})

        self.test_db_api.error_counter = 5
        self.assertRaises(exception.DBConnectionError,
                          self.dbapi.api_raise_conn_err_default)

    def test_raise_connection_error_decorated(self):
        self.dbapi = api.DBAPI('sqlalchemy', {'sqlalchemy': __name__})

        self.test_db_api.error_counter = 5
        self.assertRaises(exception.DBConnectionError,
                          self.dbapi.api_raise_conn_err_enable_retry)
        self.assertEqual(4, self.test_db_api.error_counter, 'Unexpected retry')

    def test_raise_connection_error_enabled(self):
        self.dbapi = api.DBAPI('sqlalchemy',
                               {'sqlalchemy': __name__},
                               use_db_reconnect=True)

        self.test_db_api.error_counter = 5
        self.assertRaises(exception.DBConnectionError,
                          self.dbapi.api_raise_conn_err_default)
        self.assertEqual(4, self.test_db_api.error_counter, 'Unexpected retry')

    def test_retry_one(self):
        self.dbapi = api.DBAPI('sqlalchemy',
                               {'sqlalchemy': __name__},
                               use_db_reconnect=True,
                               retry_interval=1)

        func = self.dbapi.api_raise_conn_err_enable_retry
        self.test_db_api.error_counter = 1
        self.assertTrue(func(), 'Single retry did not succeed.')

        self.assertEqual(
            0, self.test_db_api.error_counter,
            'Counter not decremented, retry logic probably failed.')

    def test_retry_two(self):
        self.dbapi = api.DBAPI('sqlalchemy',
                               {'sqlalchemy': __name__},
                               use_db_reconnect=True,
                               retry_interval=1,
                               inc_retry_interval=False)

        func = self.dbapi.api_raise_conn_err_enable_retry
        self.test_db_api.error_counter = 2
        self.assertTrue(func(), 'Multiple retry did not succeed.')

        self.assertEqual(
            0, self.test_db_api.error_counter,
            'Counter not decremented, retry logic probably failed.')

    def test_retry_until_failure(self):
        self.dbapi = api.DBAPI('sqlalchemy',
                               {'sqlalchemy': __name__},
                               use_db_reconnect=True,
                               retry_interval=1,
                               inc_retry_interval=False,
                               max_retries=3)

        func = self.dbapi.api_raise_conn_err_enable_retry
        self.test_db_api.error_counter = 5
        self.assertRaises(
            exception.DBError, func,
            'Retry of permanent failure did not throw DBError exception.')

        self.assertNotEqual(
            0, self.test_db_api.error_counter,
            'Retry did not stop after sql_max_retries iterations.')


class DBDeadlockTestCase(DBAPITestCase):
    def setUp(self):
        super(DBDeadlockTestCase, self).setUp()

        self.test_db_api = DBAPI()
        patcher = mock.patch(__name__ + '.get_backend',
                             return_value=self.test_db_api)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_raise_deadlock_error(self):
        self.dbapi = api.DBAPI('sqlalchemy', {'sqlalchemy': __name__})

        self.test_db_api.error_counter = 5
        self.assertRaises(
            exception.DBDeadlock,
            self.dbapi.api_raise_deadlock_err_default)

    def test_raise_deadlock_error_db_reconnect_enabled(self):
        self.dbapi = api.DBAPI('sqlalchemy',
                               {'sqlalchemy': __name__},
                               use_db_reconnect=True)

        self.test_db_api.error_counter = 5
        self.assertRaises(exception.DBDeadlock,
                          self.dbapi.api_raise_deadlock_err_default)
        self.assertEqual(4, self.test_db_api.error_counter, 'Unexpected retry')

    def test_raise_deadlock_error_connection_error_decorated(self):
        self.dbapi = api.DBAPI('sqlalchemy',
                               {'sqlalchemy': __name__},
                               use_db_reconnect=True)

        self.test_db_api.error_counter = 5
        self.assertRaises(
            exception.DBDeadlock,
            self.dbapi.api_raise_deadlock_safe_db_retry_decorated)

    def test_retry_one(self):
        self.dbapi = api.DBAPI('sqlalchemy',
                               {'sqlalchemy': __name__},
                               retry_interval=1)

        func = self.dbapi.api_raise_deadlock_err_decorated
        self.test_db_api.error_counter = 1
        self.assertTrue(func(), 'Single retry did not succeed.')

        self.assertEqual(
            0, self.test_db_api.error_counter,
            'Counter not decremented, retry logic probably failed.')

    def test_retry_two(self):
        self.dbapi = api.DBAPI('sqlalchemy',
                               {'sqlalchemy': __name__},
                               retry_interval=1,
                               inc_retry_interval=False)

        func = self.dbapi.api_raise_deadlock_err_decorated
        self.test_db_api.error_counter = 2
        self.assertTrue(func(), 'Multiple retry did not succeed.')

        self.assertEqual(
            0, self.test_db_api.error_counter,
            'Counter not decremented, retry logic probably failed.')

    def test_retry_two_different_exception(self):
        self.dbapi = api.DBAPI('sqlalchemy',
                               {'sqlalchemy': __name__},
                               use_db_reconnect=True,
                               retry_interval=1,
                               inc_retry_interval=False)

        func = self.dbapi.api_raise_deadlock_err_two_decorators
        self.test_db_api.error_counter = 2
        self.assertTrue(func(), 'Multiple retry did not succeed.')

        self.assertEqual(
            0, self.test_db_api.error_counter,
            'Counter not decremented, retry logic probably failed.')


class DBRetryRequestCase(DBAPITestCase):
    def test_retry_wrapper_succeeds(self):
        @api.wrap_db_retry(max_retries=10, retry_on_request=True)
        def some_method():
            pass

        some_method()

    def test_retry_wrapper_reaches_limit(self):
        max_retries = 10

        @api.wrap_db_retry(max_retries=10, retry_on_request=True)
        def some_method(res):
            res['result'] += 1
            raise exception.RetryRequest(ValueError())

        res = {'result': 0}
        self.assertRaises(ValueError, some_method, res)
        self.assertEqual(max_retries + 1, res['result'])
