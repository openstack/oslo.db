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

from unittest import mock

from oslo_config import cfg
from oslo_utils import importutils

from oslo_db import api
from oslo_db import exception
from oslo_db.tests import utils as test_utils

sqla = importutils.try_import('sqlalchemy')
if not sqla:
    raise ImportError("Unable to import module 'sqlalchemy'.")


def get_backend():
    return DBAPI()


class DBAPI(object):
    def _api_raise(self, *args, **kwargs):
        """Simulate raising a database-has-gone-away error

        This method creates a fake OperationalError with an ID matching
        a valid MySQL "database has gone away" situation. It also decrements
        the error_counter so that we can artificially keep track of
        how many times this function is called by the wrapper. When
        error_counter reaches zero, this function returns True, simulating
        the database becoming available again and the query succeeding.
        """

        if self.error_counter > 0:
            self.error_counter -= 1
            orig = sqla.exc.DBAPIError(False, False, False)
            orig.args = [2006, 'Test raise operational error']
            e = exception.DBConnectionError(orig)
            raise e
        else:
            return True

    def api_raise_default(self, *args, **kwargs):
        return self._api_raise(*args, **kwargs)

    @api.safe_for_db_retry
    def api_raise_enable_retry(self, *args, **kwargs):
        return self._api_raise(*args, **kwargs)

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
        self.assertRaises(exception.DBConnectionError, self.dbapi._api_raise)

    def test_raise_connection_error_decorated(self):
        self.dbapi = api.DBAPI('sqlalchemy', {'sqlalchemy': __name__})

        self.test_db_api.error_counter = 5
        self.assertRaises(exception.DBConnectionError,
                          self.dbapi.api_raise_enable_retry)
        self.assertEqual(4, self.test_db_api.error_counter, 'Unexpected retry')

    def test_raise_connection_error_enabled(self):
        self.dbapi = api.DBAPI('sqlalchemy',
                               {'sqlalchemy': __name__},
                               use_db_reconnect=True)

        self.test_db_api.error_counter = 5
        self.assertRaises(exception.DBConnectionError,
                          self.dbapi.api_raise_default)
        self.assertEqual(4, self.test_db_api.error_counter, 'Unexpected retry')

    @mock.patch('oslo_db.api.time.sleep', return_value=None)
    def test_retry_one(self, p_time_sleep):
        self.dbapi = api.DBAPI('sqlalchemy',
                               {'sqlalchemy': __name__},
                               use_db_reconnect=True,
                               retry_interval=1)

        try:
            func = self.dbapi.api_raise_enable_retry
            self.test_db_api.error_counter = 1
            self.assertTrue(func(), 'Single retry did not succeed.')
        except Exception:
            self.fail('Single retry raised an un-wrapped error.')
        p_time_sleep.assert_called_with(1)
        self.assertEqual(
            0, self.test_db_api.error_counter,
            'Counter not decremented, retry logic probably failed.')

    @mock.patch('oslo_db.api.time.sleep', return_value=None)
    def test_retry_two(self, p_time_sleep):
        self.dbapi = api.DBAPI('sqlalchemy',
                               {'sqlalchemy': __name__},
                               use_db_reconnect=True,
                               retry_interval=1,
                               inc_retry_interval=False)

        try:
            func = self.dbapi.api_raise_enable_retry
            self.test_db_api.error_counter = 2
            self.assertTrue(func(), 'Multiple retry did not succeed.')
        except Exception:
            self.fail('Multiple retry raised an un-wrapped error.')
        p_time_sleep.assert_called_with(1)
        self.assertEqual(
            0, self.test_db_api.error_counter,
            'Counter not decremented, retry logic probably failed.')

    @mock.patch('oslo_db.api.time.sleep', return_value=None)
    def test_retry_float_interval(self, p_time_sleep):
        self.dbapi = api.DBAPI('sqlalchemy',
                               {'sqlalchemy': __name__},
                               use_db_reconnect=True,
                               retry_interval=0.5)
        try:
            func = self.dbapi.api_raise_enable_retry
            self.test_db_api.error_counter = 1
            self.assertTrue(func(), 'Single retry did not succeed.')
        except Exception:
            self.fail('Single retry raised an un-wrapped error.')

        p_time_sleep.assert_called_with(0.5)
        self.assertEqual(
            0, self.test_db_api.error_counter,
            'Counter not decremented, retry logic probably failed.')

    @mock.patch('oslo_db.api.time.sleep', return_value=None)
    def test_retry_until_failure(self, p_time_sleep):
        self.dbapi = api.DBAPI('sqlalchemy',
                               {'sqlalchemy': __name__},
                               use_db_reconnect=True,
                               retry_interval=1,
                               inc_retry_interval=False,
                               max_retries=3)

        func = self.dbapi.api_raise_enable_retry
        self.test_db_api.error_counter = 5
        self.assertRaises(
            exception.DBError, func,
            'Retry of permanent failure did not throw DBError exception.')
        p_time_sleep.assert_called_with(1)
        self.assertNotEqual(
            0, self.test_db_api.error_counter,
            'Retry did not stop after sql_max_retries iterations.')


class DBRetryRequestCase(DBAPITestCase):
    def test_retry_wrapper_succeeds(self):
        @api.wrap_db_retry(max_retries=10)
        def some_method():
            pass

        some_method()

    def test_retry_wrapper_reaches_limit(self):
        max_retries = 2

        @api.wrap_db_retry(max_retries=max_retries)
        def some_method(res):
            res['result'] += 1
            raise exception.RetryRequest(ValueError())

        res = {'result': 0}
        self.assertRaises(ValueError, some_method, res)
        self.assertEqual(max_retries + 1, res['result'])

    def test_retry_wrapper_exception_checker(self):

        def exception_checker(exc):
            return isinstance(exc, ValueError) and exc.args[0] < 5

        @api.wrap_db_retry(max_retries=10,
                           exception_checker=exception_checker)
        def some_method(res):
            res['result'] += 1
            raise ValueError(res['result'])

        res = {'result': 0}
        self.assertRaises(ValueError, some_method, res)
        # our exception checker should have stopped returning True after 5
        self.assertEqual(5, res['result'])

    @mock.patch.object(DBAPI, 'api_class_call1')
    @mock.patch.object(api, 'wrap_db_retry')
    def test_mocked_methods_are_not_wrapped(self, mocked_wrap, mocked_method):
        dbapi = api.DBAPI('oslo_db.tests.test_api')
        dbapi.api_class_call1()

        self.assertFalse(mocked_wrap.called)

    @mock.patch('oslo_db.api.LOG')
    def test_retry_wrapper_non_db_error_not_logged(self, mock_log):
        # Tests that if the retry wrapper hits a non-db error (raised from the
        # wrapped function), then that exception is reraised but not logged.

        @api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
        def some_method():
            raise AttributeError('test')

        self.assertRaises(AttributeError, some_method)
        self.assertFalse(mock_log.called)

    @mock.patch('oslo_db.api.time.sleep', return_value=None)
    def test_retry_wrapper_deadlock(self, mock_sleep):

        # Tests that jitter is False, if the retry wrapper hits a
        # non-deadlock error
        @api.wrap_db_retry(max_retries=1, retry_on_deadlock=True)
        def some_method_no_deadlock():
            raise exception.RetryRequest(ValueError())
        with mock.patch(
                'oslo_db.api.wrap_db_retry._get_inc_interval') as mock_get:
            mock_get.return_value = 2, 2
            self.assertRaises(ValueError, some_method_no_deadlock)
            mock_get.assert_called_once_with(1, False)

        # Tests that jitter is True, if the retry wrapper hits a deadlock
        # error.
        @api.wrap_db_retry(max_retries=1, retry_on_deadlock=True)
        def some_method_deadlock():
            raise exception.DBDeadlock('test')
        with mock.patch(
                'oslo_db.api.wrap_db_retry._get_inc_interval') as mock_get:
            mock_get.return_value = 0.1, 2
            self.assertRaises(exception.DBDeadlock, some_method_deadlock)
            mock_get.assert_called_once_with(1, True)

        # Tests that jitter is True, if the jitter is enable by user
        @api.wrap_db_retry(max_retries=1, retry_on_deadlock=True, jitter=True)
        def some_method_no_deadlock_exp():
            raise exception.RetryRequest(ValueError())
        with mock.patch(
                'oslo_db.api.wrap_db_retry._get_inc_interval') as mock_get:
            mock_get.return_value = 0.1, 2
            self.assertRaises(ValueError, some_method_no_deadlock_exp)
            mock_get.assert_called_once_with(1, True)

    def test_wrap_db_retry_get_interval(self):
        x = api.wrap_db_retry(max_retries=5, retry_on_deadlock=True,
                              max_retry_interval=11)
        self.assertEqual(11, x.max_retry_interval)
        for i in (1, 2, 4):
            # With jitter: sleep_time = [0, 2 ** retry_times)
            sleep_time, n = x._get_inc_interval(i, True)
            self.assertEqual(2 * i, n)
            self.assertTrue(2 * i > sleep_time)
            # Without jitter: sleep_time = 2 ** retry_times
            sleep_time, n = x._get_inc_interval(i, False)
            self.assertEqual(2 * i, n)
            self.assertEqual(2 * i, sleep_time)
        for i in (8, 16, 32):
            sleep_time, n = x._get_inc_interval(i, False)
            self.assertEqual(x.max_retry_interval, sleep_time)
            self.assertEqual(2 * i, n)
            sleep_time, n = x._get_inc_interval(i, True)
            self.assertTrue(x.max_retry_interval >= sleep_time)
            self.assertEqual(2 * i, n)
