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

import sys
from unittest import mock

from oslo_db import concurrency
from oslo_db.tests import utils as test_utils

FAKE_BACKEND_MAPPING = {'sqlalchemy': 'fake.db.sqlalchemy.api'}


class TpoolDbapiWrapperTestCase(test_utils.BaseTestCase):

    def setUp(self):
        super(TpoolDbapiWrapperTestCase, self).setUp()
        self.db_api = concurrency.TpoolDbapiWrapper(
            conf=self.conf, backend_mapping=FAKE_BACKEND_MAPPING)

        # NOTE(akurilin): We are not  going to add `eventlet` to `oslo_db` in
        # requirements (`requirements.txt` and `test-requirements.txt`) due to
        # the following reasons:
        #  - supporting of eventlet's thread pooling is totally optional;
        #  - we don't need to test `tpool.Proxy` functionality itself,
        #    because it's a tool from the third party library;
        #  - `eventlet` would prevent us from running unit tests on Python 3.x
        #    versions, because it doesn't support them yet.
        #
        # As we don't test `tpool.Proxy`, we can safely mock it in tests.

        self.proxy = mock.MagicMock()
        self.eventlet = mock.MagicMock()
        self.eventlet.tpool.Proxy.return_value = self.proxy
        sys.modules['eventlet'] = self.eventlet
        self.addCleanup(sys.modules.pop, 'eventlet', None)

    @mock.patch('oslo_db.api.DBAPI')
    def test_db_api_common(self, mock_db_api):
        # test context:
        #     CONF.database.use_tpool == False
        #     eventlet is installed
        # expected result:
        #     TpoolDbapiWrapper should wrap DBAPI

        fake_db_api = mock.MagicMock()
        mock_db_api.from_config.return_value = fake_db_api

        # get access to some db-api method
        self.db_api.fake_call_1

        mock_db_api.from_config.assert_called_once_with(
            conf=self.conf, backend_mapping=FAKE_BACKEND_MAPPING)
        self.assertEqual(fake_db_api, self.db_api._db_api)
        self.assertFalse(self.eventlet.tpool.Proxy.called)

        # get access to other db-api method to be sure that api didn't changed
        self.db_api.fake_call_2

        self.assertEqual(fake_db_api, self.db_api._db_api)
        self.assertFalse(self.eventlet.tpool.Proxy.called)
        self.assertEqual(1, mock_db_api.from_config.call_count)

    @mock.patch('oslo_db.api.DBAPI')
    def test_db_api_config_change(self, mock_db_api):
        # test context:
        #     CONF.database.use_tpool == True
        #     eventlet is installed
        # expected result:
        #     TpoolDbapiWrapper should wrap tpool proxy

        fake_db_api = mock.MagicMock()
        mock_db_api.from_config.return_value = fake_db_api
        self.conf.set_override('use_tpool', True, group='database')

        # get access to some db-api method
        self.db_api.fake_call

        # CONF.database.use_tpool is True, so we get tpool proxy in this case
        mock_db_api.from_config.assert_called_once_with(
            conf=self.conf, backend_mapping=FAKE_BACKEND_MAPPING)
        self.eventlet.tpool.Proxy.assert_called_once_with(fake_db_api)
        self.assertEqual(self.proxy, self.db_api._db_api)

    @mock.patch('oslo_db.api.DBAPI')
    def test_db_api_without_installed_eventlet(self, mock_db_api):
        # test context:
        #     CONF.database.use_tpool == True
        #     eventlet is not installed
        # expected result:
        #     raise ImportError

        self.conf.set_override('use_tpool', True, group='database')
        sys.modules['eventlet'] = None

        self.assertRaises(ImportError, getattr, self.db_api, 'fake')
