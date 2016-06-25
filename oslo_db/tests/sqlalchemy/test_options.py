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

from oslo_config import cfg
from oslo_config import fixture as config

from oslo_db import options
from oslo_db.tests import utils as test_utils


class DbApiOptionsTestCase(test_utils.BaseTestCase):
    def setUp(self):
        super(DbApiOptionsTestCase, self).setUp()

        config_fixture = self.useFixture(config.Config())
        self.conf = config_fixture.conf
        self.conf.register_opts(options.database_opts, group='database')
        self.config = config_fixture.config

    def test_deprecated_session_parameters(self):
        path = self.create_tempfiles([["tmp", b"""[DEFAULT]
sql_connection=x://y.z
sql_min_pool_size=10
sql_max_pool_size=20
sql_max_retries=30
sql_retry_interval=40
sql_max_overflow=50
sql_connection_debug=60
sql_connection_trace=True
"""]])[0]
        self.conf(['--config-file', path])
        self.assertEqual('x://y.z', self.conf.database.connection)
        self.assertEqual(10, self.conf.database.min_pool_size)
        self.assertEqual(20, self.conf.database.max_pool_size)
        self.assertEqual(30, self.conf.database.max_retries)
        self.assertEqual(40, self.conf.database.retry_interval)
        self.assertEqual(50, self.conf.database.max_overflow)
        self.assertEqual(60, self.conf.database.connection_debug)
        self.assertEqual(True, self.conf.database.connection_trace)

    def test_session_parameters(self):
        path = self.create_tempfiles([["tmp", b"""[database]
connection=x://y.z
min_pool_size=10
max_pool_size=20
max_retries=30
retry_interval=40
max_overflow=50
connection_debug=60
connection_trace=True
pool_timeout=7
"""]])[0]
        self.conf(['--config-file', path])
        self.assertEqual('x://y.z', self.conf.database.connection)
        self.assertEqual(10, self.conf.database.min_pool_size)
        self.assertEqual(20, self.conf.database.max_pool_size)
        self.assertEqual(30, self.conf.database.max_retries)
        self.assertEqual(40, self.conf.database.retry_interval)
        self.assertEqual(50, self.conf.database.max_overflow)
        self.assertEqual(60, self.conf.database.connection_debug)
        self.assertEqual(True, self.conf.database.connection_trace)
        self.assertEqual(7, self.conf.database.pool_timeout)

    def test_dbapi_database_deprecated_parameters(self):
        path = self.create_tempfiles([['tmp', b'[DATABASE]\n'
                                       b'sql_connection=fake_connection\n'
                                       b'sql_idle_timeout=100\n'
                                       b'sql_min_pool_size=99\n'
                                       b'sql_max_pool_size=199\n'
                                       b'sql_max_retries=22\n'
                                       b'reconnect_interval=17\n'
                                       b'sqlalchemy_max_overflow=101\n'
                                       b'sqlalchemy_pool_timeout=5\n'
                                       ]])[0]
        self.conf(['--config-file', path])
        self.assertEqual('fake_connection', self.conf.database.connection)
        self.assertEqual(100, self.conf.database.connection_recycle_time)
        self.assertEqual(100, self.conf.database.idle_timeout)
        self.assertEqual(99, self.conf.database.min_pool_size)
        self.assertEqual(199, self.conf.database.max_pool_size)
        self.assertEqual(22, self.conf.database.max_retries)
        self.assertEqual(17, self.conf.database.retry_interval)
        self.assertEqual(101, self.conf.database.max_overflow)
        self.assertEqual(5, self.conf.database.pool_timeout)

    def test_dbapi_database_deprecated_parameters_sql(self):
        path = self.create_tempfiles([['tmp', b'[sql]\n'
                                       b'connection=test_sql_connection\n'
                                       b'idle_timeout=99\n'
                                       ]])[0]
        self.conf(['--config-file', path])
        self.assertEqual('test_sql_connection', self.conf.database.connection)
        self.assertEqual(99, self.conf.database.connection_recycle_time)
        self.assertEqual(99, self.conf.database.idle_timeout)

    def test_deprecated_dbapi_parameters(self):
        path = self.create_tempfiles([['tmp', b'[DEFAULT]\n'
                                      b'db_backend=test_123\n'
                                       ]])[0]

        self.conf(['--config-file', path])
        self.assertEqual('test_123', self.conf.database.backend)

    def test_dbapi_parameters(self):
        path = self.create_tempfiles([['tmp', b'[database]\n'
                                      b'backend=test_123\n'
                                       ]])[0]

        self.conf(['--config-file', path])
        self.assertEqual('test_123', self.conf.database.backend)

    def test_set_defaults(self):
        conf = cfg.ConfigOpts()

        options.set_defaults(conf,
                             connection='sqlite:///:memory:')

        self.assertTrue(len(conf.database.items()) > 1)
        self.assertEqual('sqlite:///:memory:', conf.database.connection)
