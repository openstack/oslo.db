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
from oslo_config import fixture as config_fixture

from oslo_db import options
from oslo_db.tests import base as test_base


class DbApiOptionsTestCase(test_base.BaseTestCase):

    def setUp(self):
        super().setUp()

        self.conf = self.useFixture(config_fixture.Config()).conf
        self.conf.register_opts(options.database_opts, group='database')

    def test_session_parameters(self):
        path = self.create_tempfiles([["tmp", b"""[database]
connection=x://y.z
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
        self.assertEqual(20, self.conf.database.max_pool_size)
        self.assertEqual(30, self.conf.database.max_retries)
        self.assertEqual(40, self.conf.database.retry_interval)
        self.assertEqual(50, self.conf.database.max_overflow)
        self.assertEqual(60, self.conf.database.connection_debug)
        self.assertEqual(True, self.conf.database.connection_trace)
        self.assertEqual(7, self.conf.database.pool_timeout)

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
        self.assertEqual(None, self.conf.database.mysql_wsrep_sync_wait)
