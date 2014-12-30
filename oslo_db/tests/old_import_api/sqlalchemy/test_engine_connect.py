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

"""Test the compatibility layer for the engine_connect() event.

This event is added as of SQLAlchemy 0.9.0; oslo.db provides a compatibility
layer for prior SQLAlchemy versions.

"""

import mock
from oslotest import base as test_base
import sqlalchemy as sqla

from oslo.db.sqlalchemy import compat


class EngineConnectTest(test_base.BaseTestCase):

    def setUp(self):
        super(EngineConnectTest, self).setUp()

        self.engine = engine = sqla.create_engine("sqlite://")
        self.addCleanup(engine.dispose)

    def test_connect_event(self):
        engine = self.engine

        listener = mock.Mock()
        compat.engine_connect(engine, listener)

        conn = engine.connect()
        self.assertEqual(
            listener.mock_calls,
            [mock.call(conn, False)]
        )

        conn.close()

        conn2 = engine.connect()
        conn2.close()
        self.assertEqual(
            listener.mock_calls,
            [mock.call(conn, False), mock.call(conn2, False)]
        )

    def test_branch(self):
        engine = self.engine

        listener = mock.Mock()
        compat.engine_connect(engine, listener)

        conn = engine.connect()
        branched = conn.connect()
        conn.close()
        self.assertEqual(
            listener.mock_calls,
            [mock.call(conn, False), mock.call(branched, True)]
        )
