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

import mock
import testresources

from oslo_db.sqlalchemy import enginefacade
from oslo_db.sqlalchemy import provision
from oslo_db.sqlalchemy import test_base as legacy_test_base
from oslo_db.sqlalchemy import test_fixtures
from oslotest import base as oslo_test_base


class BackendSkipTest(oslo_test_base.BaseTestCase):

    def test_skip_no_dbapi(self):

        class FakeDatabaseOpportunisticFixture(
                test_fixtures.OpportunisticDbFixture):
            DRIVER = 'postgresql'

        class SomeTest(test_fixtures.OpportunisticDBTestMixin,
                       oslo_test_base.BaseTestCase):
            FIXTURE = FakeDatabaseOpportunisticFixture

            def runTest(self):
                pass

        st = SomeTest()

        # patch in replacement lookup dictionaries to avoid
        # leaking from/to other tests
        with mock.patch(
                "oslo_db.sqlalchemy.provision."
                "Backend.backends_by_database_type", {
                    "postgresql":
                    provision.Backend("postgresql", "postgresql://")}):
            st._database_resources = {}
            st._db_not_available = {}
            st._schema_resources = {}

            with mock.patch(
                    "sqlalchemy.create_engine",
                    mock.Mock(side_effect=ImportError())):

                self.assertEqual([], st.resources)

                ex = self.assertRaises(
                    self.skipException,
                    st.setUp
                )

        self.assertEqual(
            "Backend 'postgresql' is unavailable: No DBAPI installed",
            str(ex)
        )

    def test_skip_no_such_backend(self):

        class FakeDatabaseOpportunisticFixture(
                test_fixtures.OpportunisticDbFixture):
            DRIVER = 'postgresql+nosuchdbapi'

        class SomeTest(test_fixtures.OpportunisticDBTestMixin,
                       oslo_test_base.BaseTestCase):

            FIXTURE = FakeDatabaseOpportunisticFixture

            def runTest(self):
                pass

        st = SomeTest()

        ex = self.assertRaises(
            self.skipException,
            st.setUp
        )

        self.assertEqual(
            "Backend 'postgresql+nosuchdbapi' is unavailable: No such backend",
            str(ex)
        )

    def test_skip_no_dbapi_legacy(self):

        class FakeDatabaseOpportunisticFixture(
                legacy_test_base.DbFixture):
            DRIVER = 'postgresql'

        class SomeTest(legacy_test_base.DbTestCase):
            FIXTURE = FakeDatabaseOpportunisticFixture

            def runTest(self):
                pass

        st = SomeTest()

        # patch in replacement lookup dictionaries to avoid
        # leaking from/to other tests
        with mock.patch(
                "oslo_db.sqlalchemy.provision."
                "Backend.backends_by_database_type", {
                    "postgresql":
                    provision.Backend("postgresql", "postgresql://")}):
            st._database_resources = {}
            st._db_not_available = {}
            st._schema_resources = {}

            with mock.patch(
                    "sqlalchemy.create_engine",
                    mock.Mock(side_effect=ImportError())):

                self.assertEqual([], st.resources)

                ex = self.assertRaises(
                    self.skipException,
                    st.setUp
                )

        self.assertEqual(
            "Backend 'postgresql' is unavailable: No DBAPI installed",
            str(ex)
        )

    def test_skip_no_such_backend_legacy(self):

        class FakeDatabaseOpportunisticFixture(
                legacy_test_base.DbFixture):
            DRIVER = 'postgresql+nosuchdbapi'

        class SomeTest(legacy_test_base.DbTestCase):

            FIXTURE = FakeDatabaseOpportunisticFixture

            def runTest(self):
                pass

        st = SomeTest()

        ex = self.assertRaises(
            self.skipException,
            st.setUp
        )

        self.assertEqual(
            "Backend 'postgresql+nosuchdbapi' is unavailable: No such backend",
            str(ex)
        )


class EnginefacadeIntegrationTest(oslo_test_base.BaseTestCase):
    def test_db_fixture(self):
        normal_mgr = enginefacade.transaction_context()
        normal_mgr.configure(
            connection="sqlite://",
            sqlite_fk=True,
            mysql_sql_mode="FOOBAR",
            max_overflow=38
        )

        class MyFixture(test_fixtures.OpportunisticDbFixture):
            def get_enginefacade(self):
                return normal_mgr

        test = mock.Mock(SCHEMA_SCOPE=None)
        fixture = MyFixture(test=test)
        resources = fixture._get_resources()

        testresources.setUpResources(test, resources, None)
        self.addCleanup(
            testresources.tearDownResources,
            test, resources, None
        )
        fixture.setUp()
        self.addCleanup(fixture.cleanUp)

        self.assertTrue(normal_mgr._factory._started)

        test.engine = normal_mgr.writer.get_engine()
        self.assertEqual("sqlite://", str(test.engine.url))
        self.assertIs(test.engine, normal_mgr._factory._writer_engine)
        engine_args = normal_mgr._factory._engine_args_for_conf(None)
        self.assertTrue(engine_args['sqlite_fk'])
        self.assertEqual("FOOBAR", engine_args["mysql_sql_mode"])
        self.assertEqual(38, engine_args["max_overflow"])

        fixture.cleanUp()
        fixture._clear_cleanups()  # so the real cleanUp works
        self.assertFalse(normal_mgr._factory._started)
