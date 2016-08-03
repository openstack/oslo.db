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

from oslo_db.sqlalchemy import provision
from oslo_db.sqlalchemy import test_base
from oslotest import base as oslo_test_base


class BackendSkipTest(oslo_test_base.BaseTestCase):

    def test_skip_no_dbapi(self):

        class FakeDatabaseOpportunisticFixture(test_base.DbFixture):
            DRIVER = 'postgresql'

        class SomeTest(test_base.DbTestCase):
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

        class FakeDatabaseOpportunisticFixture(test_base.DbFixture):
            DRIVER = 'postgresql+nosuchdbapi'

        class SomeTest(test_base.DbTestCase):
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
