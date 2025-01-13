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

import os
from unittest import mock

from sqlalchemy.engine import url as sqla_url
from sqlalchemy import exc as sa_exc
from sqlalchemy import inspect
from sqlalchemy import schema
from sqlalchemy import types

from oslo_db import exception
from oslo_db.sqlalchemy import enginefacade
from oslo_db.sqlalchemy import provision
from oslo_db.sqlalchemy import test_fixtures
from oslo_db.tests import base as test_base
from oslo_db.tests.sqlalchemy import base as db_test_base


class DropAllObjectsTest(db_test_base._DbTestCase):

    def setUp(self):
        super().setUp()

        self.metadata = metadata = schema.MetaData()
        schema.Table(
            'a', metadata,
            schema.Column('id', types.Integer, primary_key=True),
            mysql_engine='InnoDB'
        )
        schema.Table(
            'b', metadata,
            schema.Column('id', types.Integer, primary_key=True),
            schema.Column('a_id', types.Integer, schema.ForeignKey('a.id')),
            mysql_engine='InnoDB'
        )
        schema.Table(
            'c', metadata,
            schema.Column('id', types.Integer, primary_key=True),
            schema.Column('b_id', types.Integer, schema.ForeignKey('b.id')),
            schema.Column(
                'd_id', types.Integer,
                schema.ForeignKey('d.id', use_alter=True, name='c_d_fk')),
            mysql_engine='InnoDB'
        )
        schema.Table(
            'd', metadata,
            schema.Column('id', types.Integer, primary_key=True),
            schema.Column('c_id', types.Integer, schema.ForeignKey('c.id')),
            mysql_engine='InnoDB'
        )

        metadata.create_all(self.engine, checkfirst=False)
        # will drop nothing if the test worked
        self.addCleanup(metadata.drop_all, self.engine, checkfirst=True)

    def test_drop_all(self):
        insp = inspect(self.engine)
        self.assertEqual(
            {'a', 'b', 'c', 'd'},
            set(insp.get_table_names())
        )

        self._get_default_provisioned_db().\
            backend.drop_all_objects(self.engine)

        insp = inspect(self.engine)
        self.assertEqual(
            [],
            insp.get_table_names()
        )


class BackendNotAvailableTest(test_base.BaseTestCase):
    def test_no_dbapi(self):
        backend = provision.Backend(
            "postgresql", "postgresql+nosuchdbapi://hostname/dsn")

        with mock.patch(
                "sqlalchemy.create_engine",
                mock.Mock(side_effect=ImportError("nosuchdbapi"))):

            # NOTE(zzzeek): Call and test the _verify function twice, as it
            # exercises a different code path on subsequent runs vs.
            # the first run
            ex = self.assertRaises(
                exception.BackendNotAvailable,
                backend._verify)
            self.assertEqual(
                "Backend 'postgresql+nosuchdbapi' is unavailable: "
                "No DBAPI installed", str(ex))

            ex = self.assertRaises(
                exception.BackendNotAvailable,
                backend._verify)
            self.assertEqual(
                "Backend 'postgresql+nosuchdbapi' is unavailable: "
                "No DBAPI installed", str(ex))

    def test_cant_connect(self):
        backend = provision.Backend(
            "postgresql", "postgresql+nosuchdbapi://hostname/dsn")

        with mock.patch(
                "sqlalchemy.create_engine",
                mock.Mock(return_value=mock.Mock(connect=mock.Mock(
                    side_effect=sa_exc.OperationalError(
                        "can't connect", None, None))
                ))
        ):

            # NOTE(zzzeek): Call and test the _verify function twice, as it
            # exercises a different code path on subsequent runs vs.
            # the first run
            ex = self.assertRaises(
                exception.BackendNotAvailable,
                backend._verify)
            self.assertEqual(
                "Backend 'postgresql+nosuchdbapi' is unavailable: "
                "Could not connect", str(ex))

            ex = self.assertRaises(
                exception.BackendNotAvailable,
                backend._verify)
            self.assertEqual(
                "Backend 'postgresql+nosuchdbapi' is unavailable: "
                "Could not connect", str(ex))


class MySQLDropAllObjectsTest(
    DropAllObjectsTest, db_test_base._MySQLOpportunisticTestCase,
):
    pass


class PostgreSQLDropAllObjectsTest(
    DropAllObjectsTest, db_test_base._PostgreSQLOpportunisticTestCase,
):
    pass


class AdHocURLTest(test_base.BaseTestCase):
    def test_sqlite_setup_teardown(self):

        fixture = test_fixtures.AdHocDbFixture("sqlite:///foo.db")

        fixture.setUp()

        self.assertEqual(
            enginefacade._context_manager._factory._writer_engine.url,
            sqla_url.make_url("sqlite:///foo.db")
            )

        self.assertTrue(os.path.exists("foo.db"))
        fixture.cleanUp()

        self.assertFalse(os.path.exists("foo.db"))

    def test_mysql_setup_teardown(self):
        try:
            mysql_backend = provision.Backend.backend_for_database_type(
                "mysql")
        except exception.BackendNotAvailable:
            self.skipTest("mysql backend not available")

        mysql_backend.create_named_database("adhoc_test")
        self.addCleanup(
            mysql_backend.drop_named_database, "adhoc_test"
        )
        url = mysql_backend.provisioned_database_url("adhoc_test")

        fixture = test_fixtures.AdHocDbFixture(url)

        fixture.setUp()

        self.assertEqual(
            enginefacade._context_manager._factory._writer_engine.url,
            url
        )

        fixture.cleanUp()
