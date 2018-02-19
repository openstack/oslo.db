#    -*- encoding: utf-8 -*-
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

"""Test exception filters applied to engines."""

import contextlib
import itertools

import mock
from oslotest import base as oslo_test_base
import six
import sqlalchemy as sqla
from sqlalchemy.engine import url as sqla_url
from sqlalchemy import event
import sqlalchemy.exc
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import mapper

from oslo_db import exception
from oslo_db.sqlalchemy.compat import utils as compat_utils
from oslo_db.sqlalchemy import engines
from oslo_db.sqlalchemy import exc_filters
from oslo_db.tests.sqlalchemy import base as test_base
from oslo_db.tests import utils as test_utils

_TABLE_NAME = '__tmp__test__tmp__'


class _SQLAExceptionMatcher(object):
    def assertInnerException(
            self,
            matched, exception_type, message, sql=None, params=None):

        exc = matched.inner_exception
        self.assertSQLAException(exc, exception_type, message, sql, params)

    def assertSQLAException(
            self,
            exc, exception_type, message, sql=None, params=None):
        if isinstance(exception_type, (type, tuple)):
            self.assertTrue(issubclass(exc.__class__, exception_type))
        else:
            self.assertEqual(exception_type, exc.__class__.__name__)
        if isinstance(message, tuple):
            self.assertEqual(
                [m.lower()
                 if isinstance(m, six.string_types) else m for m in message],
                [a.lower()
                 if isinstance(a, six.string_types) else a
                 for a in exc.orig.args]
            )
        else:
            self.assertEqual(message.lower(), str(exc.orig).lower())
        if sql is not None:
            if params is not None:
                if '?' in exc.statement:
                    self.assertEqual(sql, exc.statement)
                    self.assertEqual(params, exc.params)
                else:
                    self.assertEqual(sql % params, exc.statement % exc.params)
            else:
                self.assertEqual(sql, exc.statement)


class TestsExceptionFilter(_SQLAExceptionMatcher, oslo_test_base.BaseTestCase):

    class Error(Exception):
        """DBAPI base error.

        This exception and subclasses are used in a mock context
        within these tests.

        """

    class DataError(Error):
        pass

    class OperationalError(Error):
        pass

    class InterfaceError(Error):
        pass

    class InternalError(Error):
        pass

    class IntegrityError(Error):
        pass

    class ProgrammingError(Error):
        pass

    class TransactionRollbackError(OperationalError):
        """Special psycopg2-only error class.

        SQLAlchemy has an issue with this per issue #3075:

        https://bitbucket.org/zzzeek/sqlalchemy/issue/3075/

        """

    def setUp(self):
        super(TestsExceptionFilter, self).setUp()
        self.engine = sqla.create_engine("sqlite://")
        exc_filters.register_engine(self.engine)
        self.engine.connect().close()  # initialize

    @contextlib.contextmanager
    def _dbapi_fixture(self, dialect_name, is_disconnect=False):
        engine = self.engine
        with test_utils.nested(
            mock.patch.object(engine.dialect.dbapi,
                              "Error",
                              self.Error),
            mock.patch.object(engine.dialect, "name", dialect_name),
            mock.patch.object(engine.dialect,
                              "is_disconnect",
                              lambda *args: is_disconnect)
        ):
            yield

    @contextlib.contextmanager
    def _fixture(self, dialect_name, exception, is_disconnect=False):

        def do_execute(self, cursor, statement, parameters, **kw):
            raise exception

        engine = self.engine

        # ensure the engine has done its initial checks against the
        # DB as we are going to be removing its ability to execute a
        # statement
        self.engine.connect().close()

        with test_utils.nested(
            mock.patch.object(engine.dialect, "do_execute", do_execute),
            # replace the whole DBAPI rather than patching "Error"
            # as some DBAPIs might not be patchable (?)
            mock.patch.object(engine.dialect,
                              "dbapi",
                              mock.Mock(Error=self.Error)),
            mock.patch.object(engine.dialect, "name", dialect_name),
            mock.patch.object(engine.dialect,
                              "is_disconnect",
                              lambda *args: is_disconnect)
        ):
            yield

    def _run_test(self, dialect_name, statement, raises, expected,
                  is_disconnect=False, params=()):
        with self._fixture(dialect_name, raises, is_disconnect=is_disconnect):
            with self.engine.connect() as conn:
                matched = self.assertRaises(
                    expected, conn.execute, statement, params
                )
                return matched


class TestFallthroughsAndNonDBAPI(TestsExceptionFilter):

    def test_generic_dbapi(self):
        matched = self._run_test(
            "mysql", "select you_made_a_programming_error",
            self.ProgrammingError("Error 123, you made a mistake"),
            exception.DBError
        )
        self.assertInnerException(
            matched,
            "ProgrammingError",
            "Error 123, you made a mistake",
            'select you_made_a_programming_error', ())

    def test_generic_dbapi_disconnect(self):
        matched = self._run_test(
            "mysql", "select the_db_disconnected",
            self.InterfaceError("connection lost"),
            exception.DBConnectionError,
            is_disconnect=True
        )
        self.assertInnerException(
            matched,
            "InterfaceError", "connection lost",
            "select the_db_disconnected", ()),

    def test_operational_dbapi_disconnect(self):
        matched = self._run_test(
            "mysql", "select the_db_disconnected",
            self.OperationalError("connection lost"),
            exception.DBConnectionError,
            is_disconnect=True
        )
        self.assertInnerException(
            matched,
            "OperationalError", "connection lost",
            "select the_db_disconnected", ()),

    def test_operational_error_asis(self):
        """Test operational errors.

        test that SQLAlchemy OperationalErrors that aren't disconnects
        are passed through without wrapping.
        """

        matched = self._run_test(
            "mysql", "select some_operational_error",
            self.OperationalError("some op error"),
            sqla.exc.OperationalError
        )
        self.assertSQLAException(
            matched,
            "OperationalError", "some op error"
        )

    def test_unicode_encode(self):
        # intentionally generate a UnicodeEncodeError, as its
        # constructor is quite complicated and seems to be non-public
        # or at least not documented anywhere.
        uee_ref = None
        try:
            six.u('\u2435').encode('ascii')
        except UnicodeEncodeError as uee:
            # Python3.x added new scoping rules here (sadly)
            # http://legacy.python.org/dev/peps/pep-3110/#semantic-changes
            uee_ref = uee

        self._run_test(
            "postgresql", six.u('select \u2435'),
            uee_ref,
            exception.DBInvalidUnicodeParameter
            )

    def test_garden_variety(self):
        matched = self._run_test(
            "mysql", "select some_thing_that_breaks",
            AttributeError("mysqldb has an attribute error"),
            exception.DBError
        )
        self.assertEqual("mysqldb has an attribute error", matched.args[0])


class TestNonExistentConstraint(
        _SQLAExceptionMatcher,
        test_base._DbTestCase):

    def setUp(self):
        super(TestNonExistentConstraint, self).setUp()

        meta = sqla.MetaData(bind=self.engine)

        self.table_1 = sqla.Table(
            "resource_foo", meta,
            sqla.Column("id", sqla.Integer, primary_key=True),
            mysql_engine='InnoDB',
            mysql_charset='utf8',
        )
        self.table_1.create()


class TestNonExistentConstraintPostgreSQL(
        TestNonExistentConstraint,
        test_base._PostgreSQLOpportunisticTestCase):

    def test_raise(self):
        matched = self.assertRaises(
            exception.DBNonExistentConstraint,
            self.engine.execute,
            sqla.schema.DropConstraint(
                sqla.ForeignKeyConstraint(["id"], ["baz.id"],
                                          name="bar_fkey",
                                          table=self.table_1)),
        )
        self.assertInnerException(
            matched,
            "ProgrammingError",
            "constraint \"bar_fkey\" of relation "
            "\"resource_foo\" does not exist\n",
            "ALTER TABLE resource_foo DROP CONSTRAINT bar_fkey",
        )
        self.assertEqual("resource_foo", matched.table)
        self.assertEqual("bar_fkey", matched.constraint)


class TestNonExistentConstraintMySQL(
        TestNonExistentConstraint,
        test_base._MySQLOpportunisticTestCase):

    def test_raise(self):
        matched = self.assertRaises(
            exception.DBNonExistentConstraint,
            self.engine.execute,
            sqla.schema.DropConstraint(
                sqla.ForeignKeyConstraint(["id"], ["baz.id"],
                                          name="bar_fkey",
                                          table=self.table_1)),
        )
        # NOTE(jd) Cannot check precisely with assertInnerException since MySQL
        # error are not the same depending on its version…
        self.assertIsInstance(matched.inner_exception,
                              (sqlalchemy.exc.InternalError,
                               sqlalchemy.exc.OperationalError))
        if matched.table is not None:
            self.assertEqual("resource_foo", matched.table)
        if matched.constraint is not None:
            self.assertEqual("bar_fkey", matched.constraint)


class TestNonExistentTable(
        _SQLAExceptionMatcher,
        test_base._DbTestCase):

    def setUp(self):
        super(TestNonExistentTable, self).setUp()

        self.meta = sqla.MetaData(bind=self.engine)

        self.table_1 = sqla.Table(
            "foo", self.meta,
            sqla.Column("id", sqla.Integer, primary_key=True),
            mysql_engine='InnoDB',
            mysql_charset='utf8',
        )

    def test_raise(self):
        matched = self.assertRaises(
            exception.DBNonExistentTable,
            self.engine.execute,
            sqla.schema.DropTable(self.table_1),
        )
        self.assertInnerException(
            matched,
            "OperationalError",
            "no such table: foo",
            "\nDROP TABLE foo",
        )
        self.assertEqual("foo", matched.table)


class TestNonExistentTablePostgreSQL(
        TestNonExistentTable,
        test_base._PostgreSQLOpportunisticTestCase):

    def test_raise(self):
        matched = self.assertRaises(
            exception.DBNonExistentTable,
            self.engine.execute,
            sqla.schema.DropTable(self.table_1),
        )
        self.assertInnerException(
            matched,
            "ProgrammingError",
            "table \"foo\" does not exist\n",
            "\nDROP TABLE foo",
        )
        self.assertEqual("foo", matched.table)


class TestNonExistentTableMySQL(
        TestNonExistentTable,
        test_base._MySQLOpportunisticTestCase):

    def test_raise(self):
        matched = self.assertRaises(
            exception.DBNonExistentTable,
            self.engine.execute,
            sqla.schema.DropTable(self.table_1),
        )
        # NOTE(jd) Cannot check precisely with assertInnerException since MySQL
        # error are not the same depending on its version…
        self.assertIsInstance(matched.inner_exception,
                              (sqlalchemy.exc.InternalError,
                               sqlalchemy.exc.OperationalError))
        self.assertEqual("foo", matched.table)


class TestNonExistentDatabase(
        _SQLAExceptionMatcher,
        test_base._DbTestCase):

    def setUp(self):
        super(TestNonExistentDatabase, self).setUp()

        url = sqla_url.make_url(str(self.engine.url))
        url.database = 'non_existent_database'
        self.url = url

    def test_raise(self):
        matched = self.assertRaises(
            exception.DBNonExistentDatabase,
            engines.create_engine,
            sqla_url.make_url(
                'sqlite:////non_existent_dir/non_existent_database')
        )
        self.assertIsNone(matched.database)
        self.assertInnerException(
            matched,
            sqlalchemy.exc.OperationalError,
            'unable to open database file',
        )


class TestNonExistentDatabaseMySQL(
        TestNonExistentDatabase,
        test_base._MySQLOpportunisticTestCase):

    def test_raise(self):
        matched = self.assertRaises(
            exception.DBNonExistentDatabase,
            engines.create_engine,
            self.url
        )
        self.assertEqual('non_existent_database', matched.database)
        # NOTE(rpodolyaka) cannot check precisely with assertInnerException
        # since MySQL errors are not the same depending on its version
        self.assertIsInstance(
            matched.inner_exception,
            (sqlalchemy.exc.InternalError, sqlalchemy.exc.OperationalError),
        )


class TestNonExistentDatabasePostgreSQL(
        TestNonExistentDatabase,
        test_base._PostgreSQLOpportunisticTestCase):

    def test_raise(self):
        matched = self.assertRaises(
            exception.DBNonExistentDatabase,
            engines.create_engine,
            self.url
        )
        self.assertEqual('non_existent_database', matched.database)
        self.assertInnerException(
            matched,
            sqlalchemy.exc.OperationalError,
            'fatal:  database "non_existent_database" does not exist\n',
        )


class TestReferenceErrorSQLite(_SQLAExceptionMatcher, test_base._DbTestCase):

    def setUp(self):
        super(TestReferenceErrorSQLite, self).setUp()

        meta = sqla.MetaData(bind=self.engine)

        self.table_1 = sqla.Table(
            "resource_foo", meta,
            sqla.Column("id", sqla.Integer, primary_key=True),
            sqla.Column("foo", sqla.Integer),
            mysql_engine='InnoDB',
            mysql_charset='utf8',
        )
        self.table_1.create()

        self.table_2 = sqla.Table(
            "resource_entity", meta,
            sqla.Column("id", sqla.Integer, primary_key=True),
            sqla.Column("foo_id", sqla.Integer,
                        sqla.ForeignKey("resource_foo.id", name="foo_fkey")),
            mysql_engine='InnoDB',
            mysql_charset='utf8',
        )
        self.table_2.create()

    def test_raise(self):
        self.engine.execute("PRAGMA foreign_keys = ON;")

        matched = self.assertRaises(
            exception.DBReferenceError,
            self.engine.execute,
            self.table_2.insert({'id': 1, 'foo_id': 2})
        )

        self.assertInnerException(
            matched,
            "IntegrityError",
            "FOREIGN KEY constraint failed",
            'INSERT INTO resource_entity (id, foo_id) VALUES (?, ?)',
            (1, 2)
        )

        self.assertIsNone(matched.table)
        self.assertIsNone(matched.constraint)
        self.assertIsNone(matched.key)
        self.assertIsNone(matched.key_table)

    def test_raise_delete(self):
        self.engine.execute("PRAGMA foreign_keys = ON;")

        with self.engine.connect() as conn:
            conn.execute(self.table_1.insert({"id": 1234, "foo": 42}))
            conn.execute(self.table_2.insert({"id": 4321, "foo_id": 1234}))
        matched = self.assertRaises(
            exception.DBReferenceError,
            self.engine.execute,
            self.table_1.delete()
        )
        self.assertInnerException(
            matched,
            "IntegrityError",
            "foreign key constraint failed",
            "DELETE FROM resource_foo",
            (),
        )

        self.assertIsNone(matched.table)
        self.assertIsNone(matched.constraint)
        self.assertIsNone(matched.key)
        self.assertIsNone(matched.key_table)


class TestReferenceErrorPostgreSQL(TestReferenceErrorSQLite,
                                   test_base._PostgreSQLOpportunisticTestCase):
    def test_raise(self):
        params = {'id': 1, 'foo_id': 2}
        matched = self.assertRaises(
            exception.DBReferenceError,
            self.engine.execute,
            self.table_2.insert(params)
        )
        self.assertInnerException(
            matched,
            "IntegrityError",
            "insert or update on table \"resource_entity\" "
            "violates foreign key constraint \"foo_fkey\"\nDETAIL:  Key "
            "(foo_id)=(2) is not present in table \"resource_foo\".\n",
            "INSERT INTO resource_entity (id, foo_id) VALUES (%(id)s, "
            "%(foo_id)s)",
            params,
        )

        self.assertEqual("resource_entity", matched.table)
        self.assertEqual("foo_fkey", matched.constraint)
        self.assertEqual("foo_id", matched.key)
        self.assertEqual("resource_foo", matched.key_table)

    def test_raise_delete(self):
        with self.engine.connect() as conn:
            conn.execute(self.table_1.insert({"id": 1234, "foo": 42}))
            conn.execute(self.table_2.insert({"id": 4321, "foo_id": 1234}))
        matched = self.assertRaises(
            exception.DBReferenceError,
            self.engine.execute,
            self.table_1.delete()
        )
        self.assertInnerException(
            matched,
            "IntegrityError",
            "update or delete on table \"resource_foo\" violates foreign key "
            "constraint \"foo_fkey\" on table \"resource_entity\"\n"
            "DETAIL:  Key (id)=(1234) is still referenced from "
            "table \"resource_entity\".\n",
            "DELETE FROM resource_foo",
            {},
        )

        self.assertEqual("resource_foo", matched.table)
        self.assertEqual("foo_fkey", matched.constraint)
        self.assertEqual("id", matched.key)
        self.assertEqual("resource_entity", matched.key_table)


class TestReferenceErrorMySQL(TestReferenceErrorSQLite,
                              test_base._MySQLOpportunisticTestCase):
    def test_raise(self):
        matched = self.assertRaises(
            exception.DBReferenceError,
            self.engine.execute,
            self.table_2.insert({'id': 1, 'foo_id': 2})
        )

        # NOTE(jd) Cannot check precisely with assertInnerException since MySQL
        # error are not the same depending on its version…
        self.assertIsInstance(matched.inner_exception,
                              sqlalchemy.exc.IntegrityError)
        self.assertEqual(matched.inner_exception.orig.args[0], 1452)
        self.assertEqual("resource_entity", matched.table)
        self.assertEqual("foo_fkey", matched.constraint)
        self.assertEqual("foo_id", matched.key)
        self.assertEqual("resource_foo", matched.key_table)

    def test_raise_ansi_quotes(self):
        with self.engine.connect() as conn:
            conn.detach()  # will not be returned to the pool when closed

            # this is incompatible with some internals of the engine
            conn.execute("SET SESSION sql_mode = 'ANSI';")

            matched = self.assertRaises(
                exception.DBReferenceError,
                conn.execute,
                self.table_2.insert({'id': 1, 'foo_id': 2})
            )

        # NOTE(jd) Cannot check precisely with assertInnerException since MySQL
        # error are not the same depending on its version…
        self.assertIsInstance(matched.inner_exception,
                              sqlalchemy.exc.IntegrityError)
        self.assertEqual(matched.inner_exception.orig.args[0], 1452)
        self.assertEqual("resource_entity", matched.table)
        self.assertEqual("foo_fkey", matched.constraint)
        self.assertEqual("foo_id", matched.key)
        self.assertEqual("resource_foo", matched.key_table)

    def test_raise_delete(self):
        with self.engine.connect() as conn:
            conn.execute(self.table_1.insert({"id": 1234, "foo": 42}))
            conn.execute(self.table_2.insert({"id": 4321, "foo_id": 1234}))
        matched = self.assertRaises(
            exception.DBReferenceError,
            self.engine.execute,
            self.table_1.delete()
        )
        # NOTE(jd) Cannot check precisely with assertInnerException since MySQL
        # error are not the same depending on its version…
        self.assertIsInstance(matched.inner_exception,
                              sqlalchemy.exc.IntegrityError)
        self.assertEqual(1451, matched.inner_exception.orig.args[0])
        self.assertEqual("resource_entity", matched.table)
        self.assertEqual("foo_fkey", matched.constraint)
        self.assertEqual("foo_id", matched.key)
        self.assertEqual("resource_foo", matched.key_table)


class TestExceptionCauseMySQLSavepoint(test_base._MySQLOpportunisticTestCase):
    def setUp(self):
        super(TestExceptionCauseMySQLSavepoint, self).setUp()

        Base = declarative_base()

        class A(Base):
            __tablename__ = 'a'

            id = sqla.Column(sqla.Integer, primary_key=True)

            __table_args__ = {'mysql_engine': 'InnoDB'}

        Base.metadata.create_all(self.engine)

        self.A = A

    def test_cause_for_failed_flush_plus_no_savepoint(self):
        session = self.sessionmaker()

        with session.begin():
            session.add(self.A(id=1))
        try:

            with session.begin():

                try:
                    with session.begin_nested():
                        session.execute("rollback")
                        session.add(self.A(id=1))

                # outermost is the failed SAVEPOINT rollback
                # from the "with session.begin_nested()"
                except exception.DBError as dbe_inner:

                    if not compat_utils.sqla_110:
                        # first "cause" is the failed SAVEPOINT rollback
                        # from inside of flush(), when it fails
                        self.assertTrue(
                            isinstance(
                                dbe_inner.cause,
                                exception.DBError
                            )
                        )

                        # second "cause" is then the actual DB duplicate
                        self.assertTrue(
                            isinstance(
                                dbe_inner.cause.cause,
                                exception.DBDuplicateEntry
                            )
                        )
                    else:
                        # in SQLA 1.1, the rollback() method of Session
                        # catches the error and repairs the state of the
                        # session even though the SAVEPOINT was lost;
                        # the net result here is that one exception is thrown
                        # instead of two.  This is SQLAlchemy ticket #3680
                        self.assertTrue(
                            isinstance(
                                dbe_inner.cause,
                                exception.DBDuplicateEntry
                            )
                        )

        except exception.DBError as dbe_outer:
            self.assertTrue(
                isinstance(
                    dbe_outer.cause,
                    exception.DBDuplicateEntry
                )
            )

        # resets itself afterwards
        try:
            with session.begin():
                session.add(self.A(id=1))
        except exception.DBError as dbe_outer:
            self.assertIsNone(dbe_outer.cause)

    def test_rollback_doesnt_interfere_with_killed_conn(self):
        session = self.sessionmaker()

        session.begin()
        try:
            session.execute("select 1")

            # close underying DB connection
            session.connection().connection.connection.close()

            # alternate approach, but same idea:
            # conn_id = session.scalar("select connection_id()")
            # session.execute("kill connection %s" % conn_id)

            # try using it, will raise an error
            session.execute("select 1")
        except exception.DBConnectionError:
            # issue being tested is that this session.rollback()
            # does not itself try to re-connect and raise another
            # error.
            session.rollback()
        else:
            assert False, "no exception raised"

    def test_savepoint_rollback_doesnt_interfere_with_killed_conn(self):
        session = self.sessionmaker()

        session.begin()
        try:
            session.begin_nested()
            session.execute("select 1")

            # close underying DB connection
            session.connection().connection.connection.close()

            # alternate approach, but same idea:
            # conn_id = session.scalar("select connection_id()")
            # session.execute("kill connection %s" % conn_id)

            # try using it, will raise an error
            session.execute("select 1")
        except exception.DBConnectionError:
            # issue being tested is that this session.rollback()
            # does not itself try to re-connect and raise another
            # error.
            session.rollback()
        else:
            assert False, "no exception raised"


class TestDBDataErrorSQLite(_SQLAExceptionMatcher, test_base._DbTestCase):

    def setUp(self):
        super(TestDBDataErrorSQLite, self).setUp()

        if six.PY3:
            self.skipTest("SQLite database supports unicode value for python3")

        meta = sqla.MetaData(bind=self.engine)

        self.table_1 = sqla.Table(
            "resource_foo", meta,
            sqla.Column("name", sqla.String),
        )
        self.table_1.create()

    def test_raise(self):

        matched = self.assertRaises(
            exception.DBDataError,
            self.engine.execute,
            self.table_1.insert({'name': u'\u2713'.encode('utf-8')})
        )

        self.assertInnerException(
            matched,
            "ProgrammingError",
            "You must not use 8-bit bytestrings unless you use a "
            "text_factory that can interpret 8-bit bytestrings "
            "(like text_factory = str). It is highly recommended that "
            "you instead just switch your application to Unicode strings.",
            "INSERT INTO resource_foo (name) VALUES (?)",
            (u'\u2713'.encode('utf-8'),)
        )


class TestConstraint(TestsExceptionFilter):
    def test_postgresql(self):
        matched = self._run_test(
            "postgresql", "insert into resource some_values",
            self.IntegrityError(
                "new row for relation \"resource\" violates "
                "check constraint \"ck_started_before_ended\""),
            exception.DBConstraintError,
        )
        self.assertEqual("resource", matched.table)
        self.assertEqual("ck_started_before_ended", matched.check_name)


class TestDuplicate(TestsExceptionFilter):

    def _run_dupe_constraint_test(self, dialect_name, message,
                                  expected_columns=['a', 'b'],
                                  expected_value=None):
        matched = self._run_test(
            dialect_name, "insert into table some_values",
            self.IntegrityError(message),
            exception.DBDuplicateEntry
        )
        self.assertEqual(expected_columns, matched.columns)
        self.assertEqual(expected_value, matched.value)

    def _not_dupe_constraint_test(self, dialect_name, statement, message,
                                  expected_cls):
        matched = self._run_test(
            dialect_name, statement,
            self.IntegrityError(message),
            expected_cls
        )
        self.assertInnerException(
            matched,
            "IntegrityError",
            str(self.IntegrityError(message)),
            statement
        )

    def test_sqlite(self):
        self._run_dupe_constraint_test("sqlite", 'column a, b are not unique')

    def test_sqlite_3_7_16_or_3_8_2_and_higher(self):
        self._run_dupe_constraint_test(
            "sqlite",
            'UNIQUE constraint failed: tbl.a, tbl.b')

    def test_sqlite_dupe_primary_key(self):
        self._run_dupe_constraint_test(
            "sqlite",
            "PRIMARY KEY must be unique 'insert into t values(10)'",
            expected_columns=[])

    def test_mysql_pymysql(self):
        self._run_dupe_constraint_test(
            "mysql",
            '(1062, "Duplicate entry '
            '\'2-3\' for key \'uniq_tbl0a0b\'")', expected_value='2-3')
        self._run_dupe_constraint_test(
            "mysql",
            '(1062, "Duplicate entry '
            '\'\' for key \'uniq_tbl0a0b\'")', expected_value='')

    def test_mysql_mysqlconnector(self):
        self._run_dupe_constraint_test(
            "mysql",
            '1062 (23000): Duplicate entry '
            '\'2-3\' for key \'uniq_tbl0a0b\'")', expected_value='2-3')

    def test_postgresql(self):
        self._run_dupe_constraint_test(
            'postgresql',
            'duplicate key value violates unique constraint'
            '"uniq_tbl0a0b"'
            '\nDETAIL:  Key (a, b)=(2, 3) already exists.\n',
            expected_value='2, 3'
        )

    def test_mysql_single(self):
        self._run_dupe_constraint_test(
            "mysql",
            "1062 (23000): Duplicate entry '2' for key 'b'",
            expected_columns=['b'],
            expected_value='2'
        )

    def test_mysql_binary(self):
        self._run_dupe_constraint_test(
            "mysql",
            "(1062, \'Duplicate entry "
            "\\\'\\\\x8A$\\\\x8D\\\\xA6\"s\\\\x8E\\\' "
            "for key \\\'PRIMARY\\\'\')",
            expected_columns=['PRIMARY'],
            expected_value="\\\\x8A$\\\\x8D\\\\xA6\"s\\\\x8E"
        )
        self._run_dupe_constraint_test(
            "mysql",
            "(1062, \'Duplicate entry "
            "''\\\\x8A$\\\\x8D\\\\xA6\"s\\\\x8E!,' "
            "for key 'PRIMARY'\')",
            expected_columns=['PRIMARY'],
            expected_value="'\\\\x8A$\\\\x8D\\\\xA6\"s\\\\x8E!,"
        )

    def test_postgresql_single(self):
        self._run_dupe_constraint_test(
            'postgresql',
            'duplicate key value violates unique constraint "uniq_tbl0b"\n'
            'DETAIL:  Key (b)=(2) already exists.\n',
            expected_columns=['b'],
            expected_value='2'
        )

    def test_unsupported_backend(self):
        self._not_dupe_constraint_test(
            "nonexistent", "insert into table some_values",
            self.IntegrityError("constraint violation"),
            exception.DBError
        )

    def test_ibm_db_sa(self):
        self._run_dupe_constraint_test(
            'ibm_db_sa',
            'SQL0803N  One or more values in the INSERT statement, UPDATE '
            'statement, or foreign key update caused by a DELETE statement are'
            ' not valid because the primary key, unique constraint or unique '
            'index identified by "2" constrains table "NOVA.KEY_PAIRS" from '
            'having duplicate values for the index key.',
            expected_columns=[]
        )

    def test_ibm_db_sa_notadupe(self):
        self._not_dupe_constraint_test(
            'ibm_db_sa',
            'ALTER TABLE instance_types ADD CONSTRAINT '
            'uniq_name_x_deleted UNIQUE (name, deleted)',
            'SQL0542N  The column named "NAME" cannot be a column of a '
            'primary key or unique key constraint because it can contain null '
            'values.',
            exception.DBError
        )


class TestDeadlock(TestsExceptionFilter):
    statement = ('SELECT quota_usages.created_at AS '
                 'quota_usages_created_at FROM quota_usages '
                 'WHERE quota_usages.project_id = %(project_id_1)s '
                 'AND quota_usages.deleted = %(deleted_1)s FOR UPDATE')
    params = {
        'project_id_1': '8891d4478bbf48ad992f050cdf55e9b5',
        'deleted_1': 0
    }

    def _run_deadlock_detect_test(
        self, dialect_name, message,
        orig_exception_cls=TestsExceptionFilter.OperationalError):
        self._run_test(
            dialect_name, self.statement,
            orig_exception_cls(message),
            exception.DBDeadlock,
            params=self.params
        )

    def _not_deadlock_test(
        self, dialect_name, message,
        expected_cls, expected_dbapi_cls,
        orig_exception_cls=TestsExceptionFilter.OperationalError):

        matched = self._run_test(
            dialect_name, self.statement,
            orig_exception_cls(message),
            expected_cls,
            params=self.params
        )

        if isinstance(matched, exception.DBError):
            matched = matched.inner_exception

        self.assertEqual(expected_dbapi_cls, matched.orig.__class__.__name__)

    def test_mysql_pymysql_deadlock(self):
        self._run_deadlock_detect_test(
            "mysql",
            "(1213, 'Deadlock found when trying "
            "to get lock; try restarting "
            "transaction')"
        )

    def test_mysql_pymysql_wsrep_deadlock(self):
        self._run_deadlock_detect_test(
            "mysql",
            "(1213, 'WSREP detected deadlock/conflict and aborted the "
            "transaction. Try restarting the transaction')",
            orig_exception_cls=self.InternalError
        )

    def test_mysql_pymysql_galera_deadlock(self):
        self._run_deadlock_detect_test(
            "mysql",
            "(1205, 'Lock wait timeout exceeded; "
            "try restarting transaction')",
            orig_exception_cls=self.InternalError
        )

    def test_mysql_mysqlconnector_deadlock(self):
        self._run_deadlock_detect_test(
            "mysql",
            "1213 (40001): Deadlock found when trying to get lock; try "
            "restarting transaction",
            orig_exception_cls=self.InternalError
        )

    def test_mysql_not_deadlock(self):
        self._not_deadlock_test(
            "mysql",
            "(1005, 'some other error')",
            sqla.exc.OperationalError,  # note OperationalErrors are sent thru
            "OperationalError",
        )

    def test_postgresql_deadlock(self):
        self._run_deadlock_detect_test(
            "postgresql",
            "deadlock detected",
            orig_exception_cls=self.TransactionRollbackError
        )

    def test_postgresql_not_deadlock(self):
        self._not_deadlock_test(
            "postgresql",
            'relation "fake" does not exist',
            # can be either depending on #3075
            (exception.DBError, sqla.exc.OperationalError),
            "TransactionRollbackError",
            orig_exception_cls=self.TransactionRollbackError
        )

    def test_ibm_db_sa_deadlock(self):
        self._run_deadlock_detect_test(
            "ibm_db_sa",
            "SQL0911N The current transaction has been "
            "rolled back because of a deadlock or timeout",
            # use the lowest class b.c. I don't know what actual error
            # class DB2's driver would raise for this
            orig_exception_cls=self.Error
        )

    def test_ibm_db_sa_not_deadlock(self):
        self._not_deadlock_test(
            "ibm_db_sa",
            "SQL01234B Some other error.",
            exception.DBError,
            "Error",
            orig_exception_cls=self.Error
        )


class TestDataError(TestsExceptionFilter):
    def _run_bad_data_test(self, dialect_name, message, error_class):
        self._run_test(dialect_name,
                       "INSERT INTO TABLE some_values",
                       error_class(message),
                       exception.DBDataError)

    def test_bad_data_incorrect_string(self):
        # Error sourced from https://bugs.launchpad.net/cinder/+bug/1393871
        self._run_bad_data_test("mysql",
                                '(1366, "Incorrect string value: \'\\xF0\' '
                                'for column \'resource\' at row 1"',
                                self.OperationalError)

    def test_bad_data_out_of_range(self):
        # Error sourced from https://bugs.launchpad.net/cinder/+bug/1463379
        self._run_bad_data_test("mysql",
                                '(1264, "Out of range value for column '
                                '\'resource\' at row 1"',
                                self.DataError)

    def test_data_too_long_for_column(self):
        self._run_bad_data_test("mysql",
                                '(1406, "Data too long for column '
                                '\'resource\' at row 1"',
                                self.DataError)


class IntegrationTest(test_base._DbTestCase):
    """Test an actual error-raising round trips against the database."""

    def setUp(self):
        super(IntegrationTest, self).setUp()
        meta = sqla.MetaData()
        self.test_table = sqla.Table(
            _TABLE_NAME, meta,
            sqla.Column('id', sqla.Integer,
                        primary_key=True, nullable=False),
            sqla.Column('counter', sqla.Integer,
                        nullable=False),
            sqla.UniqueConstraint('counter',
                                  name='uniq_counter'))
        self.test_table.create(self.engine)
        self.addCleanup(self.test_table.drop, self.engine)

        class Foo(object):
            def __init__(self, counter):
                self.counter = counter
        mapper(Foo, self.test_table)
        self.Foo = Foo

    def test_flush_wrapper_duplicate_entry(self):
        """test a duplicate entry exception."""

        _session = self.sessionmaker()

        with _session.begin():
            foo = self.Foo(counter=1)
            _session.add(foo)

        _session.begin()
        self.addCleanup(_session.rollback)
        foo = self.Foo(counter=1)
        _session.add(foo)
        self.assertRaises(exception.DBDuplicateEntry, _session.flush)

    def test_autoflush_wrapper_duplicate_entry(self):
        """Test a duplicate entry exception raised.

        test a duplicate entry exception raised via query.all()-> autoflush
        """

        _session = self.sessionmaker()

        with _session.begin():
            foo = self.Foo(counter=1)
            _session.add(foo)

        _session.begin()
        self.addCleanup(_session.rollback)
        foo = self.Foo(counter=1)
        _session.add(foo)
        self.assertTrue(_session.autoflush)
        self.assertRaises(exception.DBDuplicateEntry,
                          _session.query(self.Foo).all)

    def test_flush_wrapper_plain_integrity_error(self):
        """test a plain integrity error wrapped as DBError."""

        _session = self.sessionmaker()

        with _session.begin():
            foo = self.Foo(counter=1)
            _session.add(foo)

        _session.begin()
        self.addCleanup(_session.rollback)
        foo = self.Foo(counter=None)
        _session.add(foo)
        self.assertRaises(exception.DBError, _session.flush)

    def test_flush_wrapper_operational_error(self):
        """test an operational error from flush() raised as-is."""

        _session = self.sessionmaker()

        with _session.begin():
            foo = self.Foo(counter=1)
            _session.add(foo)

        _session.begin()
        self.addCleanup(_session.rollback)
        foo = self.Foo(counter=sqla.func.imfake(123))
        _session.add(foo)
        matched = self.assertRaises(sqla.exc.OperationalError, _session.flush)
        self.assertIn("no such function", str(matched))

    def test_query_wrapper_operational_error(self):
        """test an operational error from query.all() raised as-is."""

        _session = self.sessionmaker()

        _session.begin()
        self.addCleanup(_session.rollback)
        q = _session.query(self.Foo).filter(
            self.Foo.counter == sqla.func.imfake(123))
        matched = self.assertRaises(sqla.exc.OperationalError, q.all)
        self.assertIn("no such function", str(matched))


class TestDBDisconnected(TestsExceptionFilter):

    @contextlib.contextmanager
    def _fixture(
            self,
            dialect_name, exception, num_disconnects, is_disconnect=True):
        engine = self.engine

        event.listen(engine, "engine_connect", engines._connect_ping_listener)

        real_do_execute = engine.dialect.do_execute
        counter = itertools.count(1)

        def fake_do_execute(self, *arg, **kw):
            if next(counter) > num_disconnects:
                return real_do_execute(self, *arg, **kw)
            else:
                raise exception

        with self._dbapi_fixture(dialect_name):
            with test_utils.nested(
                mock.patch.object(engine.dialect,
                                  "do_execute",
                                  fake_do_execute),
                mock.patch.object(engine.dialect,
                                  "is_disconnect",
                                  mock.Mock(return_value=is_disconnect))
            ):
                yield

    def _test_ping_listener_disconnected(
            self, dialect_name, exc_obj, is_disconnect=True):
        with self._fixture(dialect_name, exc_obj, 1, is_disconnect):
            conn = self.engine.connect()
            with conn.begin():
                self.assertEqual(1, conn.scalar(sqla.select([1])))
                self.assertFalse(conn.closed)
                self.assertFalse(conn.invalidated)
                self.assertTrue(conn.in_transaction())

        with self._fixture(dialect_name, exc_obj, 2, is_disconnect):
            self.assertRaises(
                exception.DBConnectionError,
                self.engine.connect
            )

        # test implicit execution
        with self._fixture(dialect_name, exc_obj, 1):
            self.assertEqual(1, self.engine.scalar(sqla.select([1])))

    def test_mariadb_error_1927(self):
        for code in [1927]:
            self._test_ping_listener_disconnected(
                "mysql",
                self.InternalError('%d Connection was killed' % code),
                is_disconnect=False
            )

    def test_packet_sequence_wrong_error(self):
        self._test_ping_listener_disconnected(
            "mysql",
            self.InternalError(
                'Packet sequence number wrong - got 35 expected 1'),
            is_disconnect=False
        )

    def test_mysql_ping_listener_disconnected(self):
        for code in [2006, 2013, 2014, 2045, 2055]:
            self._test_ping_listener_disconnected(
                "mysql",
                self.OperationalError('%d MySQL server has gone away' % code)
            )

    def test_mysql_ping_listener_disconnected_regex_only(self):
        # intentionally set the is_disconnect flag to False
        # in the "sqlalchemy" layer to make sure the regexp
        # on _is_db_connection_error is catching
        for code in [2002, 2003, 2006, 2013]:
            self._test_ping_listener_disconnected(
                "mysql",
                self.OperationalError('%d MySQL server has gone away' % code),
                is_disconnect=False
            )

    def test_mysql_galera_non_primary_disconnected(self):
        self._test_ping_listener_disconnected(
            "mysql",
            self.OperationalError('(1047, \'Unknown command\') '
                                  '\'SELECT DATABASE()\' ()')
        )

    def test_mysql_galera_non_primary_disconnected_regex_only(self):
        # intentionally set the is_disconnect flag to False
        # in the "sqlalchemy" layer to make sure the regexp
        # on _is_db_connection_error is catching
        self._test_ping_listener_disconnected(
            "mysql",
            self.OperationalError('(1047, \'Unknown command\') '
                                  '\'SELECT DATABASE()\' ()'),
            is_disconnect=False
        )

    def test_db2_ping_listener_disconnected(self):
        self._test_ping_listener_disconnected(
            "ibm_db_sa",
            self.OperationalError(
                'SQL30081N: DB2 Server connection is no longer active')
        )

    def test_db2_ping_listener_disconnected_regex_only(self):
        self._test_ping_listener_disconnected(
            "ibm_db_sa",
            self.OperationalError(
                'SQL30081N: DB2 Server connection is no longer active'),
            is_disconnect=False
        )

    def test_postgresql_ping_listener_disconnected(self):
        self._test_ping_listener_disconnected(
            "postgresql",
            self.OperationalError(
                "could not connect to server: Connection refused"),
        )

    def test_postgresql_ping_listener_disconnected_regex_only(self):
        self._test_ping_listener_disconnected(
            "postgresql",
            self.OperationalError(
                "could not connect to server: Connection refused"),
            is_disconnect=False
        )


class TestDBConnectRetry(TestsExceptionFilter):

    def _run_test(self, dialect_name, exception, count, retries):
        counter = itertools.count()

        engine = self.engine

        # empty out the connection pool
        engine.dispose()

        connect_fn = engine.dialect.connect

        def cant_connect(*arg, **kw):
            if next(counter) < count:
                raise exception
            else:
                return connect_fn(*arg, **kw)

        with self._dbapi_fixture(dialect_name):
            with mock.patch.object(engine.dialect, "connect", cant_connect):
                return engines._test_connection(engine, retries, .01)

    def test_connect_no_retries(self):
        conn = self._run_test(
            "mysql",
            self.OperationalError("Error: (2003) something wrong"),
            2, 0
        )
        # didnt connect because nothing was tried
        self.assertIsNone(conn)

    def test_connect_inifinite_retries(self):
        conn = self._run_test(
            "mysql",
            self.OperationalError("Error: (2003) something wrong"),
            2, -1
        )
        # conn is good
        self.assertEqual(1, conn.scalar(sqla.select([1])))

    def test_connect_retry_past_failure(self):
        conn = self._run_test(
            "mysql",
            self.OperationalError("Error: (2003) something wrong"),
            2, 3
        )
        # conn is good
        self.assertEqual(1, conn.scalar(sqla.select([1])))

    def test_connect_retry_not_candidate_exception(self):
        self.assertRaises(
            sqla.exc.OperationalError,  # remember, we pass OperationalErrors
                                        # through at the moment :)
            self._run_test,
            "mysql",
            self.OperationalError("Error: (2015) I can't connect period"),
            2, 3
        )

    def test_connect_retry_stops_infailure(self):
        self.assertRaises(
            exception.DBConnectionError,
            self._run_test,
            "mysql",
            self.OperationalError("Error: (2003) something wrong"),
            3, 2
        )

    def test_db2_error_positive(self):
        conn = self._run_test(
            "ibm_db_sa",
            self.OperationalError("blah blah -30081 blah blah"),
            2, -1
        )
        # conn is good
        self.assertEqual(1, conn.scalar(sqla.select([1])))

    def test_db2_error_negative(self):
        self.assertRaises(
            sqla.exc.OperationalError,
            self._run_test,
            "ibm_db_sa",
            self.OperationalError("blah blah -39981 blah blah"),
            2, 3
        )


class TestDBConnectPingWrapping(TestsExceptionFilter):

    def setUp(self):
        super(TestDBConnectPingWrapping, self).setUp()
        event.listen(
            self.engine, "engine_connect", engines._connect_ping_listener)

    @contextlib.contextmanager
    def _fixture(
            self, dialect_name, exception, good_conn_count,
            is_disconnect=True):
        engine = self.engine

        # empty out the connection pool
        engine.dispose()

        connect_fn = engine.dialect.connect
        real_do_execute = engine.dialect.do_execute

        counter = itertools.count(1)

        def cant_execute(*arg, **kw):
            value = next(counter)
            if value > good_conn_count:
                raise exception
            else:
                return real_do_execute(*arg, **kw)

        def cant_connect(*arg, **kw):
            value = next(counter)
            if value > good_conn_count:
                raise exception
            else:
                return connect_fn(*arg, **kw)

        with self._dbapi_fixture(dialect_name, is_disconnect=is_disconnect):
            with mock.patch.object(engine.dialect, "connect", cant_connect):
                with mock.patch.object(
                        engine.dialect, "do_execute", cant_execute):
                    yield

    def _test_ping_listener_disconnected(
            self, dialect_name, exc_obj, is_disconnect=True):
        with self._fixture(dialect_name, exc_obj, 3, is_disconnect):
            conn = self.engine.connect()
            self.assertEqual(1, conn.scalar(sqla.select([1])))
            conn.close()

        with self._fixture(dialect_name, exc_obj, 1, is_disconnect):
            self.assertRaises(
                exception.DBConnectionError,
                self.engine.connect
            )
            self.assertRaises(
                exception.DBConnectionError,
                self.engine.connect
            )
            self.assertRaises(
                exception.DBConnectionError,
                self.engine.connect
            )

        with self._fixture(dialect_name, exc_obj, 1, is_disconnect):
            self.assertRaises(
                exception.DBConnectionError,
                self.engine.contextual_connect
            )
            self.assertRaises(
                exception.DBConnectionError,
                self.engine.contextual_connect
            )
            self.assertRaises(
                exception.DBConnectionError,
                self.engine.contextual_connect
            )

    def test_mysql_w_disconnect_flag(self):
        for code in [2002, 2003, 2002]:
            self._test_ping_listener_disconnected(
                "mysql",
                self.OperationalError('%d MySQL server has gone away' % code)
            )

    def test_mysql_wo_disconnect_flag(self):
        for code in [2002, 2003]:
            self._test_ping_listener_disconnected(
                "mysql",
                self.OperationalError('%d MySQL server has gone away' % code),
                is_disconnect=False
            )


class TestsErrorHandler(TestsExceptionFilter):
    def test_multiple_error_handlers(self):
        handler = mock.MagicMock(return_value=None)
        sqla.event.listen(self.engine, "handle_error", handler, retval=True)

        # cause an error in DB API
        self._run_test(
            "mysql", "select you_made_a_programming_error",
            self.ProgrammingError("Error 123, you made a mistake"),
            exception.DBError
        )

        # expect custom handler to be called together with oslo.db's one
        self.assertEqual(1, handler.call_count,
                         'Custom handler should be called')

    def test_chained_exceptions(self):
        class CustomError(Exception):
            pass

        def handler(context):
            return CustomError('Custom Error')

        sqla.event.listen(self.engine, "handle_error", handler, retval=True)

        # cause an error in DB API, expect exception from custom handler
        self._run_test(
            "mysql", "select you_made_a_programming_error",
            self.ProgrammingError("Error 123, you made a mistake"),
            CustomError
        )
