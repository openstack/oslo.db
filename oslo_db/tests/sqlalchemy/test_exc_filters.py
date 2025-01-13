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
from unittest import mock

import sqlalchemy as sqla
from sqlalchemy import event
import sqlalchemy.exc
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import registry
from sqlalchemy import sql

from oslo_db import exception
from oslo_db.sqlalchemy import compat
from oslo_db.sqlalchemy import engines
from oslo_db.sqlalchemy import exc_filters
from oslo_db.sqlalchemy import utils
from oslo_db.tests import base as test_base
from oslo_db.tests.sqlalchemy import base as db_test_base
from oslo_db.tests import utils as test_utils

_TABLE_NAME = '__tmp__test__tmp__'


class _SQLAExceptionMatcher:
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
                [m.lower() if isinstance(m, str) else m for m in message],
                [a.lower() if isinstance(a, str) else a for a in exc.orig.args]
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


class TestsExceptionFilter(_SQLAExceptionMatcher, test_base.BaseTestCase):

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
        super().setUp()
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

        patches = [
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
        ]
        if compat.sqla_2:
            patches.append(
                mock.patch.object(
                    engine.dialect,
                    "loaded_dbapi",
                    mock.Mock(Error=self.Error),
                )
            )

        with test_utils.nested(*patches):
            yield

    def _run_test(self, dialect_name, statement, raises, expected,
                  is_disconnect=False, params=None):
        with self._fixture(dialect_name, raises, is_disconnect=is_disconnect):
            with self.engine.connect() as conn:
                matched = self.assertRaises(
                    expected, conn.execute, sql.text(statement), params
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
            '\u2435'.encode('ascii')
        except UnicodeEncodeError as uee:
            # Python3.x added new scoping rules here (sadly)
            # http://legacy.python.org/dev/peps/pep-3110/#semantic-changes
            uee_ref = uee

        self._run_test(
            'postgresql', 'select \u2435',
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
    db_test_base._DbTestCase,
):

    def setUp(self):
        super().setUp()

        meta = sqla.MetaData()

        self.table_1 = sqla.Table(
            "resource_foo", meta,
            sqla.Column("id", sqla.Integer, primary_key=True),
            mysql_engine='InnoDB',
            mysql_charset='utf8',
        )
        self.table_1.create(self.engine)


class TestNonExistentConstraintPostgreSQL(
    TestNonExistentConstraint,
    db_test_base._PostgreSQLOpportunisticTestCase,
):

    def test_raise(self):
        with self.engine.connect() as conn:
            matched = self.assertRaises(
                exception.DBNonExistentConstraint,
                conn.execute,
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
    db_test_base._MySQLOpportunisticTestCase,
):

    def test_raise(self):
        with self.engine.connect() as conn:
            matched = self.assertRaises(
                exception.DBNonExistentConstraint,
                conn.execute,
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
    db_test_base._DbTestCase,
):

    def setUp(self):
        super().setUp()

        self.meta = sqla.MetaData()

        self.table_1 = sqla.Table(
            "foo", self.meta,
            sqla.Column("id", sqla.Integer, primary_key=True),
            mysql_engine='InnoDB',
            mysql_charset='utf8',
        )

    def test_raise(self):
        with self.engine.connect() as conn:
            matched = self.assertRaises(
                exception.DBNonExistentTable,
                conn.execute,
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
    db_test_base._PostgreSQLOpportunisticTestCase,
):

    def test_raise(self):
        with self.engine.connect() as conn:
            matched = self.assertRaises(
                exception.DBNonExistentTable,
                conn.execute,
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
    db_test_base._MySQLOpportunisticTestCase,
):

    def test_raise(self):
        with self.engine.connect() as conn:
            matched = self.assertRaises(
                exception.DBNonExistentTable,
                conn.execute,
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
    db_test_base._DbTestCase,
):

    def setUp(self):
        super().setUp()

        url = utils.make_url(self.engine.url)
        self.url = url.set(database="non_existent_database")

    def test_raise(self):
        matched = self.assertRaises(
            exception.DBNonExistentDatabase,
            engines.create_engine,
            utils.make_url(
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
    db_test_base._MySQLOpportunisticTestCase,
):

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
    db_test_base._PostgreSQLOpportunisticTestCase,
):

    def test_raise(self):
        matched = self.assertRaises(
            exception.DBNonExistentDatabase,
            engines.create_engine,
            self.url
        )
        self.assertEqual('non_existent_database', matched.database)
        # NOTE(stephenfin): As above, we cannot use assertInnerException since
        # the error messages vary depending on the version of PostgreSQL
        self.assertIsInstance(
            matched.inner_exception,
            sqlalchemy.exc.OperationalError,
        )
        # On Postgres 13:
        #   fatal:  database "non_existent_database" does not exist
        # On Postgres 14 or later:
        #   connection to server at "localhost" (::1), port 5432 failed: fatal:
        #   database "non_existent_database" does not exist
        self.assertIn(
            'fatal:  database "non_existent_database" does not exist',
            str(matched.inner_exception).lower(),
        )


class TestReferenceErrorSQLite(
    _SQLAExceptionMatcher, db_test_base._DbTestCase,
):

    def setUp(self):
        super().setUp()

        meta = sqla.MetaData()

        self.table_1 = sqla.Table(
            "resource_foo", meta,
            sqla.Column("id", sqla.Integer, primary_key=True),
            sqla.Column("foo", sqla.Integer),
            mysql_engine='InnoDB',
            mysql_charset='utf8',
        )
        self.table_1.create(self.engine)

        self.table_2 = sqla.Table(
            "resource_entity", meta,
            sqla.Column("id", sqla.Integer, primary_key=True),
            sqla.Column("foo_id", sqla.Integer,
                        sqla.ForeignKey("resource_foo.id", name="foo_fkey")),
            mysql_engine='InnoDB',
            mysql_charset='utf8',
        )
        self.table_2.create(self.engine)

    def test_raise(self):
        connection = self.engine.raw_connection()
        try:
            cursor = connection.cursor()
            cursor.execute('PRAGMA foreign_keys = ON')
            cursor.close()
        finally:
            connection.close()

        with self.engine.connect() as conn:
            matched = self.assertRaises(
                exception.DBReferenceError,
                conn.execute,
                self.table_2.insert().values(id=1, foo_id=2)
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
        connection = self.engine.raw_connection()
        try:
            cursor = connection.cursor()
            cursor.execute('PRAGMA foreign_keys = ON')
            cursor.close()
        finally:
            connection.close()

        with self.engine.connect() as conn:
            with conn.begin():
                conn.execute(self.table_1.insert().values(id=1234, foo=42))
                conn.execute(
                    self.table_2.insert().values(id=4321, foo_id=1234))
                matched = self.assertRaises(
                    exception.DBReferenceError,
                    conn.execute,
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


class TestReferenceErrorPostgreSQL(
    TestReferenceErrorSQLite,
    db_test_base._PostgreSQLOpportunisticTestCase,
):
    def test_raise(self):
        with self.engine.connect() as conn:
            params = {'id': 1, 'foo_id': 2}
            matched = self.assertRaises(
                exception.DBReferenceError,
                conn.execute,
                self.table_2.insert().values(**params)
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
            with conn.begin():
                conn.execute(self.table_1.insert().values(id=1234, foo=42))
                conn.execute(
                    self.table_2.insert().values(id=4321, foo_id=1234))

            with conn.begin():
                matched = self.assertRaises(
                    exception.DBReferenceError,
                    conn.execute,
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


class TestReferenceErrorMySQL(
    TestReferenceErrorSQLite,
    db_test_base._MySQLOpportunisticTestCase,
):
    def test_raise(self):
        with self.engine.connect() as conn:
            matched = self.assertRaises(
                exception.DBReferenceError,
                conn.execute,
                self.table_2.insert().values(id=1, foo_id=2)
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
            conn.execute(sql.text("SET SESSION sql_mode = 'ANSI';"))

            matched = self.assertRaises(
                exception.DBReferenceError,
                conn.execute,
                self.table_2.insert().values(id=1, foo_id=2)
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
        with self.engine.connect() as conn, conn.begin():
            conn.execute(self.table_1.insert().values(id=1234, foo=42))
            conn.execute(self.table_2.insert().values(id=4321, foo_id=1234))
            matched = self.assertRaises(
                exception.DBReferenceError,
                conn.execute,
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


class TestExceptionCauseMySQLSavepoint(
    db_test_base._MySQLOpportunisticTestCase,
):
    def setUp(self):
        super().setUp()

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
                        session.execute(sql.text("rollback"))
                        session.add(self.A(id=1))
                # outermost is the failed SAVEPOINT rollback
                # from the "with session.begin_nested()"
                except exception.DBError as dbe_inner:
                    # in SQLA 1.1+, the rollback() method of Session
                    # catches the error and repairs the state of the
                    # session even though the SAVEPOINT was lost;
                    # the net result here is that one exception is thrown
                    # instead of two.  This is SQLAlchemy ticket #3680
                    self.assertIsInstance(
                        dbe_inner.cause, exception.DBDuplicateEntry)

        except exception.DBError as dbe_outer:
            self.AssertIsInstance(dbe_outer.cause, exception.DBDuplicateEntry)

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
            session.execute(sql.text("select 1"))

            # close underying DB connection
            compat.driver_connection(session.connection()).close()

            # alternate approach, but same idea:
            # conn_id = session.scalar("select connection_id()")
            # session.execute("kill connection %s" % conn_id)

            # try using it, will raise an error
            session.execute(sql.text("select 1"))
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
            session.execute(sql.text("select 1"))

            # close underying DB connection
            compat.driver_connection(session.connection()).close()

            # alternate approach, but same idea:
            # conn_id = session.scalar("select connection_id()")
            # session.execute("kill connection %s" % conn_id)

            # try using it, will raise an error
            session.execute(sql.text("select 1"))
        except exception.DBConnectionError:
            # issue being tested is that this session.rollback()
            # does not itself try to re-connect and raise another
            # error.
            session.rollback()
        else:
            assert False, "no exception raised"


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

    def test_mysql_duplicate_entry_key_start_with_tablename(self):
        self._run_dupe_constraint_test(
            "mysql",
            "1062 (23000): Duplicate entry '2' for key 'tbl.uniq_tbl0b'",
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

    def test_mysql_duplicate_entry_key_start_with_tablename_binary(self):
        self._run_dupe_constraint_test(
            "mysql",
            "(1062, \'Duplicate entry "
            "\\\'\\\\x8A$\\\\x8D\\\\xA6\"s\\\\x8E\\\' "
            "for key \\\'tbl.uniq_tbl0c1\\\'\')",
            expected_columns=['c1'],
            expected_value="\\\\x8A$\\\\x8D\\\\xA6\"s\\\\x8E"
        )
        self._run_dupe_constraint_test(
            "mysql",
            "(1062, \'Duplicate entry "
            "''\\\\x8A$\\\\x8D\\\\xA6\"s\\\\x8E!,' "
            "for key 'tbl.uniq_tbl0c1'\')",
            expected_columns=['c1'],
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


class TestDeadlock(TestsExceptionFilter):
    statement = ('SELECT quota_usages.created_at AS '
                 'quota_usages_created_at FROM quota_usages '
                 'WHERE quota_usages.project_id = :project_id_1 '
                 'AND quota_usages.deleted = :deleted_1 FOR UPDATE')
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

        self._run_deadlock_detect_test(
            "mysql",
            "(1213, 'Deadlock: wsrep aborted transaction')",
            orig_exception_cls=self.InternalError
        )

        self._run_deadlock_detect_test(
            "mysql",
            "(1213, 'Deadlock: wsrep aborted transaction')",
            orig_exception_cls=self.OperationalError
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


class IntegrationTest(db_test_base._DbTestCase):
    """Test an actual error-raising round trips against the database."""

    def setUp(self):
        super().setUp()
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

        reg = registry()

        class Foo:
            def __init__(self, counter):
                self.counter = counter

        reg.map_imperatively(Foo, self.test_table)
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


class TestDBDisconnectedFixture(TestsExceptionFilter):
    native_pre_ping = False

    def _test_ping_listener_disconnected(
        self, dialect_name, exc_obj, is_disconnect=True,
    ):
        with self._fixture(
            dialect_name, exc_obj, False, is_disconnect,
        ) as engine:
            conn = engine.connect()
            with conn.begin():
                self.assertEqual(
                    1, conn.execute(sqla.select(1)).scalars().first(),
                )
                self.assertFalse(conn.closed)
                self.assertFalse(conn.invalidated)
                self.assertTrue(conn.in_transaction())

        with self._fixture(
            dialect_name, exc_obj, True, is_disconnect,
        ) as engine:
            self.assertRaises(
                exception.DBConnectionError,
                engine.connect
            )

        # test implicit execution
        with self._fixture(dialect_name, exc_obj, False) as engine:
            with engine.connect() as conn:
                self.assertEqual(
                    1, conn.execute(sqla.select(1)).scalars().first(),
                )

    @contextlib.contextmanager
    def _fixture(
        self,
        dialect_name,
        exception,
        db_stays_down,
        is_disconnect=True,
    ):
        """Fixture for testing the ping listener.

        For SQLAlchemy 2.0, the mocking is placed more deeply in the
        stack within the DBAPI connection / cursor so that we can also
        effectively mock out the "pre ping" condition.

        :param dialect_name: dialect to use.  "postgresql" or "mysql"
        :param exception: an exception class to raise
        :param db_stays_down: if True, the database will stay down after the
         first ping fails
        :param is_disconnect: whether or not the SQLAlchemy dialect should
         consider the exception object as a "disconnect error".   Openstack's
         own exception handlers upgrade various DB exceptions to be
         "disconnect" scenarios that SQLAlchemy itself does not, such as
         some specific Galera error messages.

        The importance of an exception being a "disconnect error" means that
        SQLAlchemy knows it can discard the connection and then reconnect.
        If the error is not a "disconnection error", then it raises.
        """
        connect_args = {}
        patchers = []
        db_disconnected = False

        class DisconnectCursorMixin:
            def execute(self, *arg, **kw):
                if db_disconnected:
                    raise exception
                else:
                    return super().execute(*arg, **kw)

        if dialect_name == "postgresql":
            import psycopg2.extensions

            class Curs(DisconnectCursorMixin, psycopg2.extensions.cursor):
                pass

            connect_args = {"cursor_factory": Curs}

        elif dialect_name == "mysql":
            import pymysql

            def fake_ping(self, *arg, **kw):
                if db_disconnected:
                    raise exception
                else:
                    return True

            class Curs(DisconnectCursorMixin, pymysql.cursors.Cursor):
                pass

            connect_args = {"cursorclass": Curs}

            patchers.append(
                mock.patch.object(
                    pymysql.Connection, "ping", fake_ping
                )
            )
        else:
            raise NotImplementedError()

        with mock.patch.object(
            compat,
            "native_pre_ping_event_support",
            self.native_pre_ping,
        ):
            engine = engines.create_engine(
                self.engine.url, max_retries=0)

        # 1.  override how we connect.   if we want the DB to be down
        # for the moment, but recover, reset db_disconnected after
        # connect is called.    If we want the DB to stay down, then
        # make sure connect raises the error also.
        @event.listens_for(engine, "do_connect")
        def _connect(dialect, connrec, cargs, cparams):
            nonlocal db_disconnected

            # while we're here, add our cursor classes to the DBAPI
            # connect args
            cparams.update(connect_args)

            if db_disconnected:
                if db_stays_down:
                    raise exception
                else:
                    db_disconnected = False

        # 2. initialize the dialect with a first connect
        conn = engine.connect()
        conn.close()

        # 3. add additional patchers
        patchers.extend([
            mock.patch.object(
                engine.dialect.dbapi,
                "Error",
                self.Error,
            ),
            mock.patch.object(
                engine.dialect,
                "is_disconnect",
                mock.Mock(return_value=is_disconnect),
            ),
        ])

        with test_utils.nested(*patchers):
            # "disconnect" the DB
            db_disconnected = True
            yield engine


class MySQLPrePingHandlerTests(
    db_test_base._MySQLOpportunisticTestCase,
    TestDBDisconnectedFixture,
):

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


class PostgreSQLPrePingHandlerTests(
        db_test_base._PostgreSQLOpportunisticTestCase,
        TestDBDisconnectedFixture):

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


if compat.sqla_2:
    class MySQLNativePrePingTests(MySQLPrePingHandlerTests):
        native_pre_ping = True

    class PostgreSQLNativePrePingTests(PostgreSQLPrePingHandlerTests):
        native_pre_ping = True


class TestDBConnectPingListener(TestsExceptionFilter):

    def setUp(self):
        super().setUp()
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
            self.assertEqual(1, conn.scalar(sqla.select(1)))
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
                return engines._test_connection(
                    engine, retries, .01, _close=False)

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
        self.assertEqual(1, conn.scalar(sqla.select(1)))

    def test_connect_retry_past_failure(self):
        conn = self._run_test(
            "mysql",
            self.OperationalError("Error: (2003) something wrong"),
            2, 3
        )
        # conn is good
        self.assertEqual(1, conn.scalar(sqla.select(1)))

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
