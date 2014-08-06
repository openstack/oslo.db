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
import six
import sqlalchemy as sqla
from sqlalchemy.orm import mapper

from oslo.db import exception
from oslo.db.sqlalchemy import session
from oslo.db.sqlalchemy import test_base

_TABLE_NAME = '__tmp__test__tmp__'


class TestsExceptionFilter(test_base.DbTestCase):

    class Error(Exception):
        """DBAPI base error.

        This exception and subclasses are used in a mock context
        within these tests.

        """

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

    @contextlib.contextmanager
    def _dbapi_fixture(self, dialect_name):
        engine = self.engine
        with contextlib.nested(
            mock.patch.object(engine.dialect.dbapi,
                              "Error",
                              self.Error),
            mock.patch.object(engine.dialect, "name", dialect_name),
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

        with contextlib.nested(
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
        self.assertEqual(
            "(ProgrammingError) Error 123, you made a "
            "mistake 'select you_made_a_programming_error' ()",
            matched.args[0])

    def test_generic_dbapi_disconnect(self):
        matched = self._run_test(
            "mysql", "select the_db_disconnected",
            self.InterfaceError("connection lost"),
            exception.DBConnectionError,
            is_disconnect=True
        )
        self.assertEqual(
            "(InterfaceError) connection lost "
            "'select the_db_disconnected' ()",
            matched.args[0])

    def test_operational_dbapi_disconnect(self):
        matched = self._run_test(
            "mysql", "select the_db_disconnected",
            self.OperationalError("connection lost"),
            exception.DBConnectionError,
            is_disconnect=True
        )
        self.assertEqual(
            "(OperationalError) connection lost "
            "'select the_db_disconnected' ()",
            matched.args[0])

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
        self.assertEqual(
            "(OperationalError) some op error",
            matched.args[0])

    def test_unicode_encode(self):
        # intentionally generate a UnicodeEncodeError, as its
        # constructor is quite complicated and seems to be non-public
        # or at least not documented anywhere.
        try:
            six.u('\u2435').encode('ascii')
        except UnicodeEncodeError as uee:
            pass

        self._run_test(
            "postgresql", six.u('select \u2435'),
            uee,
            exception.DBInvalidUnicodeParameter
            )

    def test_garden_variety(self):
        matched = self._run_test(
            "mysql", "select some_thing_that_breaks",
            AttributeError("mysqldb has an attribute error"),
            exception.DBError
        )
        self.assertEqual("mysqldb has an attribute error", matched.args[0])


class TestRaiseReferenceError(TestsExceptionFilter):
    def test_postgresql(self):
        e = self._run_test(
            "postgresql",
            "INSERT SOMETHING",
            self.IntegrityError(
                "insert or update on table "
                "\"resource_entity\" "
                "violates foreign key constraint "
                "\"resource_entity_entity_id_fkey\"\n"
                "DETAIL:  Key "
                "(entity_id)=(74b5da71-5a9c-4f89-a8e9-4a2d856e6c29) "
                "is not present in table \"entity\".\n"
                "'INSERT INTO resource_entity (resource_id, entity_id, name) "
                "VALUES (%(resource_id)s, "
                "%(entity_id)s, %(name)s)' "
                "{'entity_id': '74b5da71-5a9c-4f89-a8e9-4a2d856e6c29', "
                "'name': u'foo', "
                "'resource_id': 'ffb12cb4-d955-4d96-a315-5f48ea161eef'}"),
            exception.DBReferenceError,
        )
        self.assertEqual("resource_entity", e.table)
        self.assertEqual("resource_entity_entity_id_fkey", e.constraint)
        self.assertEqual("entity_id", e.key)
        self.assertEqual("entity", e.key_table)
        self.assertEqual(
            "(IntegrityError) insert or update on table "
            "\"resource_entity\" violates foreign key constraint "
            "\"resource_entity_entity_id_fkey\"\n"
            "DETAIL:  Key (entity_id)=(74b5da71-5a9c-4f89-a8e9-4a2d856e6c29) "
            "is not present in table \"entity\".\n"
            "'INSERT INTO resource_entity (resource_id, entity_id, name) "
            "VALUES (%(resource_id)s, %(entity_id)s, %(name)s)' "
            "{'entity_id': '74b5da71-5a9c-4f89-a8e9-4a2d856e6c29', "
            "'name': u'foo', "
            "'resource_id': 'ffb12cb4-d955-4d96-a315-5f48ea161eef'} "
            "'INSERT SOMETHING' ()",
            str(e))

    def test_mysql(self):
        e = self._run_test(
            "mysql",
            "INSERT SOMETHING",
            self.IntegrityError(
                "Cannot add or update a child row: "
                "a foreign key constraint fails "
                "(resource_entity, CONSTRAINT resource_entity_entity_id_fkey "
                "FOREIGN KEY (entity_id) "
                "REFERENCES entity (entity_id))"
            ),
            exception.DBReferenceError,
        )
        self.assertEqual("resource_entity", e.table)
        self.assertEqual("resource_entity_entity_id_fkey", e.constraint)
        self.assertEqual("entity_id", e.key)
        self.assertEqual("entity", e.key_table)
        self.assertEqual(
            "(IntegrityError) Cannot add or update a child row: "
            "a foreign key constraint fails "
            "(resource_entity, CONSTRAINT resource_entity_entity_id_fkey "
            "FOREIGN KEY (entity_id) REFERENCES entity (entity_id)) "
            "'INSERT SOMETHING' ()",
            str(e))

    def test_sqlite(self):
        e = self._run_test(
            "sqlite",
            "INSERT SOMETHING",
            self.IntegrityError(
                "SQL error: foreign key constraint failed"
            ),
            exception.DBReferenceError,
        )
        self.assertIsNone(e.table)
        self.assertIsNone(e.constraint)
        self.assertIsNone(e.key)
        self.assertIsNone(e.key_table)
        self.assertEqual(
            "(IntegrityError) SQL error: foreign key "
            "constraint failed 'INSERT SOMETHING' ()",
            str(e))


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
                                  expected_cls, expected_message):
        matched = self._run_test(
            dialect_name, statement,
            self.IntegrityError(message),
            expected_cls
        )
        self.assertEqual(expected_message, matched.args[0])

    def test_sqlite(self):
        self._run_dupe_constraint_test("sqlite", 'column a, b are not unique')

    def test_sqlite_3_7_16_or_3_8_2_and_higher(self):
        self._run_dupe_constraint_test(
            "sqlite",
            'UNIQUE constraint failed: tbl.a, tbl.b')

    def test_mysql_mysqldb(self):
        self._run_dupe_constraint_test(
            "mysql",
            '(1062, "Duplicate entry '
            '\'2-3\' for key \'uniq_tbl0a0b\'")', expected_value='2-3')

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
            exception.DBError,
            "(IntegrityError) constraint violation "
            "'insert into table some_values' ()"
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
            exception.DBError,
            '(IntegrityError) SQL0542N  The column named "NAME" cannot be a '
            'column of a primary key or unique key constraint because it can '
            'contain null values. \'ALTER TABLE instance_types ADD CONSTRAINT '
            'uniq_name_x_deleted UNIQUE (name, deleted)\' ()'
        )


class TestDeadlock(TestsExceptionFilter):
    def _run_deadlock_detect_test(
        self, dialect_name, message,
        orig_exception_cls=TestsExceptionFilter.OperationalError):
        statement = ('SELECT quota_usages.created_at AS '
                     'quota_usages_created_at FROM quota_usages \n'
                     'WHERE quota_usages.project_id = %(project_id_1)s '
                     'AND quota_usages.deleted = %(deleted_1)s FOR UPDATE')
        params = {
            'project_id_1': '8891d4478bbf48ad992f050cdf55e9b5',
            'deleted_1': 0
        }
        self._run_test(
            dialect_name, statement,
            orig_exception_cls(message),
            exception.DBDeadlock,
            params=params
        )

    def _not_deadlock_test(
        self, dialect_name, message,
        expected_cls, expected_message,
        orig_exception_cls=TestsExceptionFilter.OperationalError):
        statement = ('SELECT quota_usages.created_at AS '
                     'quota_usages_created_at FROM quota_usages \n'
                     'WHERE quota_usages.project_id = %%(project_id_1)s '
                     'AND quota_usages.deleted = %%(deleted_1)s FOR UPDATE')
        params = {
            'project_id_1': '8891d4478bbf48ad992f050cdf55e9b5',
            'deleted_1': 0
        }
        matched = self._run_test(
            dialect_name, statement,
            orig_exception_cls(message),
            expected_cls,
            params=params
        )
        self.assertEqual(
            expected_message % {'statement': statement, 'params': params},
            str(matched)
        )

    def test_mysql_mysqldb_deadlock(self):
        self._run_deadlock_detect_test(
            "mysql",
            "(1213, 'Deadlock found when trying "
            "to get lock; try restarting "
            "transaction')"
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
            "(OperationalError) (1005, 'some other error') "
            "%(statement)r %(params)r"
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
            '(TransactionRollbackError) '
            'relation "fake" does not exist %(statement)r %(params)r',
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
            "(Error) SQL01234B Some other error. %(statement)r %(params)r",
            orig_exception_cls=self.Error
        )


class IntegrationTest(test_base.DbTestCase):
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
        self.assertTrue("no such function" in str(matched))

    def test_query_wrapper_operational_error(self):
        """test an operational error from query.all() raised as-is."""

        _session = self.sessionmaker()

        _session.begin()
        self.addCleanup(_session.rollback)
        q = _session.query(self.Foo).filter(
            self.Foo.counter == sqla.func.imfake(123))
        matched = self.assertRaises(sqla.exc.OperationalError, q.all)
        self.assertTrue("no such function" in str(matched))


class TestDBDisconnected(TestsExceptionFilter):

    @contextlib.contextmanager
    def _fixture(self, dialect_name, exception, num_disconnects):
        engine = self.engine

        real_do_execute = engine.dialect.do_execute
        counter = itertools.count(1)

        def fake_do_execute(self, *arg, **kw):
            if next(counter) > num_disconnects:
                return real_do_execute(self, *arg, **kw)
            else:
                raise exception

        with self._dbapi_fixture(dialect_name):
            with contextlib.nested(
                mock.patch.object(engine.dialect,
                                  "do_execute",
                                  fake_do_execute),
                mock.patch.object(engine.dialect,
                                  "is_disconnect",
                                  mock.Mock(return_value=True))
            ):
                yield

    def _test_ping_listener_disconnected(self, dialect_name, exc_obj):
        with self._fixture(dialect_name, exc_obj, 1):
            conn = self.engine.connect()
            with conn.begin():
                self.assertEqual(conn.scalar(sqla.select([1])), 1)
                self.assertFalse(conn.closed)
                self.assertFalse(conn.invalidated)
                self.assertTrue(conn.in_transaction())

        with self._fixture(dialect_name, exc_obj, 2):
            conn = self.engine.connect()
            self.assertRaises(
                exception.DBConnectionError,
                conn.begin
            )
            self.assertFalse(conn.closed)
            self.assertFalse(conn.in_transaction())
            self.assertTrue(conn.invalidated)

    def test_mysql_ping_listener_disconnected(self):
        for code in [2006, 2013, 2014, 2045, 2055]:
            self._test_ping_listener_disconnected(
                "mysql",
                self.OperationalError('%d MySQL server has gone away' % code)
            )

    def test_db2_ping_listener_disconnected(self):
        self._test_ping_listener_disconnected(
            "ibm_db_sa",
            self.OperationalError(
                'SQL30081N: DB2 Server connection is no longer active')
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
                return session._test_connection(engine, retries, .01)

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
        self.assertEqual(conn.scalar(sqla.select([1])), 1)

    def test_connect_retry_past_failure(self):
        conn = self._run_test(
            "mysql",
            self.OperationalError("Error: (2003) something wrong"),
            2, 3
        )
        # conn is good
        self.assertEqual(conn.scalar(sqla.select([1])), 1)

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
        self.assertEqual(conn.scalar(sqla.select([1])), 1)

    def test_db2_error_negative(self):
        self.assertRaises(
            sqla.exc.OperationalError,
            self._run_test,
            "ibm_db_sa",
            self.OperationalError("blah blah -39981 blah blah"),
            2, 3
        )
