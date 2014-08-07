# coding=utf-8

# Copyright (c) 2012 Rackspace Hosting
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

"""Unit tests for SQLAlchemy specific code."""
import logging

import fixtures
import mock
from oslo.config import cfg
from oslotest import base as oslo_test
import sqlalchemy
from sqlalchemy import Column, MetaData, Table
from sqlalchemy import Integer, String
from sqlalchemy.ext.declarative import declarative_base

from oslo.db import exception
from oslo.db import options as db_options
from oslo.db.sqlalchemy import models
from oslo.db.sqlalchemy import session
from oslo.db.sqlalchemy import test_base


BASE = declarative_base()
_TABLE_NAME = '__tmp__test__tmp__'

_REGEXP_TABLE_NAME = _TABLE_NAME + "regexp"


class RegexpTable(BASE, models.ModelBase):
    __tablename__ = _REGEXP_TABLE_NAME
    id = Column(Integer, primary_key=True)
    bar = Column(String(255))


class RegexpFilterTestCase(test_base.DbTestCase):

    def setUp(self):
        super(RegexpFilterTestCase, self).setUp()
        meta = MetaData()
        meta.bind = self.engine
        test_table = Table(_REGEXP_TABLE_NAME, meta,
                           Column('id', Integer, primary_key=True,
                                  nullable=False),
                           Column('bar', String(255)))
        test_table.create()
        self.addCleanup(test_table.drop)

    def _test_regexp_filter(self, regexp, expected):
        _session = self.sessionmaker()
        with _session.begin():
            for i in ['10', '20', u'♥']:
                tbl = RegexpTable()
                tbl.update({'bar': i})
                tbl.save(session=_session)

        regexp_op = RegexpTable.bar.op('REGEXP')(regexp)
        result = _session.query(RegexpTable).filter(regexp_op).all()
        self.assertEqual([r.bar for r in result], expected)

    def test_regexp_filter(self):
        self._test_regexp_filter('10', ['10'])

    def test_regexp_filter_nomatch(self):
        self._test_regexp_filter('11', [])

    def test_regexp_filter_unicode(self):
        self._test_regexp_filter(u'♥', [u'♥'])

    def test_regexp_filter_unicode_nomatch(self):
        self._test_regexp_filter(u'♦', [])


class FakeDBAPIConnection():
    def cursor(self):
        return FakeCursor()


class FakeCursor():
    def execute(self, sql):
        pass


class FakeConnectionProxy():
    pass


class FakeConnectionRec():
    pass


class OperationalError(Exception):
    pass


class ProgrammingError(Exception):
    pass


class FakeDB2Engine(object):

    class Dialect():

        def is_disconnect(self, e, *args):
            expected_error = ('SQL30081N: DB2 Server connection is no longer '
                              'active')
            return (str(e) == expected_error)

    dialect = Dialect()
    name = 'ibm_db_sa'

    def dispose(self):
        pass


class MySQLModeTestCase(test_base.MySQLOpportunisticTestCase):

    def __init__(self, *args, **kwargs):
        super(MySQLModeTestCase, self).__init__(*args, **kwargs)
        # By default, run in empty SQL mode.
        # Subclasses override this with specific modes.
        self.mysql_mode = ''

    def setUp(self):
        super(MySQLModeTestCase, self).setUp()

        self.engine = session.create_engine(self.engine.url,
                                            mysql_sql_mode=self.mysql_mode)
        self.connection = self.engine.connect()

        meta = MetaData()
        meta.bind = self.engine
        self.test_table = Table(_TABLE_NAME + "mode", meta,
                                Column('id', Integer, primary_key=True),
                                Column('bar', String(255)))
        self.test_table.create()

        self.addCleanup(self.test_table.drop)
        self.addCleanup(self.connection.close)

    def _test_string_too_long(self, value):
        with self.connection.begin():
            self.connection.execute(self.test_table.insert(),
                                    bar=value)
            result = self.connection.execute(self.test_table.select())
            return result.fetchone()['bar']

    def test_string_too_long(self):
        value = 'a' * 512
        # String is too long.
        # With no SQL mode set, this gets truncated.
        self.assertNotEqual(value,
                            self._test_string_too_long(value))


class MySQLStrictAllTablesModeTestCase(MySQLModeTestCase):
    "Test data integrity enforcement in MySQL STRICT_ALL_TABLES mode."

    def __init__(self, *args, **kwargs):
        super(MySQLStrictAllTablesModeTestCase, self).__init__(*args, **kwargs)
        self.mysql_mode = 'STRICT_ALL_TABLES'

    def test_string_too_long(self):
        value = 'a' * 512
        # String is too long.
        # With STRICT_ALL_TABLES or TRADITIONAL mode set, this is an error.
        self.assertRaises(exception.DBError,
                          self._test_string_too_long, value)


class MySQLTraditionalModeTestCase(MySQLStrictAllTablesModeTestCase):
    """Test data integrity enforcement in MySQL TRADITIONAL mode.

    Since TRADITIONAL includes STRICT_ALL_TABLES, this inherits all
    STRICT_ALL_TABLES mode tests.
    """

    def __init__(self, *args, **kwargs):
        super(MySQLTraditionalModeTestCase, self).__init__(*args, **kwargs)
        self.mysql_mode = 'TRADITIONAL'


class EngineFacadeTestCase(oslo_test.BaseTestCase):
    def setUp(self):
        super(EngineFacadeTestCase, self).setUp()

        self.facade = session.EngineFacade('sqlite://')

    def test_get_engine(self):
        eng1 = self.facade.get_engine()
        eng2 = self.facade.get_engine()

        self.assertIs(eng1, eng2)

    def test_get_session(self):
        ses1 = self.facade.get_session()
        ses2 = self.facade.get_session()

        self.assertIsNot(ses1, ses2)

    def test_get_session_arguments_override_default_settings(self):
        ses = self.facade.get_session(autocommit=False, expire_on_commit=True)

        self.assertFalse(ses.autocommit)
        self.assertTrue(ses.expire_on_commit)

    @mock.patch('oslo.db.sqlalchemy.session.get_maker')
    @mock.patch('oslo.db.sqlalchemy.session.create_engine')
    def test_creation_from_config(self, create_engine, get_maker):
        conf = cfg.ConfigOpts()
        conf.register_opts(db_options.database_opts, group='database')

        overrides = {
            'connection': 'sqlite:///:memory:',
            'slave_connection': None,
            'connection_debug': 100,
            'max_pool_size': 10,
            'mysql_sql_mode': 'TRADITIONAL',
        }
        for optname, optvalue in overrides.items():
            conf.set_override(optname, optvalue, group='database')

        session.EngineFacade.from_config(conf,
                                         autocommit=False,
                                         expire_on_commit=True)

        create_engine.assert_called_once_with(
            sql_connection='sqlite:///:memory:',
            connection_debug=100,
            max_pool_size=10,
            mysql_sql_mode='TRADITIONAL',
            sqlite_fk=False,
            idle_timeout=mock.ANY,
            retry_interval=mock.ANY,
            max_retries=mock.ANY,
            max_overflow=mock.ANY,
            connection_trace=mock.ANY,
            sqlite_synchronous=mock.ANY,
            pool_timeout=mock.ANY,
            thread_checkin=mock.ANY,
        )
        get_maker.assert_called_once_with(engine=create_engine(),
                                          autocommit=False,
                                          expire_on_commit=True)

    def test_slave_connection(self):
        paths = self.create_tempfiles([('db.master', ''), ('db.slave', '')],
                                      ext='')
        master_path = 'sqlite:///' + paths[0]
        slave_path = 'sqlite:///' + paths[1]

        facade = session.EngineFacade(
            sql_connection=master_path,
            slave_connection=slave_path
        )

        master = facade.get_engine()
        self.assertEqual(master_path, str(master.url))
        slave = facade.get_engine(use_slave=True)
        self.assertEqual(slave_path, str(slave.url))

        master_session = facade.get_session()
        self.assertEqual(master_path, str(master_session.bind.url))
        slave_session = facade.get_session(use_slave=True)
        self.assertEqual(slave_path, str(slave_session.bind.url))

    def test_slave_connection_string_not_provided(self):
        master_path = 'sqlite:///' + self.create_tempfiles(
            [('db.master', '')], ext='')[0]

        facade = session.EngineFacade(sql_connection=master_path)

        master = facade.get_engine()
        slave = facade.get_engine(use_slave=True)
        self.assertIs(master, slave)
        self.assertEqual(master_path, str(master.url))

        master_session = facade.get_session()
        self.assertEqual(master_path, str(master_session.bind.url))
        slave_session = facade.get_session(use_slave=True)
        self.assertEqual(master_path, str(slave_session.bind.url))


class MysqlSetCallbackTest(oslo_test.BaseTestCase):

    class FakeCursor(object):
        def __init__(self, execs):
            self._execs = execs

        def execute(self, sql, arg):
            self._execs.append(sql % arg)

    class FakeDbapiCon(object):
        def __init__(self, execs):
            self._execs = execs

        def cursor(self):
            return MysqlSetCallbackTest.FakeCursor(self._execs)

    class FakeResultSet(object):
        def __init__(self, realmode):
            self._realmode = realmode

        def fetchone(self):
            return ['ignored', self._realmode]

    class FakeEngine(object):
        def __init__(self, realmode=None):
            self._cbs = {}
            self._execs = []
            self._realmode = realmode
            self._connected = False

        def set_callback(self, name, cb):
            self._cbs[name] = cb

        def connect(self, **kwargs):
            cb = self._cbs.get('connect', lambda *x, **y: None)
            dbapi_con = MysqlSetCallbackTest.FakeDbapiCon(self._execs)
            connection_rec = None  # Not used.
            cb(dbapi_con, connection_rec)

        def execute(self, sql):
            if not self._connected:
                self.connect()
                self._connected = True
            self._execs.append(sql)
            return MysqlSetCallbackTest.FakeResultSet(self._realmode)

    def stub_listen(engine, name, cb):
        engine.set_callback(name, cb)

    @mock.patch.object(sqlalchemy.event, 'listen', side_effect=stub_listen)
    def _call_set_callback(self, listen_mock, sql_mode=None, realmode=None):
        engine = self.FakeEngine(realmode=realmode)

        self.stream = self.useFixture(fixtures.FakeLogger(
            format="%(levelname)8s [%(name)s] %(message)s",
            level=logging.DEBUG,
            nuke_handlers=True
        ))

        session._mysql_set_mode_callback(engine, sql_mode=sql_mode)
        return engine

    def test_set_mode_traditional(self):
        # If _mysql_set_mode_callback is called with an sql_mode, then the SQL
        # mode is set on the connection.

        engine = self._call_set_callback(sql_mode='TRADITIONAL')

        exp_calls = [
            "SET SESSION sql_mode = ['TRADITIONAL']",
            "SHOW VARIABLES LIKE 'sql_mode'"
        ]
        self.assertEqual(exp_calls, engine._execs)

    def test_set_mode_ansi(self):
        # If _mysql_set_mode_callback is called with an sql_mode, then the SQL
        # mode is set on the connection.

        engine = self._call_set_callback(sql_mode='ANSI')

        exp_calls = [
            "SET SESSION sql_mode = ['ANSI']",
            "SHOW VARIABLES LIKE 'sql_mode'"
        ]
        self.assertEqual(exp_calls, engine._execs)

    def test_set_mode_no_mode(self):
        # If _mysql_set_mode_callback is called with sql_mode=None, then
        # the SQL mode is NOT set on the connection.

        engine = self._call_set_callback()

        exp_calls = [
            "SHOW VARIABLES LIKE 'sql_mode'"
        ]
        self.assertEqual(exp_calls, engine._execs)

    def test_fail_detect_mode(self):
        # If "SHOW VARIABLES LIKE 'sql_mode'" results in no row, then
        # we get a log indicating can't detect the mode.

        self._call_set_callback()

        self.assertIn('Unable to detect effective SQL mode',
                      self.stream.output)

    def test_logs_real_mode(self):
        # If "SHOW VARIABLES LIKE 'sql_mode'" results in a value, then
        # we get a log with the value.

        self._call_set_callback(realmode='SOMETHING')

        self.assertIn('MySQL server mode set to SOMETHING',
                      self.stream.output)

    def test_warning_when_not_traditional(self):
        # If "SHOW VARIABLES LIKE 'sql_mode'" results in a value that doesn't
        # include 'TRADITIONAL', then a warning is logged.

        self._call_set_callback(realmode='NOT_TRADIT')

        self.assertIn("consider enabling TRADITIONAL or STRICT_ALL_TABLES",
                      self.stream.output)

    def test_no_warning_when_traditional(self):
        # If "SHOW VARIABLES LIKE 'sql_mode'" results in a value that includes
        # 'TRADITIONAL', then no warning is logged.

        self._call_set_callback(realmode='TRADITIONAL')

        self.assertNotIn("consider enabling TRADITIONAL or STRICT_ALL_TABLES",
                         self.stream.output)

    def test_no_warning_when_strict_all_tables(self):
        # If "SHOW VARIABLES LIKE 'sql_mode'" results in a value that includes
        # 'STRICT_ALL_TABLES', then no warning is logged.

        self._call_set_callback(realmode='STRICT_ALL_TABLES')

        self.assertNotIn("consider enabling TRADITIONAL or STRICT_ALL_TABLES",
                         self.stream.output)

    def test_multiple_executes(self):
        # We should only set the sql_mode on a connection once.

        engine = self._call_set_callback(sql_mode='TRADITIONAL',
                                         realmode='TRADITIONAL')

        engine.execute('SELECT * FROM foo')
        engine.execute('SELECT * FROM bar')

        exp_calls = [
            "SET SESSION sql_mode = ['TRADITIONAL']",
            "SHOW VARIABLES LIKE 'sql_mode'",
            "SELECT * FROM foo",
            "SELECT * FROM bar",
        ]
        self.assertEqual(exp_calls, engine._execs)


class PatchStacktraceTest(test_base.DbTestCase):

    def test_trace(self):
        engine = self.engine
        engine.connect()
        with mock.patch.object(engine.dialect, "do_execute") as mock_exec:
            session._add_trace_comments(engine)

            engine.execute("select * from table")

        call = mock_exec.mock_calls[0]

        # we're the caller, see that we're in there
        self.assertTrue("tests/sqlalchemy/test_sqlalchemy.py" in call[1][1])
