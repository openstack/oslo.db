# Copyright (c) 2017, Oracle and/or its affiliates. All rights reserved.
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
"""Tests for MySQL Cluster (NDB) Support."""

import logging
from unittest import mock

from oslo_db import exception
from oslo_db.sqlalchemy import enginefacade
from oslo_db.sqlalchemy import engines
from oslo_db.sqlalchemy import ndb
from oslo_db.sqlalchemy import test_fixtures
from oslo_db.sqlalchemy.types import String
from oslo_db.sqlalchemy import utils
from oslo_db.tests import base as test_base

from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy.dialects.mysql import TEXT
from sqlalchemy.dialects.mysql import TINYTEXT
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import schema
from sqlalchemy import Table
from sqlalchemy import Text

LOG = logging.getLogger(__name__)

_MOCK_CONNECTION = 'mysql+pymysql://'
_TEST_TABLE = Table("test_ndb", MetaData(),
                    Column('id', Integer, primary_key=True),
                    Column('test1', String(255, mysql_ndb_type=TEXT)),
                    Column('test2', String(4096, mysql_ndb_type=TEXT)),
                    Column('test3', String(255, mysql_ndb_length=64)),
                    mysql_engine='InnoDB')


class NDBMockTestBase(test_base.BaseTestCase):
    def setUp(self):
        super(NDBMockTestBase, self).setUp()
        mock_dbapi = mock.Mock()
        self.test_engine = test_engine = create_engine(
            _MOCK_CONNECTION, module=mock_dbapi)
        test_engine.dialect._oslodb_enable_ndb_support = True

        self.addCleanup(
            setattr, test_engine.dialect, "_oslodb_enable_ndb_support", False
        )
        ndb.init_ndb_events(test_engine)


class NDBEventTestCase(NDBMockTestBase):

    def test_ndb_createtable_override(self):
        test_engine = self.test_engine
        self.assertRegex(
            str(schema.CreateTable(_TEST_TABLE).compile(
                dialect=test_engine.dialect)),
            "ENGINE=NDBCLUSTER")

    def test_ndb_engine_override(self):
        test_engine = self.test_engine
        statement = "ENGINE=InnoDB"
        for fn in test_engine.dispatch.before_cursor_execute:
            statement, dialect = fn(
                mock.Mock(), mock.Mock(), statement, {}, mock.Mock(), False)
        self.assertEqual(statement, "ENGINE=NDBCLUSTER")

    def test_ndb_savepoint_override(self):
        test_engine = self.test_engine
        statement = "SAVEPOINT xyx"
        for fn in test_engine.dispatch.before_cursor_execute:
            statement, dialect = fn(
                mock.Mock(), mock.Mock(), statement, {}, mock.Mock(), False)
        self.assertEqual(statement,
                         "SET @oslo_db_ndb_savepoint_rollback_disabled = 0;")

    def test_ndb_rollback_override(self):
        test_engine = self.test_engine
        statement = "ROLLBACK TO SAVEPOINT xyz"
        for fn in test_engine.dispatch.before_cursor_execute:
            statement, dialect = fn(
                mock.Mock(), mock.Mock(), statement, {}, mock.Mock(), False)
        self.assertEqual(statement,
                         "SET @oslo_db_ndb_savepoint_rollback_disabled = 0;")

    def test_ndb_rollback_release_override(self):
        test_engine = self.test_engine
        statement = "RELEASE SAVEPOINT xyz"
        for fn in test_engine.dispatch.before_cursor_execute:
            statement, dialect = fn(
                mock.Mock(), mock.Mock(), statement, {}, mock.Mock(), False)
        self.assertEqual(statement,
                         "SET @oslo_db_ndb_savepoint_rollback_disabled = 0;")


class NDBDatatypesTestCase(NDBMockTestBase):
    def test_ndb_string_to_tinytext(self):
        test_engine = self.test_engine
        self.assertEqual("TINYTEXT",
                         str(String(255, mysql_ndb_type=TINYTEXT).compile(
                             dialect=test_engine.dialect)))

    def test_ndb_string_to_text(self):
        test_engine = self.test_engine
        self.assertEqual("TEXT",
                         str(String(4096, mysql_ndb_type=TEXT).compile(
                             dialect=test_engine.dialect)))

    def test_ndb_string_length(self):
        test_engine = self.test_engine
        self.assertEqual('VARCHAR(64)',
                         str(String(255, mysql_ndb_length=64).compile(
                             dialect=test_engine.dialect)))


class NDBDatatypesDefaultTestCase(NDBMockTestBase):
    def setUp(self):
        super(NDBMockTestBase, self).setUp()
        mock_dbapi = mock.Mock()
        self.test_engine = create_engine(_MOCK_CONNECTION, module=mock_dbapi)

    def test_non_ndb_string_to_text(self):
        test_engine = self.test_engine
        self.assertEqual("VARCHAR(255)",
                         str(String(255, mysql_ndb_type=TINYTEXT).compile(
                             dialect=test_engine.dialect)))

    def test_non_ndb_autostringtext(self):
        test_engine = self.test_engine
        self.assertEqual("VARCHAR(4096)",
                         str(String(4096, mysql_ndb_type=TEXT).compile(
                             dialect=test_engine.dialect)))

    def test_non_ndb_autostringsize(self):
        test_engine = self.test_engine
        self.assertEqual('VARCHAR(255)',
                         str(String(255, mysql_ndb_length=64).compile(
                             dialect=test_engine.dialect)))


class NDBOpportunisticTestCase(
    test_fixtures.OpportunisticDBTestMixin, test_base.BaseTestCase,
):

    FIXTURE = test_fixtures.MySQLOpportunisticFixture

    def init_db(self, use_ndb):
        # get the MySQL engine created by the opportunistic
        # provisioning system
        self.engine = enginefacade.writer.get_engine()
        if use_ndb:
            # if we want NDB, make a new local engine that uses the
            # URL / database / schema etc. of the provisioned engine,
            # since NDB-ness is a per-table thing
            self.engine = engines.create_engine(
                self.engine.url, mysql_enable_ndb=True
            )
            self.addCleanup(self.engine.dispose)
        self.test_table = _TEST_TABLE
        try:
            self.test_table.create(self.engine)
        except exception.DBNotSupportedError:
            self.skipTest("MySQL NDB Cluster not available")

    def test_ndb_enabled(self):
        self.init_db(True)
        self.assertTrue(ndb.ndb_status(self.engine))
        self.assertIsInstance(self.test_table.c.test1.type, TINYTEXT)
        self.assertIsInstance(self.test_table.c.test2.type, Text)
        self.assertIsInstance(self.test_table.c.test3.type, String)
        self.assertEqual(64, self.test_table.c.test3.type.length)
        self.assertEqual([], utils.get_non_ndbcluster_tables(self.engine))

    def test_ndb_disabled(self):
        self.init_db(False)
        self.assertFalse(ndb.ndb_status(self.engine))
        self.assertIsInstance(self.test_table.c.test1.type, String)
        self.assertEqual(255, self.test_table.c.test1.type.length)
        self.assertIsInstance(self.test_table.c.test2.type, String)
        self.assertEqual(4096, self.test_table.c.test2.type.length)
        self.assertIsInstance(self.test_table.c.test3.type, String)
        self.assertEqual(255, self.test_table.c.test3.type.length)
        self.assertEqual([], utils.get_non_innodb_tables(self.engine))
