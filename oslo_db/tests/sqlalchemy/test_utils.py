# Copyright (c) 2013 Boris Pavlovic (boris@pavlovic.me).
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

from unittest import mock
from urllib import parse

import fixtures
import sqlalchemy
from sqlalchemy import Boolean, Index, Integer, DateTime, String
from sqlalchemy import MetaData, Table, Column
from sqlalchemy import ForeignKey, ForeignKeyConstraint
from sqlalchemy.dialects.postgresql import psycopg2
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import column_property
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import registry
from sqlalchemy.orm import Session
from sqlalchemy import sql
from sqlalchemy.sql.expression import cast
from sqlalchemy.sql import select
from sqlalchemy.types import UserDefinedType

from oslo_db import exception
from oslo_db.sqlalchemy import models
from oslo_db.sqlalchemy import provision
from oslo_db.sqlalchemy import session
from oslo_db.sqlalchemy import utils
from oslo_db.tests import base as test_base
from oslo_db.tests.sqlalchemy import base as db_test_base

Base = declarative_base()


class TestSanitizeDbUrl(test_base.BaseTestCase):

    def test_url_with_cred(self):
        db_url = 'myproto://johndoe:secret@localhost/myschema'
        expected = 'myproto://****:****@localhost/myschema'
        actual = utils.sanitize_db_url(db_url)
        self.assertEqual(expected, actual)

    def test_url_with_no_cred(self):
        db_url = 'sqlite:///mysqlitefile'
        actual = utils.sanitize_db_url(db_url)
        self.assertEqual(db_url, actual)


class CustomType(UserDefinedType):
    """Dummy column type for testing unsupported types."""
    def get_col_spec(self):
        return "CustomType"


class FakeTable(Base):
    __tablename__ = 'fake_table'

    user_id = Column(String(50), primary_key=True)
    project_id = Column(String(50))
    snapshot_id = Column(String(50))
    updated_at = Column(DateTime, nullable=True)
    enabled = Column(Boolean, default=True)

    _some_hybrid = column_property(project_id + snapshot_id)

    @hybrid_property
    def some_hybrid(self):
        raise NotImplementedError()

    @some_hybrid.expression
    def some_hybrid(cls):
        return cls._some_hybrid

    def foo(self):
        pass


class FakeTableJoinedInh(FakeTable):
    __tablename__ = 'fake_table_inh'

    id = Column(String(50), ForeignKey('fake_table.user_id'), primary_key=True)


class FakeTableSingleInh(FakeTable):
    __mapper_args__ = {'polymorphic_identity': 'foo'}


class FakeTableWithMultipleKeys(Base):
    __tablename__ = 'fake_table_multiple_keys'

    key1 = Column(String(50), primary_key=True)
    key2 = Column(String(50), primary_key=True)
    key3 = Column(String(50))


class FakeTableWithIndexes(Base):
    __tablename__ = 'fake_table_unique_index'

    id = Column(String(50), primary_key=True)
    key1 = Column(String(50))
    key2 = Column(String(50))
    key3 = Column(String(50))

    __table_args__ = (
        Index('idx_unique', 'key1', 'key2', unique=True),
        Index('idx_unique', 'key1', 'key3', unique=False),
    )


class FakeTableClassicalyMapped:
    pass


fake_table = Table(
    'fake_table_classically_mapped',
    Base.metadata,
    Column('id', Integer, primary_key=True),
    Column('key', String(50))
)

reg = registry()
reg.map_imperatively(FakeTableClassicalyMapped, fake_table)


class FakeModel:
    def __init__(self, values):
        self.values = values

    def __getattr__(self, name):
        try:
            value = self.values[name]
        except KeyError:
            raise AttributeError(name)
        return value

    def __getitem__(self, key):
        if key in self.values:
            return self.values[key]
        else:
            raise NotImplementedError()

    def __repr__(self):
        return '<FakeModel: %s>' % self.values


class TestPaginateQuery(test_base.BaseTestCase):
    def setUp(self):
        super().setUp()

        self.query = mock.Mock()
        self.mock_asc = self.useFixture(
            fixtures.MockPatchObject(sqlalchemy, 'asc')).mock
        self.mock_desc = self.useFixture(
            fixtures.MockPatchObject(sqlalchemy, 'desc')).mock

        self.marker = FakeTable(user_id='user',
                                project_id='p',
                                snapshot_id='s',
                                updated_at=None)
        self.model = FakeTable

    def test_paginate_query_no_pagination_no_sort_dirs(self):
        self.query.order_by.return_value = self.query
        self.mock_asc.side_effect = [
            'asc_3', 'asc_2', 'asc_1'
        ]

        utils.paginate_query(self.query, self.model, 5,
                             ['user_id', 'project_id', 'snapshot_id'])

        self.mock_asc.assert_has_calls([
            mock.call(self.model.user_id),
            mock.call(self.model.project_id),
            mock.call(self.model.snapshot_id),
        ])
        self.query.order_by.assert_has_calls([
            mock.call('asc_3'),
            mock.call('asc_2'),
            mock.call('asc_1'),
        ])
        self.query.limit.assert_called_once_with(5)

    def test_paginate_query_no_pagination(self):
        self.query.order_by.return_value = self.query
        self.mock_asc.side_effect = ['asc']
        self.mock_desc.side_effect = ['desc']

        utils.paginate_query(self.query, self.model, 5,
                             ['user_id', 'project_id'],
                             sort_dirs=['asc', 'desc'])

        self.mock_asc.assert_called_once_with(self.model.user_id)
        self.mock_desc.assert_called_once_with(self.model.project_id)
        self.query.order_by.assert_has_calls([
            mock.call('asc'),
            mock.call('desc'),
        ])
        self.query.limit.assert_called_once_with(5)

    def test_invalid_sort_key_str(self):
        self.assertEqual("Sort key supplied is invalid: None",
                         str(exception.InvalidSortKey()))
        self.assertEqual("Sort key supplied is invalid: lol",
                         str(exception.InvalidSortKey("lol")))

    def test_invalid_unicode_paramater_str(self):
        self.assertEqual(
            "Invalid Parameter: Encoding directive wasn't provided.",
            str(exception.DBInvalidUnicodeParameter()))

    def test_paginate_query_attribute_error(self):
        self.mock_asc.return_value = 'asc'

        self.assertRaises(exception.InvalidSortKey,
                          utils.paginate_query, self.query,
                          self.model, 5, ['user_id', 'non-existent key'])

        self.mock_asc.assert_called_once_with(self.model.user_id)
        self.query.order_by.assert_called_once_with('asc')

    def test_paginate_query_attribute_error_invalid_sortkey(self):
        self.assertRaises(exception.InvalidSortKey,
                          utils.paginate_query, self.query,
                          self.model, 5, ['bad_user_id'])

    def test_paginate_query_attribute_error_invalid_sortkey_2(self):
        self.assertRaises(exception.InvalidSortKey,
                          utils.paginate_query, self.query,
                          self.model, 5, ['foo'])

    def test_paginate_query_attribute_error_invalid_sortkey_3(self):
        self.assertRaises(exception.InvalidSortKey,
                          utils.paginate_query, self.query,
                          self.model, 5, ['asc-nullinvalid'])

    def test_paginate_query_assertion_error(self):
        self.assertRaises(AssertionError,
                          utils.paginate_query, self.query,
                          self.model, 5, ['user_id'],
                          marker=self.marker,
                          sort_dir='asc', sort_dirs=['asc'])

    def test_paginate_query_assertion_error_2(self):
        self.assertRaises(AssertionError,
                          utils.paginate_query, self.query,
                          self.model, 5, ['user_id'],
                          marker=self.marker,
                          sort_dir=None, sort_dirs=['asc', 'desk'])

    @mock.patch.object(sqlalchemy.sql, 'and_')
    @mock.patch.object(sqlalchemy.sql, 'or_')
    def test_paginate_query(self, mock_or, mock_and):
        self.query.order_by.return_value = self.query
        self.query.filter.return_value = self.query
        self.mock_asc.return_value = 'asc_1'
        self.mock_desc.return_value = 'desc_1'
        mock_and.side_effect = ['some_crit', 'another_crit']
        mock_or.return_value = 'some_f'

        utils.paginate_query(self.query, self.model, 5,
                             ['user_id', 'project_id'],
                             marker=self.marker,
                             sort_dirs=['asc', 'desc'])

        self.mock_asc.assert_called_once_with(self.model.user_id)
        self.mock_desc.assert_called_once_with(self.model.project_id)
        self.query.order_by.assert_has_calls([
            mock.call('asc_1'),
            mock.call('desc_1'),
        ])
        mock_and.assert_has_calls([
            mock.call(mock.ANY),
            mock.call(mock.ANY, mock.ANY)
        ])
        mock_or.assert_called_once_with('some_crit', 'another_crit')
        self.query.filter.assert_called_once_with('some_f')
        self.query.limit.assert_called_once_with(5)

    @mock.patch.object(sqlalchemy.sql, 'and_')
    @mock.patch.object(sqlalchemy.sql, 'or_')
    def test_paginate_query_null(self, mock_or, mock_and):
        self.query.order_by.return_value = self.query
        self.query.filter.return_value = self.query
        self.mock_desc.side_effect = [
            'asc_null_2',
            'desc_null_2',
            'desc_1',
        ]
        self.mock_asc.side_effect = [
            'asc_1'
        ]
        mock_or.side_effect = [
            'or_1',
            'or_2',
            'some_f',
        ]
        mock_and.side_effect = [
            'some_crit',
            'another_crit',
        ]

        with mock.patch.object(
            self.model.user_id.comparator.expression, 'is_not'
        ) as mock_is_not, \
                mock.patch.object(
                    self.model.user_id.comparator.expression, 'is_'
                ) as mock_is_a, \
                mock.patch.object(
                    self.model.project_id.comparator.expression, 'is_'
                ) as mock_is_b:
            mock_is_not.return_value = 'asc_null_1'
            mock_is_a.side_effect = [
                'desc_null_filter_1',
                'desc_null_filter_2',
            ]
            mock_is_b.side_effect = [
                'desc_null_1',
                'asc_null_filter',
            ]

            utils.paginate_query(self.query, self.model, 5,
                                 ['user_id', 'project_id'],
                                 marker=self.marker,
                                 sort_dirs=[
                                     'asc-nullslast', 'desc-nullsfirst'])

            mock_is_not.assert_called_once_with(None)
            mock_is_a.assert_has_calls([
                mock.call(None),
                mock.call(None),
            ])
            mock_is_b.assert_has_calls([
                mock.call(None),
                mock.call(None),
            ])

        self.mock_desc.assert_has_calls([
            mock.call('asc_null_1'),
            mock.call('desc_null_1'),
            mock.call(self.model.project_id),
        ])
        self.mock_asc.assert_has_calls([
            mock.call(self.model.user_id),
        ])
        mock_or.assert_has_calls([
            mock.call(mock.ANY, 'desc_null_filter_2'),
            mock.call(mock.ANY, 'asc_null_filter'),
            mock.call('some_crit', 'another_crit'),
        ])
        mock_and.assert_has_calls([
            mock.call('or_1'),
            mock.call(mock.ANY, 'or_2'),
        ])

        self.query.order_by.assert_has_calls([
            mock.call('asc_null_2'),
            mock.call('asc_1'),
            mock.call('desc_null_2'),
            mock.call('desc_1'),
        ])
        self.query.filter.assert_called_once_with('some_f')
        self.query.limit.assert_called_once_with(5)

    @mock.patch.object(sqlalchemy.sql, 'and_')
    @mock.patch.object(sqlalchemy.sql, 'or_')
    def test_paginate_query_marker_null(self, mock_or, mock_and):

        self.mock_asc.side_effect = [
            'asc_1'
        ]
        self.mock_desc.side_effect = [
            'asc_null_2',
            'desc_null_2',
            'desc_1',
        ]
        self.query.order_by.return_value = self.query
        self.query.filter.return_value = self.query
        mock_and.return_value = 'some_crit'
        mock_or.side_effect = ['or_1', 'some_f']

        with mock.patch.object(
            self.model.user_id.comparator.expression, 'is_not'
        ) as mock_is_not, \
                mock.patch.object(
                    self.model.updated_at.comparator.expression, 'is_'
                ) as mock_is_a, \
                mock.patch.object(
                    self.model.user_id.comparator.expression, 'is_'
                ) as mock_is_b:

            mock_is_not.return_value = 'asc_null_1'
            mock_is_a.return_value = 'desc_null_1'
            mock_is_b.side_effect = ['asc_null_filter_1', 'asc_null_filter_2']

            utils.paginate_query(self.query, self.model, 5,
                                 ['user_id', 'updated_at'],
                                 marker=self.marker,
                                 sort_dirs=[
                                     'asc-nullslast', 'desc-nullsfirst'])
            mock_is_not.assert_called_once_with(None)
            mock_is_a.assert_called_once_with(None)
            mock_is_b.assert_has_calls([mock.call(None), mock.call(None)])

        self.mock_asc.assert_called_once_with(self.model.user_id)
        self.mock_desc.assert_has_calls([
            mock.call('asc_null_1'),
            mock.call('desc_null_1'),
            mock.call(self.model.updated_at),
        ])
        mock_and.assert_called_once_with('or_1')
        mock_or.assert_has_calls([
            mock.call(mock.ANY, 'asc_null_filter_2'),
            mock.call('some_crit'),
        ])
        self.query.order_by.assert_has_calls([
            mock.call('asc_null_2'),
            mock.call('asc_1'),
            mock.call('desc_null_2'),
            mock.call('desc_1'),
        ])
        self.query.filter.assert_called_once_with('some_f')
        self.query.limit.assert_called_once_with(5)

    @mock.patch.object(sqlalchemy.sql, 'and_')
    @mock.patch.object(sqlalchemy.sql, 'or_')
    def test_paginate_query_marker_null_with_two_primary_keys(
            self, mock_or, mock_and):
        self.mock_asc.return_value = 'asc_1'
        self.mock_desc.side_effect = [
            'asc_null_2',
            'desc_null_2',
            'desc_1',
            'desc_null_4',
            'desc_4',
        ]
        self.query.order_by.return_value = self.query
        mock_or.side_effect = [
            'or_1',
            'or_2',
            'some_f',
        ]
        mock_and.side_effect = [
            'some_crit',
            'other_crit',
        ]
        self.query.filter.return_value = self.query

        with mock.patch.object(
            self.model.user_id.comparator.expression, 'is_not'
        ) as mock_is_not, \
                mock.patch.object(
                    self.model.updated_at.comparator.expression, 'is_'
                ) as mock_is_a, \
                mock.patch.object(
                    self.model.user_id.comparator.expression, 'is_'
                ) as mock_is_b, \
                mock.patch.object(
                    self.model.project_id.comparator.expression, 'is_'
                ) as mock_is_c:

            mock_is_not.return_value = 'asc_null_1'
            mock_is_a.return_value = 'desc_null_1'
            mock_is_b.side_effect = ['asc_null_filter_1', 'asc_null_filter_2']
            mock_is_c.side_effect = ['desc_null_3', 'desc_null_filter_3']

            utils.paginate_query(self.query, self.model, 5,
                                 ['user_id', 'updated_at', 'project_id'],
                                 marker=self.marker,
                                 sort_dirs=['asc-nullslast', 'desc-nullsfirst',
                                            'desc-nullsfirst'])

            mock_is_not.assert_called_once_with(None)
            mock_is_a.assert_called_once_with(None)
            mock_is_b.assert_has_calls([mock.call(None), mock.call(None)])
            mock_is_c.assert_has_calls([mock.call(None), mock.call(None)])

        self.mock_asc.assert_called_once_with(self.model.user_id)
        self.mock_desc.assert_has_calls([
            mock.call('asc_null_1'),
            mock.call('desc_null_1'),
            mock.call(self.model.updated_at),
            mock.call('desc_null_3'),
            mock.call(self.model.project_id),
        ])
        self.query.order_by.assert_has_calls([
            mock.call('asc_null_2'),
            mock.call('asc_1'),
            mock.call('desc_null_2'),
            mock.call('desc_1'),
            mock.call('desc_null_4'),
            mock.call('desc_4'),
        ])
        mock_or.assert_has_calls([
            mock.call(mock.ANY, 'asc_null_filter_2'),
            mock.call(mock.ANY, 'desc_null_filter_3'),
            mock.call('some_crit', 'other_crit'),
        ])
        mock_and.assert_has_calls([
            mock.call('or_1'),
            mock.call(mock.ANY, 'or_2'),
        ])
        self.query.filter.assert_called_once_with('some_f')
        self.query.limit.assert_called_once_with(5)

    def test_paginate_query_value_error(self):
        self.mock_asc.return_value = 'asc_1'
        self.query.order_by.return_value = self.query

        self.assertRaises(ValueError, utils.paginate_query,
                          self.query, self.model, 5, ['user_id', 'project_id'],
                          marker=self.marker, sort_dirs=['asc', 'mixed'])

        self.mock_asc.assert_called_once_with(self.model.user_id)
        self.query.order_by.assert_called_once_with('asc_1')

    def test_paginate_on_hybrid(self):
        self.mock_asc.return_value = 'asc_1'
        self.mock_desc.return_value = 'desc_1'
        self.query.order_by.return_value = self.query

        utils.paginate_query(self.query, self.model, 5,
                             ['user_id', 'some_hybrid'],
                             sort_dirs=['asc', 'desc'])

        self.mock_asc.assert_called_once_with(self.model.user_id)
        self.mock_desc.assert_called_once_with(self.model.some_hybrid)
        self.query.order_by.assert_has_calls([
            mock.call('asc_1'),
            mock.call('desc_1'),
        ])
        self.query.limit.assert_called_once_with(5)


class Test_UnstableSortingOrder(test_base.BaseTestCase):
    def test_multiple_primary_keys_stable(self):
        self.assertTrue(
            utils._stable_sorting_order(
                FakeTableWithMultipleKeys, ['key1', 'key2']))

    def test_classically_mapped_primary_keys_stable(self):
        self.assertTrue(
            utils._stable_sorting_order(FakeTableClassicalyMapped, ['id']))

    def test_multiple_primary_keys_unstable(self):
        self.assertFalse(
            utils._stable_sorting_order(
                FakeTableWithMultipleKeys, ['key1', 'key3']))

    def test_joined_inh_stable(self):
        self.assertTrue(
            utils._stable_sorting_order(FakeTableJoinedInh, ['user_id'])
        )

    def test_single_inh_stable(self):
        self.assertTrue(
            utils._stable_sorting_order(FakeTableSingleInh, ['user_id'])
        )

    def test_unknown_primary_keys_stable(self):
        self.assertIsNone(
            utils._stable_sorting_order(object, ['key1', 'key2']))

    def test_unique_index_stable(self):
        self.assertTrue(
            utils._stable_sorting_order(
                FakeTableWithIndexes, ['key1', 'key2']))

    def test_unique_index_unstable(self):
        self.assertFalse(
            utils._stable_sorting_order(
                FakeTableWithIndexes, ['key1', 'key3']))


class TestGetUniqueKeys(test_base.BaseTestCase):
    def test_multiple_primary_keys(self):
        self.assertEqual(
            [{'key1', 'key2'}],
            utils.get_unique_keys(FakeTableWithMultipleKeys))

    def test_unique_index(self):
        self.assertEqual(
            [{'id'}, {'key1', 'key2'}],
            utils.get_unique_keys(FakeTableWithIndexes))

    def test_unknown_primary_keys(self):
        self.assertIsNone(utils.get_unique_keys(object))

    def test_cache(self):

        class CacheTable:
            info = {}
            constraints_called = 0
            indexes_called = 0

            @property
            def constraints(self):
                self.constraints_called += 1
                return []

            @property
            def indexes(self):
                self.indexes_called += 1
                return []

        class CacheModel:
            pass

        table = CacheTable()
        mapper_mock = mock.Mock(mapped_table=table, local_table=table)
        mapper_mock.base_mapper = mapper_mock
        mock_inspect = mock.Mock(
            return_value=mapper_mock)
        model = CacheModel()
        self.assertNotIn('oslodb_unique_keys', CacheTable.info)
        with mock.patch("oslo_db.sqlalchemy.utils.inspect", mock_inspect):
            utils.get_unique_keys(model)

        self.assertIn('oslodb_unique_keys', CacheTable.info)
        self.assertEqual(1, table.constraints_called)
        self.assertEqual(1, table.indexes_called)

        for i in range(10):
            utils.get_unique_keys(model)

        self.assertEqual(1, table.constraints_called)
        self.assertEqual(1, table.indexes_called)


class TestPaginateQueryActualSQL(test_base.BaseTestCase):

    def test_paginate_with_boolean_sort(self):
        s = Session()
        q = s.query(FakeTable)
        q = utils.paginate_query(q, FakeTable, 5, ['enabled'],
                                 sort_dirs=['asc'],
                                 marker=FakeTable(user_id='hello',
                                                  enabled=False))
        expected_core_sql = (
            select(FakeTable).
            order_by(sqlalchemy.asc(FakeTable.enabled)).
            where(cast(FakeTable.enabled, Integer) > 0).
            limit(5)
        )

        self.assertEqual(
            str(expected_core_sql.compile()),
            str(q.statement.compile())
        )

    def test_paginate_on_hybrid_assert_stmt(self):
        s = Session()
        q = s.query(FakeTable)
        q = utils.paginate_query(
            q, FakeTable, 5,
            ['user_id', 'some_hybrid'],
            sort_dirs=['asc', 'desc'])
        expected_core_sql = (
            select(FakeTable).
            order_by(sqlalchemy.asc(FakeTable.user_id)).
            order_by(sqlalchemy.desc(FakeTable.some_hybrid)).
            limit(5)
        )

        self.assertEqual(
            str(expected_core_sql.compile()),
            str(q.statement.compile())
        )


class TestMigrationUtils(db_test_base._DbTestCase):

    """Class for testing utils that are used in db migrations."""

    def setUp(self):
        super().setUp()
        self.meta = MetaData()
        self.conn = self.engine.connect()

        # self.conn would be better here but does not work right now
        self.addCleanup(self.meta.drop_all, self.engine)
        self.addCleanup(self.conn.close)

    def _populate_db_for_drop_duplicate_entries(self, engine, meta,
                                                table_name):
        values = [
            {'id': 11, 'a': 3, 'b': 10, 'c': 'abcdef'},
            {'id': 12, 'a': 5, 'b': 10, 'c': 'abcdef'},
            {'id': 13, 'a': 6, 'b': 10, 'c': 'abcdef'},
            {'id': 14, 'a': 7, 'b': 10, 'c': 'abcdef'},
            {'id': 21, 'a': 1, 'b': 20, 'c': 'aa'},
            {'id': 31, 'a': 1, 'b': 20, 'c': 'bb'},
            {'id': 41, 'a': 1, 'b': 30, 'c': 'aef'},
            {'id': 42, 'a': 2, 'b': 30, 'c': 'aef'},
            {'id': 43, 'a': 3, 'b': 30, 'c': 'aef'}
        ]

        test_table = Table(table_name, meta,
                           Column('id', Integer, primary_key=True,
                                  nullable=False),
                           Column('a', Integer),
                           Column('b', Integer),
                           Column('c', String(255)),
                           Column('deleted', Integer, default=0),
                           Column('deleted_at', DateTime),
                           Column('updated_at', DateTime))

        test_table.create(engine)
        with engine.connect() as conn, conn.begin():
            conn.execute(test_table.insert(), values)
        return test_table, values

    def test_drop_old_duplicate_entries_from_table(self):
        table_name = "__test_tmp_table__"

        test_table, values = self._populate_db_for_drop_duplicate_entries(
            self.engine, self.meta, table_name)
        utils.drop_old_duplicate_entries_from_table(
            self.engine, table_name, False, 'b', 'c')

        uniq_values = set()
        expected_ids = []
        for value in sorted(values, key=lambda x: x['id'], reverse=True):
            uniq_value = (('b', value['b']), ('c', value['c']))
            if uniq_value in uniq_values:
                continue
            uniq_values.add(uniq_value)
            expected_ids.append(value['id'])

        with self.engine.connect() as conn, conn.begin():
            real_ids = [
                row[0] for row in
                conn.execute(select(test_table.c.id)).fetchall()
            ]

        self.assertEqual(len(expected_ids), len(real_ids))
        for id_ in expected_ids:
            self.assertIn(id_, real_ids)

    def test_drop_dup_entries_in_file_conn(self):
        table_name = "__test_tmp_table__"
        tmp_db_file = self.create_tempfiles([['name', '']], ext='.sql')[0]
        in_file_engine = session.EngineFacade(
            'sqlite:///%s' % tmp_db_file).get_engine()
        meta = MetaData()
        test_table, values = self._populate_db_for_drop_duplicate_entries(
            in_file_engine, meta, table_name)
        utils.drop_old_duplicate_entries_from_table(
            in_file_engine, table_name, False, 'b', 'c')

    def test_drop_old_duplicate_entries_from_table_soft_delete(self):
        table_name = "__test_tmp_table__"

        table, values = self._populate_db_for_drop_duplicate_entries(
            self.engine, self.meta, table_name)
        utils.drop_old_duplicate_entries_from_table(self.engine, table_name,
                                                    True, 'b', 'c')
        uniq_values = set()
        expected_values = []
        soft_deleted_values = []

        for value in sorted(values, key=lambda x: x['id'], reverse=True):
            uniq_value = (('b', value['b']), ('c', value['c']))
            if uniq_value in uniq_values:
                soft_deleted_values.append(value)
                continue
            uniq_values.add(uniq_value)
            expected_values.append(value)

        base_select = table.select()

        with self.engine.connect() as conn, conn.begin():
            rows_select = base_select.where(table.c.deleted != table.c.id)
            row_ids = [
                row.id for row in conn.execute(rows_select).fetchall()
            ]
            self.assertEqual(len(expected_values), len(row_ids))
            for value in expected_values:
                self.assertIn(value['id'], row_ids)

            deleted_rows_select = base_select.where(
                table.c.deleted == table.c.id)
            deleted_rows_ids = [
                row.id for row in
                conn.execute(deleted_rows_select).fetchall()
            ]
        self.assertEqual(len(values) - len(row_ids),
                         len(deleted_rows_ids))
        for value in soft_deleted_values:
            self.assertIn(value['id'], deleted_rows_ids)

    def test_get_foreign_key_constraint_name(self):
        table_1 = Table('table_name_1', self.meta,
                        Column('id', Integer, primary_key=True),
                        Column('deleted', Integer))
        table_2 = Table('table_name_2', self.meta,
                        Column('id', Integer, primary_key=True),
                        Column('foreign_id', Integer),
                        ForeignKeyConstraint(['foreign_id'],
                                             ['table_name_1.id'],
                                             name='table_name_2_fk1'),
                        Column('deleted', Integer))

        self.meta.create_all(self.engine, tables=[table_1, table_2])
        fkc = utils.get_foreign_key_constraint_name(self.engine,
                                                    'table_name_2',
                                                    'foreign_id')
        self.assertEqual(fkc, 'table_name_2_fk1')


class PostgresqlTestMigrations(TestMigrationUtils,
                               db_test_base._PostgreSQLOpportunisticTestCase):

    """Test migrations on PostgreSQL."""
    pass


class MySQLTestMigrations(TestMigrationUtils,
                          db_test_base._MySQLOpportunisticTestCase):

    """Test migrations on MySQL."""
    pass


class TestConnectionUtils(test_base.BaseTestCase):

    def setUp(self):
        super().setUp()

        self.full_credentials = {'backend': 'postgresql+psycopg2',
                                 'database': 'test',
                                 'user': 'dude',
                                 'passwd': 'pass'}

        self.connect_string = 'postgresql+psycopg2://dude:pass@localhost/test'

        # NOTE(rpodolyaka): mock the dialect parts, so that we don't depend
        # on psycopg2 (or any other DBAPI implementation) in these tests

        @classmethod
        def fake_dbapi(cls):
            return mock.MagicMock()

        class OurDialect(psycopg2.PGDialect_psycopg2):
            def dbapi(self):
                return fake_dbapi

            def import_dbapi(self):
                return fake_dbapi

        patch_dbapi = mock.patch.object(
            psycopg2,
            "PGDialect_psycopg2",
            new=OurDialect,
        )

        patch_dbapi.start()
        self.addCleanup(patch_dbapi.stop)

        patch_onconnect = mock.patch.object(psycopg2.PGDialect_psycopg2,
                                            'on_connect')
        patch_onconnect.start()
        self.addCleanup(patch_onconnect.stop)

    def test_ensure_backend_available(self):
        with mock.patch.object(
                sqlalchemy.engine.base.Engine, 'connect') as mock_connect:
            fake_connection = mock.Mock()
            mock_connect.return_value = fake_connection

            eng = provision.Backend._ensure_backend_available(
                self.connect_string)

            self.assertIsInstance(eng, sqlalchemy.engine.base.Engine)
            self.assertEqual(utils.make_url(self.connect_string), eng.url)

            mock_connect.assert_called_once()
            fake_connection.close.assert_called_once()

    def test_ensure_backend_available_no_connection_raises(self):
        log = self.useFixture(fixtures.FakeLogger())
        err = OperationalError("Can't connect to database", None, None)
        with mock.patch.object(
                sqlalchemy.engine.base.Engine, 'connect') as mock_connect:
            mock_connect.side_effect = err

            exc = self.assertRaises(
                exception.BackendNotAvailable,
                provision.Backend._ensure_backend_available,
                self.connect_string)
            self.assertEqual(
                "Backend 'postgresql+psycopg2' is unavailable: "
                "Could not connect", str(exc))
            self.assertEqual(
                "The postgresql+psycopg2 backend is unavailable: %s" % err,
                log.output.strip())

    def test_ensure_backend_available_no_dbapi_raises(self):
        log = self.useFixture(fixtures.FakeLogger())
        with mock.patch.object(sqlalchemy, 'create_engine') as mock_create:
            mock_create.side_effect = ImportError(
                "Can't import DBAPI module foobar")

            exc = self.assertRaises(
                exception.BackendNotAvailable,
                provision.Backend._ensure_backend_available,
                self.connect_string)

            mock_create.assert_called_once_with(
                utils.make_url(self.connect_string))

            self.assertEqual(
                "Backend 'postgresql+psycopg2' is unavailable: "
                "No DBAPI installed", str(exc))
            self.assertEqual(
                "The postgresql+psycopg2 backend is unavailable: Can't import "
                "DBAPI module foobar", log.output.strip())

    def test_get_db_connection_info(self):
        conn_pieces = parse.urlparse(self.connect_string)
        self.assertEqual(('dude', 'pass', 'test', 'localhost'),
                         utils.get_db_connection_info(conn_pieces))


class MyModelSoftDeletedProjectId(declarative_base(), models.ModelBase,
                                  models.SoftDeleteMixin):
    __tablename__ = 'soft_deleted_project_id_test_model'
    id = Column(Integer, primary_key=True)
    project_id = Column(Integer)


class MyModel(declarative_base(), models.ModelBase):
    __tablename__ = 'test_model'
    id = Column(Integer, primary_key=True)


class MyModelSoftDeleted(declarative_base(), models.ModelBase,
                         models.SoftDeleteMixin):
    __tablename__ = 'soft_deleted_test_model'
    id = Column(Integer, primary_key=True)


class TestModelQuery(test_base.BaseTestCase):

    def setUp(self):
        super().setUp()

        self.session = mock.MagicMock()
        self.session.query.return_value = self.session.query
        self.session.query.filter.return_value = self.session.query

    def test_wrong_model(self):
        self.assertRaises(TypeError, utils.model_query,
                          FakeModel, session=self.session)

    def test_no_soft_deleted(self):
        self.assertRaises(ValueError, utils.model_query,
                          MyModel, session=self.session, deleted=True)

    def test_deleted_false(self):
        mock_query = utils.model_query(
            MyModelSoftDeleted, session=self.session, deleted=False)

        deleted_filter = mock_query.filter.call_args[0][0]
        self.assertEqual('soft_deleted_test_model.deleted = :deleted_1',
                         str(deleted_filter))
        self.assertEqual(deleted_filter.right.value,
                         MyModelSoftDeleted.__mapper__.c.deleted.default.arg)

    def test_deleted_true(self):
        mock_query = utils.model_query(
            MyModelSoftDeleted, session=self.session, deleted=True)

        deleted_filter = mock_query.filter.call_args[0][0]
        self.assertEqual(str(deleted_filter),
                         'soft_deleted_test_model.deleted != :deleted_1')
        self.assertEqual(deleted_filter.right.value,
                         MyModelSoftDeleted.__mapper__.c.deleted.default.arg)

    @mock.patch.object(utils, "_read_deleted_filter")
    def test_no_deleted_value(self, _read_deleted_filter):
        utils.model_query(MyModelSoftDeleted, session=self.session)
        self.assertEqual(0, _read_deleted_filter.call_count)

    def test_project_filter(self):
        project_id = 10

        mock_query = utils.model_query(
            MyModelSoftDeletedProjectId, session=self.session,
            project_only=True, project_id=project_id)

        deleted_filter = mock_query.filter.call_args[0][0]
        self.assertEqual(
            'soft_deleted_project_id_test_model.project_id = :project_id_1',
            str(deleted_filter))
        self.assertEqual(project_id, deleted_filter.right.value)

    def test_project_filter_wrong_model(self):
        self.assertRaises(ValueError, utils.model_query,
                          MyModelSoftDeleted, session=self.session,
                          project_id=10)

    def test_project_filter_allow_none(self):
        mock_query = utils.model_query(
            MyModelSoftDeletedProjectId,
            session=self.session, project_id=(10, None))

        self.assertRegex(
            str(
                mock_query.filter.call_args[0][0].
                compile(compile_kwargs={"render_postcompile": True})
            ),
            'soft_deleted_project_id_test_model.project_id'
            r' IN \(:project_id.+, (?::project_id.+|NULL)\)',
        )

    def test_model_query_common(self):
        utils.model_query(MyModel, args=(MyModel.id,), session=self.session)
        self.session.query.assert_called_with(MyModel.id)


class TestUtils(db_test_base._DbTestCase):
    def setUp(self):
        super().setUp()
        meta = MetaData()
        self.test_table = Table(
            'test_table',
            meta,
            Column('a', Integer),
            Column('b', Integer)
        )
        self.test_table.create(self.engine)
        self.addCleanup(meta.drop_all, self.engine)

    def test_get_indexes(self):
        Index('index_a', self.test_table.c.a).create(self.engine)
        Index('index_b', self.test_table.c.b).create(self.engine)
        indexes = utils.get_indexes(self.engine, "test_table")
        indexes = [(index['name'], index['column_names']) for index in indexes]
        self.assertIn(('index_a', ['a']), indexes)
        self.assertIn(('index_b', ['b']), indexes)

    def test_index_exists(self):
        self.assertFalse(utils.index_exists(self.engine, 'test_table',
                                            'new_index'))
        Index('new_index', self.test_table.c.a).create(self.engine)
        self.assertTrue(utils.index_exists(self.engine, 'test_table',
                                           'new_index'))

    def test_index_exists_on_columns(self):
        columns = [self.test_table.c.a, self.test_table.c.b]
        Index('new_index', *columns).create(self.engine)
        self.assertTrue(utils.index_exists_on_columns(self.engine,
                                                      'test_table',
                                                      ('a', 'b')))

    def test_add_index(self):
        self.assertFalse(utils.index_exists(self.engine, 'test_table',
                                            'new_index'))
        utils.add_index(self.engine, 'test_table', 'new_index', ('a',))
        self.assertTrue(utils.index_exists(self.engine, 'test_table',
                                           'new_index'))

    def test_add_existing_index(self):
        Index('new_index', self.test_table.c.a).create(self.engine)
        self.assertRaises(ValueError, utils.add_index, self.engine,
                          'test_table', 'new_index', ('a',))

    def test_drop_index(self):
        Index('new_index', self.test_table.c.a).create(self.engine)
        utils.drop_index(self.engine, 'test_table', 'new_index')
        self.assertFalse(utils.index_exists(self.engine, 'test_table',
                         'new_index'))

    def test_drop_unexisting_index(self):
        self.assertRaises(ValueError, utils.drop_index, self.engine,
                          'test_table', 'new_index')

    @mock.patch('oslo_db.sqlalchemy.utils.drop_index')
    @mock.patch('oslo_db.sqlalchemy.utils.add_index')
    def test_change_index_columns(self, add_index, drop_index):
        utils.change_index_columns(self.engine, 'test_table', 'a_index',
                                   ('a',))
        utils.drop_index.assert_called_once_with(self.engine, 'test_table',
                                                 'a_index')
        utils.add_index.assert_called_once_with(self.engine, 'test_table',
                                                'a_index', ('a',))

    def test_column_exists(self):
        for col in ['a', 'b']:
            self.assertTrue(utils.column_exists(self.engine, 'test_table',
                                                col))
        self.assertFalse(utils.column_exists(self.engine, 'test_table',
                                             'fake_column'))


class TestUtilsMysqlOpportunistically(
        TestUtils, db_test_base._MySQLOpportunisticTestCase):
    pass


class TestUtilsPostgresqlOpportunistically(
        TestUtils, db_test_base._PostgreSQLOpportunisticTestCase):
    pass


class TestDialectFunctionDispatcher(test_base.BaseTestCase):
    def _single_fixture(self):
        callable_fn = mock.Mock()

        dispatcher = orig = utils.dispatch_for_dialect("*")(
            callable_fn.default)
        dispatcher = dispatcher.dispatch_for("sqlite")(callable_fn.sqlite)
        dispatcher = dispatcher.dispatch_for("mysql+pymysql")(
            callable_fn.mysql_pymysql)
        dispatcher = dispatcher.dispatch_for("mysql")(
            callable_fn.mysql)
        dispatcher = dispatcher.dispatch_for("postgresql")(
            callable_fn.postgresql)

        self.assertTrue(dispatcher is orig)

        return dispatcher, callable_fn

    def _multiple_fixture(self):
        callable_fn = mock.Mock()

        for targ in [
            callable_fn.default,
            callable_fn.sqlite,
            callable_fn.mysql,
            callable_fn.mysql_pymysql,
            callable_fn.postgresql,
            callable_fn.postgresql_psycopg2,
            callable_fn.pyodbc
        ]:
            targ.return_value = None

        dispatcher = orig = utils.dispatch_for_dialect("*", multiple=True)(
            callable_fn.default)
        dispatcher = dispatcher.dispatch_for("sqlite")(callable_fn.sqlite)
        dispatcher = dispatcher.dispatch_for("mysql+pymysql")(
            callable_fn.mysql_pymysql)
        dispatcher = dispatcher.dispatch_for("mysql")(
            callable_fn.mysql)
        dispatcher = dispatcher.dispatch_for("postgresql+*")(
            callable_fn.postgresql)
        dispatcher = dispatcher.dispatch_for("postgresql+psycopg2")(
            callable_fn.postgresql_psycopg2)
        dispatcher = dispatcher.dispatch_for("*+pyodbc")(
            callable_fn.pyodbc)

        self.assertTrue(dispatcher is orig)

        return dispatcher, callable_fn

    def test_single(self):

        dispatcher, callable_fn = self._single_fixture()
        dispatcher("sqlite://", 1)
        dispatcher("postgresql+psycopg2://u:p@h/t", 2)
        dispatcher("mysql+pymysql://u:p@h/t", 3)
        dispatcher("mysql://u:p@h/t", 4)
        dispatcher("mysql+mysqlconnector://u:p@h/t", 5)

        self.assertEqual(
            [
                mock.call.sqlite('sqlite://', 1),
                mock.call.postgresql("postgresql+psycopg2://u:p@h/t", 2),
                mock.call.mysql_pymysql("mysql+pymysql://u:p@h/t", 3),
                mock.call.mysql("mysql://u:p@h/t", 4),
                mock.call.mysql("mysql+mysqlconnector://u:p@h/t", 5),
            ],
            callable_fn.mock_calls)

    def test_single_kwarg(self):
        dispatcher, callable_fn = self._single_fixture()
        dispatcher("sqlite://", foo='bar')
        dispatcher("postgresql+psycopg2://u:p@h/t", 1, x='y')

        self.assertEqual(
            [
                mock.call.sqlite('sqlite://', foo='bar'),
                mock.call.postgresql(
                    "postgresql+psycopg2://u:p@h/t",
                    1, x='y'),
            ],
            callable_fn.mock_calls)

    def test_dispatch_on_target(self):
        callable_fn = mock.Mock()

        @utils.dispatch_for_dialect("*")
        def default_fn(url, x, y):
            callable_fn.default(url, x, y)

        @default_fn.dispatch_for("sqlite")
        def sqlite_fn(url, x, y):
            callable_fn.sqlite(url, x, y)
            default_fn.dispatch_on_drivername("*")(url, x, y)

        default_fn("sqlite://", 4, 5)
        self.assertEqual(
            [
                mock.call.sqlite("sqlite://", 4, 5),
                mock.call.default("sqlite://", 4, 5)
            ],
            callable_fn.mock_calls
        )

    def test_single_no_dispatcher(self):
        callable_fn = mock.Mock()

        dispatcher = utils.dispatch_for_dialect("sqlite")(callable_fn.sqlite)
        dispatcher = dispatcher.dispatch_for("mysql")(callable_fn.mysql)
        exc = self.assertRaises(
            ValueError,
            dispatcher, "postgresql://s:t@localhost/test"
        )
        self.assertEqual(
            "No default function found for driver: 'postgresql+psycopg2'",
            str(exc)
        )

    def test_multiple_no_dispatcher(self):
        callable_fn = mock.Mock()

        dispatcher = utils.dispatch_for_dialect("sqlite", multiple=True)(
            callable_fn.sqlite)
        dispatcher = dispatcher.dispatch_for("mysql")(callable_fn.mysql)
        dispatcher("postgresql://s:t@localhost/test")
        self.assertEqual(
            [], callable_fn.mock_calls
        )

    def test_multiple_no_driver(self):
        callable_fn = mock.Mock(
            default=mock.Mock(return_value=None),
            sqlite=mock.Mock(return_value=None)
        )

        dispatcher = utils.dispatch_for_dialect("*", multiple=True)(
            callable_fn.default)
        dispatcher = dispatcher.dispatch_for("sqlite")(
            callable_fn.sqlite)

        dispatcher.dispatch_on_drivername("sqlite")("foo")
        self.assertEqual(
            [mock.call.sqlite("foo"), mock.call.default("foo")],
            callable_fn.mock_calls
        )

    def test_multiple_nesting(self):
        callable_fn = mock.Mock(
            default=mock.Mock(return_value=None),
            mysql=mock.Mock(return_value=None)
        )

        dispatcher = utils.dispatch_for_dialect("*", multiple=True)(
            callable_fn.default)

        dispatcher = dispatcher.dispatch_for("mysql+mysqlconnector")(
            dispatcher.dispatch_for("mysql+mysqldb")(
                callable_fn.mysql
            )
        )

        mysqldb_url = utils.make_url("mysql+mysqldb://")
        mysqlconnector_url = utils.make_url("mysql+mysqlconnector://")
        sqlite_url = utils.make_url("sqlite://")

        dispatcher(mysqldb_url, 1)
        dispatcher(mysqlconnector_url, 2)
        dispatcher(sqlite_url, 3)

        self.assertEqual(
            [
                mock.call.mysql(mysqldb_url, 1),
                mock.call.default(mysqldb_url, 1),
                mock.call.mysql(mysqlconnector_url, 2),
                mock.call.default(mysqlconnector_url, 2),
                mock.call.default(sqlite_url, 3)
            ],
            callable_fn.mock_calls
        )

    def test_single_retval(self):
        dispatcher, callable_fn = self._single_fixture()
        callable_fn.mysql_pymysql.return_value = 5

        self.assertEqual(
            5, dispatcher("mysql+pymysql://u:p@h/t", 3)
        )

    def test_engine(self):
        eng = sqlalchemy.create_engine("sqlite:///path/to/my/db.db")
        dispatcher, callable_fn = self._single_fixture()

        dispatcher(eng)
        self.assertEqual(
            [mock.call.sqlite(eng)],
            callable_fn.mock_calls
        )

    def test_url_pymysql(self):
        url = utils.make_url("mysql+pymysql://scott:tiger@localhost/test")
        dispatcher, callable_fn = self._single_fixture()

        dispatcher(url, 15)
        self.assertEqual(
            [mock.call.mysql_pymysql(url, 15)],
            callable_fn.mock_calls
        )

    def test_url_mysql_generic(self):
        url = utils.make_url("mysql://scott:tiger@localhost/test")
        dispatcher, callable_fn = self._single_fixture()

        dispatcher(url, 15)
        self.assertEqual(
            [mock.call.mysql(url, 15)],
            callable_fn.mock_calls
        )

    def test_invalid_target(self):
        dispatcher, callable_fn = self._single_fixture()

        exc = self.assertRaises(
            ValueError,
            dispatcher, 20
        )
        self.assertEqual("Invalid target type: 20", str(exc))

    def test_invalid_dispatch(self):
        callable_fn = mock.Mock()

        dispatcher = utils.dispatch_for_dialect("*")(callable_fn.default)

        exc = self.assertRaises(
            ValueError,
            dispatcher.dispatch_for("+pyodbc"), callable_fn.pyodbc
        )
        self.assertEqual(
            "Couldn't parse database[+driver]: '+pyodbc'",
            str(exc)
        )

    def test_single_only_one_target(self):
        callable_fn = mock.Mock()

        dispatcher = utils.dispatch_for_dialect("*")(callable_fn.default)
        dispatcher = dispatcher.dispatch_for("sqlite")(callable_fn.sqlite)

        exc = self.assertRaises(
            TypeError,
            dispatcher.dispatch_for("sqlite"), callable_fn.sqlite2
        )
        self.assertEqual(
            "Multiple functions for expression 'sqlite'", str(exc)
        )

    def test_multiple(self):
        dispatcher, callable_fn = self._multiple_fixture()

        dispatcher("postgresql+pyodbc://", 1)
        dispatcher("mysql+pymysql://", 2)
        dispatcher("postgresql+psycopg2://", 4)
        dispatcher("postgresql://", 5)

        # TODO(zzzeek): there is a deterministic order here, but we might
        # want to tweak it, or maybe provide options.  default first?
        # most specific first?  is *+pyodbc or postgresql+* more specific?
        self.assertEqual(
            [
                mock.call.postgresql('postgresql+pyodbc://', 1),
                mock.call.pyodbc('postgresql+pyodbc://', 1),
                mock.call.default('postgresql+pyodbc://', 1),
                mock.call.mysql_pymysql('mysql+pymysql://', 2),
                mock.call.mysql('mysql+pymysql://', 2),
                mock.call.default('mysql+pymysql://', 2),
                mock.call.postgresql_psycopg2('postgresql+psycopg2://', 4),
                mock.call.postgresql('postgresql+psycopg2://', 4),
                mock.call.default('postgresql+psycopg2://', 4),
                # note this is called because we resolve the default
                # DBAPI for the url
                mock.call.postgresql_psycopg2('postgresql://', 5),
                mock.call.postgresql('postgresql://', 5),
                mock.call.default('postgresql://', 5),
            ],
            callable_fn.mock_calls
        )

    def test_multiple_no_return_value(self):
        dispatcher, callable_fn = self._multiple_fixture()
        callable_fn.sqlite.return_value = 5

        exc = self.assertRaises(
            TypeError,
            dispatcher, "sqlite://"
        )
        self.assertEqual(
            "Return value not allowed for multiple filtered function",
            str(exc)
        )


class TestGetInnoDBTables(db_test_base._MySQLOpportunisticTestCase):

    def test_all_tables_use_innodb(self):
        with self.engine.connect() as conn, conn.begin():
            conn.execute(
                sql.text(
                    "CREATE TABLE customers "
                    "(a INT, b CHAR (20), INDEX (a)) ENGINE=InnoDB"))
        self.assertEqual([], utils.get_non_innodb_tables(self.engine))

    def test_all_tables_use_innodb_false(self):
        with self.engine.connect() as conn, conn.begin():
            conn.execute(
                sql.text("CREATE TABLE employee (i INT) ENGINE=MEMORY")
            )
        self.assertEqual(['employee'],
                         utils.get_non_innodb_tables(self.engine))

    def test_skip_tables_use_default_value(self):
        with self.engine.connect() as conn, conn.begin():
            conn.execute(
                sql.text("CREATE TABLE migrate_version (i INT) ENGINE=MEMORY")
            )
        self.assertEqual([],
                         utils.get_non_innodb_tables(self.engine))

    def test_skip_tables_use_passed_value(self):
        with self.engine.connect() as conn, conn.begin():
            conn.execute(
                sql.text("CREATE TABLE some_table (i INT) ENGINE=MEMORY"))
        self.assertEqual([],
                         utils.get_non_innodb_tables(
                             self.engine, skip_tables=('some_table',)))

    def test_skip_tables_use_empty_list(self):
        with self.engine.connect() as conn, conn.begin():
            conn.execute(
                sql.text("CREATE TABLE some_table_3 (i INT) ENGINE=MEMORY"))
        self.assertEqual(['some_table_3'],
                         utils.get_non_innodb_tables(
                         self.engine, skip_tables=()))

    def test_skip_tables_use_several_values(self):
        with self.engine.connect() as conn, conn.begin():
            conn.execute(
                sql.text("CREATE TABLE some_table_1 (i INT) ENGINE=MEMORY"))
            conn.execute(
                sql.text("CREATE TABLE some_table_2 (i INT) ENGINE=MEMORY"))
        self.assertEqual([],
                         utils.get_non_innodb_tables(
                             self.engine,
                             skip_tables=('some_table_1', 'some_table_2')))
