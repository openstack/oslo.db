# Copyright 2010-2011 OpenStack Foundation
# Copyright 2012-2013 IBM Corp.
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

import fixtures
from migrate.versioning import api as versioning_api
import sqlalchemy as sa
from sqlalchemy import orm

from oslo_db import exception as exc
from oslo_db.sqlalchemy import test_migrations as migrate
from oslo_db.tests import base as test_base
from oslo_db.tests.sqlalchemy import base as db_test_base


class TestWalkVersions(test_base.BaseTestCase, migrate.WalkVersionsMixin):
    migration_api = mock.MagicMock()
    REPOSITORY = mock.MagicMock()
    engine = mock.MagicMock()
    INIT_VERSION = versioning_api.VerNum(4)

    @property
    def migrate_engine(self):
        return self.engine

    def test_migrate_up(self):
        self.migration_api.db_version.return_value = 141

        self.migrate_up(141)

        self.migration_api.upgrade.assert_called_with(
            self.engine, self.REPOSITORY, 141)
        self.migration_api.db_version.assert_called_with(
            self.engine, self.REPOSITORY)

    @staticmethod
    def _fake_upgrade_boom(*args, **kwargs):
        raise exc.DBMigrationError("boom")

    def test_migrate_up_fail(self):
        version = 141
        self.migration_api.db_version.return_value = version
        expected_output = (
            "Failed to migrate to version %(version)s on "
            "engine %(engine)s\n" %
            {'version': version, 'engine': self.engine})

        with mock.patch.object(
            self.migration_api,
            'upgrade',
            side_effect=self._fake_upgrade_boom,
        ):
            log = self.useFixture(fixtures.FakeLogger())
            self.assertRaises(exc.DBMigrationError, self.migrate_up, version)
            self.assertEqual(expected_output, log.output)

    def test_migrate_up_with_data(self):
        test_value = {"a": 1, "b": 2}
        self.migration_api.db_version.return_value = 141
        self._pre_upgrade_141 = mock.MagicMock()
        self._pre_upgrade_141.return_value = test_value
        self._check_141 = mock.MagicMock()

        self.migrate_up(141, True)

        self._pre_upgrade_141.assert_called_with(self.engine)
        self._check_141.assert_called_with(self.engine, test_value)

    def test_migrate_down(self):
        self.migration_api.db_version.return_value = 42

        self.assertTrue(self.migrate_down(42))
        self.migration_api.db_version.assert_called_with(
            self.engine, self.REPOSITORY)

    def test_migrate_down_not_implemented(self):
        with mock.patch.object(
            self.migration_api,
            'downgrade',
            side_effect=NotImplementedError,
        ):
            self.assertFalse(self.migrate_down(self.engine, 42))

    def test_migrate_down_with_data(self):
        self._post_downgrade_043 = mock.MagicMock()
        self.migration_api.db_version.return_value = 42

        self.migrate_down(42, True)

        self._post_downgrade_043.assert_called_with(self.engine)

    @mock.patch.object(migrate.WalkVersionsMixin, 'migrate_up')
    @mock.patch.object(migrate.WalkVersionsMixin, 'migrate_down')
    def test_walk_versions_all_default(self, migrate_up, migrate_down):
        self.REPOSITORY.latest = versioning_api.VerNum(20)
        self.migration_api.db_version.return_value = self.INIT_VERSION

        self.walk_versions()

        self.migration_api.version_control.assert_called_with(
            self.engine, self.REPOSITORY, self.INIT_VERSION)
        self.migration_api.db_version.assert_called_with(
            self.engine, self.REPOSITORY)

        versions = range(int(self.INIT_VERSION) + 1,
                         int(self.REPOSITORY.latest) + 1)
        upgraded = [mock.call(v, with_data=True)
                    for v in versions]
        self.assertEqual(upgraded, self.migrate_up.call_args_list)

        downgraded = [mock.call(v - 1) for v in reversed(versions)]
        self.assertEqual(downgraded, self.migrate_down.call_args_list)

    @mock.patch.object(migrate.WalkVersionsMixin, 'migrate_up')
    @mock.patch.object(migrate.WalkVersionsMixin, 'migrate_down')
    def test_walk_versions_all_true(self, migrate_up, migrate_down):
        self.REPOSITORY.latest = versioning_api.VerNum(20)
        self.migration_api.db_version.return_value = self.INIT_VERSION

        self.walk_versions(snake_walk=True, downgrade=True)

        versions = range(int(self.INIT_VERSION) + 1,
                         int(self.REPOSITORY.latest) + 1)
        upgraded = []
        for v in versions:
            upgraded.append(mock.call(v, with_data=True))
            upgraded.append(mock.call(v))
        upgraded.extend([mock.call(v) for v in reversed(versions)])
        self.assertEqual(upgraded, self.migrate_up.call_args_list)

        downgraded_1 = [mock.call(v - 1, with_data=True) for v in versions]
        downgraded_2 = []
        for v in reversed(versions):
            downgraded_2.append(mock.call(v - 1))
            downgraded_2.append(mock.call(v - 1))
        downgraded = downgraded_1 + downgraded_2
        self.assertEqual(downgraded, self.migrate_down.call_args_list)

    @mock.patch.object(migrate.WalkVersionsMixin, 'migrate_up')
    @mock.patch.object(migrate.WalkVersionsMixin, 'migrate_down')
    def test_walk_versions_true_false(self, migrate_up, migrate_down):
        self.REPOSITORY.latest = versioning_api.VerNum(20)
        self.migration_api.db_version.return_value = self.INIT_VERSION

        self.walk_versions(snake_walk=True, downgrade=False)

        versions = range(int(self.INIT_VERSION) + 1,
                         int(self.REPOSITORY.latest) + 1)

        upgraded = []
        for v in versions:
            upgraded.append(mock.call(v, with_data=True))
            upgraded.append(mock.call(v))
        self.assertEqual(upgraded, self.migrate_up.call_args_list)

        downgraded = [mock.call(v - 1, with_data=True) for v in versions]
        self.assertEqual(downgraded, self.migrate_down.call_args_list)

    @mock.patch.object(migrate.WalkVersionsMixin, 'migrate_up')
    @mock.patch.object(migrate.WalkVersionsMixin, 'migrate_down')
    def test_walk_versions_all_false(self, migrate_up, migrate_down):
        self.REPOSITORY.latest = versioning_api.VerNum(20)
        self.migration_api.db_version.return_value = self.INIT_VERSION

        self.walk_versions(snake_walk=False, downgrade=False)

        versions = range(int(self.INIT_VERSION) + 1,
                         int(self.REPOSITORY.latest) + 1)

        upgraded = [mock.call(v, with_data=True) for v in versions]
        self.assertEqual(upgraded, self.migrate_up.call_args_list)


class ModelsMigrationSyncMixin(db_test_base._DbTestCase):

    def setUp(self):
        super(ModelsMigrationSyncMixin, self).setUp()

        self.metadata = sa.MetaData()
        self.metadata_migrations = sa.MetaData()

        sa.Table(
            'testtbl', self.metadata_migrations,
            sa.Column('id', sa.Integer, primary_key=True),
            sa.Column('spam', sa.String(10), nullable=False),
            sa.Column('eggs', sa.DateTime),
            sa.Column('foo', sa.Boolean,
                      server_default=sa.sql.expression.true()),
            sa.Column('bool_wo_default', sa.Boolean),
            sa.Column('bar', sa.Numeric(10, 5)),
            sa.Column('defaulttest', sa.Integer, server_default='5'),
            sa.Column('defaulttest2', sa.String(8), server_default=''),
            sa.Column('defaulttest3', sa.String(5), server_default="test"),
            sa.Column('defaulttest4', sa.Enum('first', 'second',
                                              name='testenum'),
                      server_default="first"),
            sa.Column("defaulttest5", sa.Integer, server_default=sa.text('0')),
            sa.Column('variant', sa.BigInteger()),
            sa.Column('variant2', sa.BigInteger(), server_default='0'),
            sa.Column('fk_check', sa.String(36), nullable=False),
            sa.UniqueConstraint('spam', 'eggs', name='uniq_cons'),
        )

        BASE = orm.declarative_base(metadata=self.metadata)

        class TestModel(BASE):
            __tablename__ = 'testtbl'
            __table_args__ = (
                sa.UniqueConstraint('spam', 'eggs', name='uniq_cons'),
            )

            id = sa.Column('id', sa.Integer, primary_key=True)
            spam = sa.Column('spam', sa.String(10), nullable=False)
            eggs = sa.Column('eggs', sa.DateTime)
            foo = sa.Column('foo', sa.Boolean,
                            server_default=sa.sql.expression.true())
            fk_check = sa.Column('fk_check', sa.String(36), nullable=False)
            bool_wo_default = sa.Column('bool_wo_default', sa.Boolean)
            defaulttest = sa.Column('defaulttest',
                                    sa.Integer, server_default='5')
            defaulttest2 = sa.Column('defaulttest2', sa.String(8),
                                     server_default='')
            defaulttest3 = sa.Column('defaulttest3', sa.String(5),
                                     server_default="test")
            defaulttest4 = sa.Column('defaulttest4', sa.Enum('first', 'second',
                                                             name='testenum'),
                                     server_default="first")
            defaulttest5 = sa.Column("defaulttest5",
                                     sa.Integer, server_default=sa.text('0'))
            variant = sa.Column(sa.BigInteger().with_variant(
                sa.Integer(), 'sqlite'))
            variant2 = sa.Column(sa.BigInteger().with_variant(
                sa.Integer(), 'sqlite'), server_default='0')
            bar = sa.Column('bar', sa.Numeric(10, 5))

        class ModelThatShouldNotBeCompared(BASE):
            __tablename__ = 'testtbl2'

            id = sa.Column('id', sa.Integer, primary_key=True)
            spam = sa.Column('spam', sa.String(10), nullable=False)

    def get_metadata(self):
        return self.metadata

    def get_engine(self):
        return self.engine

    def db_sync(self, engine):
        self.metadata_migrations.create_all(bind=engine)

    def include_object(self, object_, name, type_, reflected, compare_to):
        if type_ == 'table':
            return name == 'testtbl'
        return True

    def _test_models_not_sync_filtered(self):
        self.metadata_migrations.clear()
        sa.Table(
            'table', self.metadata_migrations,
            sa.Column('fk_check', sa.String(36), nullable=False),
            sa.PrimaryKeyConstraint('fk_check'),
            mysql_engine='InnoDB'
        )

        sa.Table(
            'testtbl', self.metadata_migrations,
            sa.Column('id', sa.Integer, primary_key=True),
            sa.Column('spam', sa.String(8), nullable=True),
            sa.Column('eggs', sa.DateTime),
            sa.Column('foo', sa.Boolean,
                      server_default=sa.sql.expression.false()),
            sa.Column('bool_wo_default', sa.Boolean, unique=True),
            sa.Column('bar', sa.BigInteger),
            sa.Column('defaulttest', sa.Integer, server_default='7'),
            sa.Column('defaulttest2', sa.String(8), server_default=''),
            sa.Column('defaulttest3', sa.String(5), server_default="fake"),
            sa.Column('defaulttest4',
                      sa.Enum('first', 'second', name='testenum'),
                      server_default="first"),
            sa.Column("defaulttest5", sa.Integer, server_default=sa.text('0')),
            sa.Column('fk_check', sa.String(36), nullable=False),
            sa.UniqueConstraint('spam', 'foo', name='uniq_cons'),
            sa.ForeignKeyConstraint(['fk_check'], ['table.fk_check']),
            mysql_engine='InnoDB'
        )

        with mock.patch.object(self, 'filter_metadata_diff') as filter_mock:
            def filter_diffs(diffs):
                # test filter returning only constraint related diffs
                return [
                    diff
                    for diff in diffs
                    if 'constraint' in diff[0]
                ]
            filter_mock.side_effect = filter_diffs

            msg = str(self.assertRaises(AssertionError, self.test_models_sync))
            self.assertNotIn('defaulttest', msg)
            self.assertNotIn('defaulttest3', msg)
            self.assertNotIn('remove_fk', msg)
            self.assertIn('constraint', msg)

    def _test_models_not_sync(self):
        self.metadata_migrations.clear()
        sa.Table(
            'table', self.metadata_migrations,
            sa.Column('fk_check', sa.String(36), nullable=False),
            sa.PrimaryKeyConstraint('fk_check'),
            mysql_engine='InnoDB'
        )
        sa.Table(
            'testtbl', self.metadata_migrations,
            sa.Column('id', sa.Integer, primary_key=True),
            sa.Column('spam', sa.String(8), nullable=True),
            sa.Column('eggs', sa.DateTime),
            sa.Column('foo', sa.Boolean,
                      server_default=sa.sql.expression.false()),
            sa.Column('bool_wo_default', sa.Boolean, unique=True),
            sa.Column('bar', sa.BigInteger),
            sa.Column('defaulttest', sa.Integer, server_default='7'),
            sa.Column('defaulttest2', sa.String(8), server_default=''),
            sa.Column('defaulttest3', sa.String(5), server_default="fake"),
            sa.Column('defaulttest4',
                      sa.Enum('first', 'second', name='testenum'),
                      server_default="first"),
            sa.Column("defaulttest5", sa.Integer, server_default=sa.text('0')),
            sa.Column('variant', sa.String(10)),
            sa.Column('fk_check', sa.String(36), nullable=False),
            sa.UniqueConstraint('spam', 'foo', name='uniq_cons'),
            sa.ForeignKeyConstraint(['fk_check'], ['table.fk_check']),
            mysql_engine='InnoDB'
        )

        msg = str(self.assertRaises(AssertionError, self.test_models_sync))
        # NOTE(I159): Check mentioning of the table and columns.
        # The log is invalid json, so we can't parse it and check it for
        # full compliance. We have no guarantee of the log items ordering,
        # so we can't use regexp.
        self.assertTrue(msg.startswith(
            'Models and migration scripts aren\'t in sync:'))
        self.assertIn('testtbl', msg)
        self.assertIn('spam', msg)
        self.assertIn('eggs', msg)  # test that the unique constraint is added
        self.assertIn('foo', msg)
        self.assertIn('bar', msg)
        self.assertIn('bool_wo_default', msg)
        self.assertIn('defaulttest', msg)
        self.assertIn('defaulttest3', msg)
        self.assertIn('remove_fk', msg)
        self.assertIn('variant', msg)


class ModelsMigrationsSyncMySQL(
    ModelsMigrationSyncMixin,
    migrate.ModelsMigrationsSync,
    db_test_base._MySQLOpportunisticTestCase,
):

    def test_models_not_sync(self):
        self._test_models_not_sync()

    def test_models_not_sync_filtered(self):
        self._test_models_not_sync_filtered()


class ModelsMigrationsSyncPostgreSQL(
    ModelsMigrationSyncMixin,
    migrate.ModelsMigrationsSync,
    db_test_base._PostgreSQLOpportunisticTestCase,
):

    def test_models_not_sync(self):
        self._test_models_not_sync()

    def test_models_not_sync_filtered(self):
        self._test_models_not_sync_filtered()
