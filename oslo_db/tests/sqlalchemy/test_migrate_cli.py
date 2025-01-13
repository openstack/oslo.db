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

import alembic
import sqlalchemy

from oslo_db import exception
from oslo_db.sqlalchemy.migration_cli import ext_alembic
from oslo_db.sqlalchemy.migration_cli import manager
from oslo_db.tests import base as test_base


class MockWithCmp(mock.MagicMock):

    order = 0

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.__lt__ = lambda self, other: self.order < other.order


@mock.patch('oslo_db.sqlalchemy.migration_cli.'
            'ext_alembic.alembic.command')
class TestAlembicExtension(test_base.BaseTestCase):

    def setUp(self):
        self.migration_config = {'alembic_ini_path': '.',
                                 'db_url': 'sqlite://'}
        self.engine = sqlalchemy.create_engine(self.migration_config['db_url'])
        self.alembic = ext_alembic.AlembicExtension(
            self.engine, self.migration_config)
        super().setUp()

    def test_check_enabled_true(self, command):
        """Check enabled returns True

        Verifies that enabled returns True on non empty
        alembic_ini_path conf variable
        """
        self.assertTrue(self.alembic.enabled)

    def test_check_enabled_false(self, command):
        """Check enabled returns False

        Verifies enabled returns False on empty alembic_ini_path variable
        """
        self.migration_config['alembic_ini_path'] = ''
        alembic = ext_alembic.AlembicExtension(
            self.engine, self.migration_config)
        self.assertFalse(alembic.enabled)

    def test_upgrade_none(self, command):
        self.alembic.upgrade(None)
        command.upgrade.assert_called_once_with(self.alembic.config, 'head')

    def test_upgrade_normal(self, command):
        self.alembic.upgrade('131daa')
        command.upgrade.assert_called_once_with(self.alembic.config, '131daa')

    def test_downgrade_none(self, command):
        self.alembic.downgrade(None)
        command.downgrade.assert_called_once_with(self.alembic.config, 'base')

    def test_downgrade_int(self, command):
        self.alembic.downgrade(111)
        command.downgrade.assert_called_once_with(self.alembic.config, 'base')

    def test_downgrade_normal(self, command):
        self.alembic.downgrade('131daa')
        command.downgrade.assert_called_once_with(
            self.alembic.config, '131daa')

    def test_revision(self, command):
        self.alembic.revision(message='test', autogenerate=True)
        command.revision.assert_called_once_with(
            self.alembic.config, message='test', autogenerate=True)

    def test_stamp(self, command):
        self.alembic.stamp('stamp')
        command.stamp.assert_called_once_with(
            self.alembic.config, revision='stamp')

    def test_version(self, command):
        version = self.alembic.version()
        self.assertIsNone(version)

    def test_has_revision(self, command):
        with mock.patch('oslo_db.sqlalchemy.migration_cli.'
                        'ext_alembic.alembic_script') as mocked:
            self.alembic.config.get_main_option = mock.Mock()
            # since alembic_script is mocked and no exception is raised, call
            # will result in success
            self.assertIs(True, self.alembic.has_revision('test'))
            self.alembic.config.get_main_option.assert_called_once_with(
                'script_location')
            mocked.ScriptDirectory().get_revision.assert_called_once_with(
                'test')
            self.assertIs(True, self.alembic.has_revision(None))
            self.assertIs(True, self.alembic.has_revision('head'))
            # relative revision, should be True for alembic
            self.assertIs(True, self.alembic.has_revision('+1'))

    def test_has_revision_negative(self, command):
        with mock.patch('oslo_db.sqlalchemy.migration_cli.'
                        'ext_alembic.alembic_script') as mocked:
            mocked.ScriptDirectory().get_revision.side_effect = (
                alembic.util.CommandError)
            self.alembic.config.get_main_option = mock.Mock()
            # exception is raised, the call should be false
            self.assertIs(False, self.alembic.has_revision('test'))
            self.alembic.config.get_main_option.assert_called_once_with(
                'script_location')
            mocked.ScriptDirectory().get_revision.assert_called_once_with(
                'test')


class TestMigrationManager(test_base.BaseTestCase):

    def setUp(self):
        self.migration_config = {'alembic_ini_path': '.',
                                 'migrate_repo_path': '.',
                                 'db_url': 'sqlite://'}
        engine = sqlalchemy.create_engine(self.migration_config['db_url'])
        self.migration_manager = manager.MigrationManager(
            self.migration_config, engine)
        self.ext = mock.Mock()
        self.ext.obj.version = mock.Mock(return_value=0)
        self.migration_manager._manager.extensions = [self.ext]
        super().setUp()

    def test_manager_update(self):
        self.migration_manager.upgrade('head')
        self.ext.obj.upgrade.assert_called_once_with('head')

    def test_manager_update_revision_none(self):
        self.migration_manager.upgrade(None)
        self.ext.obj.upgrade.assert_called_once_with(None)

    def test_downgrade_normal_revision(self):
        self.migration_manager.downgrade('111abcd')
        self.ext.obj.downgrade.assert_called_once_with('111abcd')

    def test_version(self):
        self.migration_manager.version()
        self.ext.obj.version.assert_called_once_with()

    def test_version_return_value(self):
        version = self.migration_manager.version()
        self.assertEqual(0, version)

    def test_revision_message_autogenerate(self):
        self.migration_manager.revision('test', True)
        self.ext.obj.revision.assert_called_once_with('test', True)

    def test_revision_only_message(self):
        self.migration_manager.revision('test', False)
        self.ext.obj.revision.assert_called_once_with('test', False)

    def test_stamp(self):
        self.migration_manager.stamp('stamp')
        self.ext.obj.stamp.assert_called_once_with('stamp')

    def test_wrong_config(self):
        err = self.assertRaises(ValueError,
                                manager.MigrationManager,
                                {'wrong_key': 'sqlite://'})
        self.assertEqual('Either database url or engine must be provided.',
                         err.args[0])


class TestMigrationMultipleExtensions(test_base.BaseTestCase):

    def setUp(self):
        self.migration_config = {'alembic_ini_path': '.',
                                 'migrate_repo_path': '.',
                                 'db_url': 'sqlite://'}
        engine = sqlalchemy.create_engine(self.migration_config['db_url'])
        self.migration_manager = manager.MigrationManager(
            self.migration_config, engine)
        self.first_ext = MockWithCmp()
        self.first_ext.obj.order = 1
        self.first_ext.obj.upgrade.return_value = 100
        self.first_ext.obj.downgrade.return_value = 0
        self.second_ext = MockWithCmp()
        self.second_ext.obj.order = 2
        self.second_ext.obj.upgrade.return_value = 200
        self.second_ext.obj.downgrade.return_value = 100
        self.migration_manager._manager.extensions = [self.first_ext,
                                                      self.second_ext]
        super().setUp()

    def test_upgrade_right_order(self):
        results = self.migration_manager.upgrade(None)
        self.assertEqual([100, 200], results)

    def test_downgrade_right_order(self):
        results = self.migration_manager.downgrade(None)
        self.assertEqual([100, 0], results)

    def test_upgrade_does_not_go_too_far(self):
        self.first_ext.obj.has_revision.return_value = True
        self.second_ext.obj.has_revision.return_value = False
        self.second_ext.obj.upgrade.side_effect = AssertionError(
            'this method should not have been called')

        results = self.migration_manager.upgrade(100)
        self.assertEqual([100], results)

    def test_downgrade_does_not_go_too_far(self):
        self.second_ext.obj.has_revision.return_value = True
        self.first_ext.obj.has_revision.return_value = False
        self.first_ext.obj.downgrade.side_effect = AssertionError(
            'this method should not have been called')

        results = self.migration_manager.downgrade(100)
        self.assertEqual([100], results)

    def test_upgrade_checks_rev_existence(self):
        self.first_ext.obj.has_revision.return_value = False
        self.second_ext.obj.has_revision.return_value = False

        # upgrade to a specific non-existent revision should fail
        self.assertRaises(exception.DBMigrationError,
                          self.migration_manager.upgrade, 100)

        # upgrade to the "head" should succeed
        self.assertEqual([100, 200], self.migration_manager.upgrade(None))

        # let's assume the second ext has the revision, upgrade should succeed
        self.second_ext.obj.has_revision.return_value = True
        self.assertEqual([100, 200], self.migration_manager.upgrade(200))

        # upgrade to the "head" should still succeed
        self.assertEqual([100, 200], self.migration_manager.upgrade(None))

    def test_downgrade_checks_rev_existence(self):
        self.first_ext.obj.has_revision.return_value = False
        self.second_ext.obj.has_revision.return_value = False

        # upgrade to a specific non-existent revision should fail
        self.assertRaises(exception.DBMigrationError,
                          self.migration_manager.downgrade, 100)

        # downgrade to the "base" should succeed
        self.assertEqual([100, 0], self.migration_manager.downgrade(None))

        # let's assume the second ext has the revision, downgrade should
        # succeed
        self.first_ext.obj.has_revision.return_value = True
        self.assertEqual([100, 0], self.migration_manager.downgrade(200))

        # downgrade to the "base" should still succeed
        self.assertEqual([100, 0], self.migration_manager.downgrade(None))
        self.assertEqual([100, 0], self.migration_manager.downgrade('base'))
