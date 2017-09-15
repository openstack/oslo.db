# coding=utf-8

# Copyright (c) 2013 OpenStack Foundation
# All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# Base on code in migrate/changeset/databases/sqlite.py which is under
# the following license:
#
# The MIT License
#
# Copyright (c) 2009 Evan Rosson, Jan Dittberner, Domen Kožar
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

import os

from migrate import exceptions as versioning_exceptions
from migrate.versioning import api as versioning_api
from migrate.versioning.repository import Repository
import sqlalchemy

from oslo_db._i18n import _
from oslo_db import exception


def db_sync(engine, abs_path, version=None, init_version=0, sanity_check=True):
    """Upgrade or downgrade a database.

    Function runs the upgrade() or downgrade() functions in change scripts.

    :param engine:       SQLAlchemy engine instance for a given database
    :param abs_path:     Absolute path to migrate repository.
    :param version:      Database will upgrade/downgrade until this version.
                         If None - database will update to the latest
                         available version.
    :param init_version: Initial database version
    :param sanity_check: Require schema sanity checking for all tables
    """

    if version is not None:
        try:
            version = int(version)
        except ValueError:
            raise exception.DBMigrationError(_("version should be an integer"))

    current_version = db_version(engine, abs_path, init_version)
    repository = _find_migrate_repo(abs_path)
    if sanity_check:
        _db_schema_sanity_check(engine)
    if version is None or version > current_version:
        try:
            migration = versioning_api.upgrade(engine, repository, version)
        except Exception as ex:
            raise exception.DBMigrationError(ex)
    else:
        migration = versioning_api.downgrade(engine, repository,
                                             version)
    if sanity_check:
        _db_schema_sanity_check(engine)

    return migration


def _db_schema_sanity_check(engine):
    """Ensure all database tables were created with required parameters.

    :param engine:  SQLAlchemy engine instance for a given database

    """

    if engine.name == 'mysql':
        onlyutf8_sql = ('SELECT TABLE_NAME,TABLE_COLLATION '
                        'from information_schema.TABLES '
                        'where TABLE_SCHEMA=%s and '
                        'TABLE_COLLATION NOT LIKE \'%%utf8%%\'')

        # NOTE(morganfainberg): exclude the sqlalchemy-migrate and alembic
        # versioning tables from the tables we need to verify utf8 status on.
        # Non-standard table names are not supported.
        EXCLUDED_TABLES = ['migrate_version', 'alembic_version']

        table_names = [res[0] for res in
                       engine.execute(onlyutf8_sql, engine.url.database) if
                       res[0].lower() not in EXCLUDED_TABLES]

        if len(table_names) > 0:
            raise ValueError(_('Tables "%s" have non utf8 collation, '
                               'please make sure all tables are CHARSET=utf8'
                               ) % ','.join(table_names))


def db_version(engine, abs_path, init_version):
    """Show the current version of the repository.

    :param engine:  SQLAlchemy engine instance for a given database
    :param abs_path: Absolute path to migrate repository
    :param init_version:  Initial database version
    """
    repository = _find_migrate_repo(abs_path)
    try:
        return versioning_api.db_version(engine, repository)
    except versioning_exceptions.DatabaseNotControlledError:
        meta = sqlalchemy.MetaData()
        meta.reflect(bind=engine)
        tables = meta.tables
        if (len(tables) == 0 or 'alembic_version' in tables or
                'migrate_version' in tables):
            db_version_control(engine, abs_path, version=init_version)
            return versioning_api.db_version(engine, repository)
        else:
            raise exception.DBMigrationError(
                _("The database is not under version control, but has "
                  "tables. Please stamp the current version of the schema "
                  "manually."))


def db_version_control(engine, abs_path, version=None):
    """Mark a database as under this repository's version control.

    Once a database is under version control, schema changes should
    only be done via change scripts in this repository.

    :param engine:  SQLAlchemy engine instance for a given database
    :param abs_path: Absolute path to migrate repository
    :param version:  Initial database version
    """
    repository = _find_migrate_repo(abs_path)

    try:
        versioning_api.version_control(engine, repository, version)
    except versioning_exceptions.InvalidVersionError as ex:
        raise exception.DBMigrationError("Invalid version : %s" % ex)
    except versioning_exceptions.DatabaseAlreadyControlledError:
        raise exception.DBMigrationError("Database is already controlled.")

    return version


def _find_migrate_repo(abs_path):
    """Get the project's change script repository

    :param abs_path: Absolute path to migrate repository
    """
    if not os.path.exists(abs_path):
        raise exception.DBMigrationError("Path %s not found" % abs_path)
    return Repository(abs_path)
