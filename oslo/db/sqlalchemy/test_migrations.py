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

import abc
import logging
import pprint

import alembic
import alembic.autogenerate
import alembic.migration
import pkg_resources as pkg
import six
import sqlalchemy
from sqlalchemy.engine import reflection
import sqlalchemy.exc
from sqlalchemy import schema
import sqlalchemy.sql.expression as expr
import sqlalchemy.types as types

from oslo.db import exception as exc
from oslo.db.openstack.common.gettextutils import _LE

LOG = logging.getLogger(__name__)


@six.add_metaclass(abc.ABCMeta)
class WalkVersionsMixin(object):
    """Test mixin to check upgrade and downgrade ability of migration.

    This is only suitable for testing of migrate_ migration scripts. An
    abstract class mixin. `INIT_VERSION`, `REPOSITORY` and `migration_api`
    attributes must be implemented in subclasses.

    .. _auxiliary-dynamic-methods:

    Auxiliary methods
    -----------------

    `_migrate_up` and `_migrate_down` instance methods of the class can be
    used with auxiliary methods named `_pre_upgrade_<revision_id>`,
    `_check_<revision_id>`, `_post_downgrade_<revision_id>`. The methods
    intended to check applied changes for correctness of data operations.
    This methods should be implemented for every particular revision
    which you want to check with data. Implementation recommendations for
    `_pre_upgrade_<revision_id>`, `_check_<revision_id>`,
    `_post_downgrade_<revision_id>` implementation:

        * `_pre_upgrade_<revision_id>`: provide a data appropriate to a
        next revision. Should be used an id of revision which going to be
        applied.

        * `_check_<revision_id>`: Insert, select, delete operations with
        newly applied changes. The data provided by
        `_pre_upgrade_<revision_id>` will be used.

        *`_post_downgrade_<revision_id>`: check for absence (inability to
        use) changes provided by reverted revision.

    Execution order of auxiliary methods when revision is upgrading:

        `_pre_upgrade_###` => `upgrade` => `_check_###`

    Execution order of auxiliary methods when revision is downgrading:

        `downgrade` => `_post_downgrade_###`

    .. _migrate: https://sqlalchemy-migrate.readthedocs.org/en/latest/
    """

    @abc.abstractproperty
    def INIT_VERSION(self):
        """Initial version of a migration repository.

        Can be different from 0, if a migrations were squashed.

        :rtype: int
        """
        pass

    @abc.abstractproperty
    def REPOSITORY(self):
        """Allows basic manipulation with migration repository.

        :returns: `migrate.versioning.repository.Repository` subclass.
        """
        pass

    @abc.abstractproperty
    def migration_api(self):
        """Provides API for upgrading, downgrading and version manipulations.

        :returns: `migrate.api` or overloaded analog.
        """
        pass

    @abc.abstractproperty
    def migrate_engine(self):
        """Provides engine instance.

        Should be the same instance as used when migrations are applied. In
        most cases, the `engine` attribute provided by the test class in a
        `setUp` method will work.

        Example of implementation:

            def migrate_engine(self):
                return self.engine

        :returns: sqlalchemy engine instance
        """
        pass

    def _walk_versions(self, snake_walk=False, downgrade=True):
        """Check if migration upgrades and downgrades successfully.

        Determine the latest version script from the repo, then
        upgrade from 1 through to the latest, with no data
        in the databases. This just checks that the schema itself
        upgrades successfully.

        `_walk_versions` calls `_migrate_up` and `_migrate_down` with
        `with_data` argument to check changes with data, but these methods
        can be called without any extra check outside of `_walk_versions`
        method.

        :param snake_walk: enables checking that each individual migration can
            be upgraded/downgraded by itself.

            If we have ordered migrations 123abc, 456def, 789ghi and we run
            upgrading with the `snake_walk` argument set to `True`, the
            migrations will be applied in the following order:

                `123abc => 456def => 123abc =>
                 456def => 789ghi => 456def => 789ghi`

        :type snake_walk: bool
        :param downgrade: Check downgrade behavior if True.
        :type downgrade: bool
        """

        # Place the database under version control
        self.migration_api.version_control(self.migrate_engine,
                                           self.REPOSITORY,
                                           self.INIT_VERSION)
        self.assertEqual(self.INIT_VERSION,
                         self.migration_api.db_version(self.migrate_engine,
                                                       self.REPOSITORY))

        LOG.debug('latest version is %s', self.REPOSITORY.latest)
        versions = range(self.INIT_VERSION + 1, self.REPOSITORY.latest + 1)

        for version in versions:
            # upgrade -> downgrade -> upgrade
            self._migrate_up(version, with_data=True)
            if snake_walk:
                downgraded = self._migrate_down(version - 1, with_data=True)
                if downgraded:
                    self._migrate_up(version)

        if downgrade:
            # Now walk it back down to 0 from the latest, testing
            # the downgrade paths.
            for version in reversed(versions):
                # downgrade -> upgrade -> downgrade
                downgraded = self._migrate_down(version - 1)

                if snake_walk and downgraded:
                    self._migrate_up(version)
                    self._migrate_down(version - 1)

    def _migrate_down(self, version, with_data=False):
        """Migrate down to a previous version of the db.

        :param version: id of revision to downgrade.
        :type version: str
        :keyword with_data: Whether to verify the absence of changes from
            migration(s) being downgraded, see
            :ref:`auxiliary-dynamic-methods`.
        :type with_data: Bool
        """

        try:
            self.migration_api.downgrade(self.migrate_engine,
                                         self.REPOSITORY, version)
        except NotImplementedError:
            # NOTE(sirp): some migrations, namely release-level
            # migrations, don't support a downgrade.
            return False

        self.assertEqual(version, self.migration_api.db_version(
            self.migrate_engine, self.REPOSITORY))

        # NOTE(sirp): `version` is what we're downgrading to (i.e. the 'target'
        # version). So if we have any downgrade checks, they need to be run for
        # the previous (higher numbered) migration.
        if with_data:
            post_downgrade = getattr(
                self, "_post_downgrade_%03d" % (version + 1), None)
            if post_downgrade:
                post_downgrade(self.migrate_engine)

        return True

    def _migrate_up(self, version, with_data=False):
        """Migrate up to a new version of the db.

        :param version: id of revision to upgrade.
        :type version: str
        :keyword with_data: Whether to verify the applied changes with data,
            see :ref:`auxiliary-dynamic-methods`.
        :type with_data: Bool
        """
        # NOTE(sdague): try block is here because it's impossible to debug
        # where a failed data migration happens otherwise
        try:
            if with_data:
                data = None
                pre_upgrade = getattr(
                    self, "_pre_upgrade_%03d" % version, None)
                if pre_upgrade:
                    data = pre_upgrade(self.migrate_engine)

            self.migration_api.upgrade(self.migrate_engine,
                                       self.REPOSITORY, version)
            self.assertEqual(version,
                             self.migration_api.db_version(self.migrate_engine,
                                                           self.REPOSITORY))
            if with_data:
                check = getattr(self, "_check_%03d" % version, None)
                if check:
                    check(self.migrate_engine, data)
        except exc.DbMigrationError:
            msg = _LE("Failed to migrate to version %(ver)s on engine %(eng)s")
            LOG.error(msg, {"ver": version, "eng": self.migrate_engine})
            raise


@six.add_metaclass(abc.ABCMeta)
class ModelsMigrationsSync(object):
    """A helper class for comparison of DB migration scripts and models.

    It's intended to be inherited by test cases in target projects. They have
    to provide implementations for methods used internally in the test (as
    we have no way to implement them here).

    test_model_sync() will run migration scripts for the engine provided and
    then compare the given metadata to the one reflected from the database.
    The difference between MODELS and MIGRATION scripts will be printed and
    the test will fail, if the difference is not empty.

    Method include_object() can be overridden to exclude some tables from
    comparison (e.g. migrate_repo).

    """

    @abc.abstractmethod
    def db_sync(self, engine):
        """Run migration scripts with the given engine instance.

        This method must be implemented in subclasses and run migration scripts
        for a DB the given engine is connected to.

        """

    @abc.abstractmethod
    def get_engine(self):
        """Return the engine instance to be used when running tests.

        This method must be implemented in subclasses and return an engine
        instance to be used when running tests.

        """

    @abc.abstractmethod
    def get_metadata(self):
        """Return the metadata instance to be used for schema comparison.

        This method must be implemented in subclasses and return the metadata
        instance attached to the BASE model.

        """

    def include_object(self, object_, name, type_, reflected, compare_to):
        """Return True for objects that should be compared.

        :param object_: a SchemaItem object such as a Table or Column object
        :param name: the name of the object
        :param type_: a string describing the type of object (e.g. "table")
        :param reflected: True if the given object was produced based on
                          table reflection, False if it's from a local
                          MetaData object
        :param compare_to: the object being compared against, if available,
                           else None

        """

        return True

    def compare_type(self, ctxt, insp_col, meta_col, insp_type, meta_type):
        """Return True if types are different, False if not.

        Return None to allow the default implementation to compare these types.

        :param ctxt: alembic MigrationContext instance
        :param insp_col: reflected column
        :param meta_col: column from model
        :param insp_type: reflected column type
        :param meta_type: column type from model

        """

        # some backends (e.g. mysql) don't provide native boolean type
        BOOLEAN_METADATA = (types.BOOLEAN, types.Boolean)
        BOOLEAN_SQL = BOOLEAN_METADATA + (types.INTEGER, types.Integer)

        if issubclass(type(meta_type), BOOLEAN_METADATA):
            return not issubclass(type(insp_type), BOOLEAN_SQL)

        return None  # tells alembic to use the default comparison method

    def compare_server_default(self, ctxt, ins_col, meta_col,
                               insp_def, meta_def, rendered_meta_def):
        """Compare default values between model and db table.

        Return True if the defaults are different, False if not, or None to
        allow the default implementation to compare these defaults.

        :param ctxt: alembic MigrationContext instance
        :param insp_col: reflected column
        :param meta_col: column from model
        :param insp_def: reflected column default value
        :param meta_def: column default value from model
        :param rendered_meta_def: rendered column default value (from model)

        """

        if (ctxt.dialect.name == 'mysql' and
                issubclass(type(meta_col.type), sqlalchemy.Boolean)):

            if meta_def is None or insp_def is None:
                return meta_def != insp_def

            return not (
                isinstance(meta_def.arg, expr.True_) and insp_def == "'1'" or
                isinstance(meta_def.arg, expr.False_) and insp_def == "'0'"
            )

        return None  # tells alembic to use the default comparison method

    def _cleanup(self):
        engine = self.get_engine()
        with engine.begin() as conn:
            inspector = reflection.Inspector.from_engine(engine)
            metadata = schema.MetaData()
            tbs = []
            all_fks = []

            for table_name in inspector.get_table_names():
                fks = []
                for fk in inspector.get_foreign_keys(table_name):
                    if not fk['name']:
                        continue
                    fks.append(
                        schema.ForeignKeyConstraint((), (), name=fk['name'])
                        )
                table = schema.Table(table_name, metadata, *fks)
                tbs.append(table)
                all_fks.extend(fks)

            for fkc in all_fks:
                conn.execute(schema.DropConstraint(fkc))

            for table in tbs:
                conn.execute(schema.DropTable(table))

    def test_models_sync(self):
        # recent versions of sqlalchemy and alembic are needed for running of
        # this test, but we already have them in requirements
        try:
            pkg.require('sqlalchemy>=0.8.4', 'alembic>=0.6.2')
        except (pkg.VersionConflict, pkg.DistributionNotFound) as e:
            self.skipTest('sqlalchemy>=0.8.4 and alembic>=0.6.3 are required'
                          ' for running of this test: %s' % e)

        # drop all tables after a test run
        self.addCleanup(self._cleanup)

        # run migration scripts
        self.db_sync(self.get_engine())

        with self.get_engine().connect() as conn:
            opts = {
                'include_object': self.include_object,
                'compare_type': self.compare_type,
                'compare_server_default': self.compare_server_default,
            }
            mc = alembic.migration.MigrationContext.configure(conn, opts=opts)

            # compare schemas and fail with diff, if it's not empty
            diff = alembic.autogenerate.compare_metadata(mc,
                                                         self.get_metadata())
            if diff:
                msg = pprint.pformat(diff, indent=2, width=20)
                self.fail(
                    "Models and migration scripts aren't in sync:\n%s" % msg)
