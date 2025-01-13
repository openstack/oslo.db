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
import functools
import logging
import pprint
import re

import alembic
import alembic.autogenerate
import alembic.migration
import sqlalchemy
import sqlalchemy.exc
import sqlalchemy.sql.expression as expr
import sqlalchemy.types as types

from oslo_db.sqlalchemy import provision
from oslo_db.sqlalchemy import utils

LOG = logging.getLogger(__name__)


class ModelsMigrationsSync(metaclass=abc.ABCMeta):
    """A helper class for comparison of DB migration scripts and models.

    It's intended to be inherited by test cases in target projects. They have
    to provide implementations for methods used internally in the test (as
    we have no way to implement them here).

    test_model_sync() will run migration scripts for the engine provided and
    then compare the given metadata to the one reflected from the database.
    The difference between MODELS and MIGRATION scripts will be printed and
    the test will fail, if the difference is not empty. The return value is
    really a list of actions, that should be performed in order to make the
    current database schema state (i.e. migration scripts) consistent with
    models definitions. It's left up to developers to analyze the output and
    decide whether the models definitions or the migration scripts should be
    modified to make them consistent.

    Output::

        [(
            'add_table',
            description of the table from models
        ),
        (
            'remove_table',
            description of the table from database
        ),
        (
            'add_column',
            schema,
            table name,
            column description from models
        ),
        (
            'remove_column',
            schema,
            table name,
            column description from database
        ),
        (
            'add_index',
            description of the index from models
        ),
        (
            'remove_index',
            description of the index from database
        ),
        (
            'add_constraint',
            description of constraint from models
        ),
        (
            'remove_constraint,
            description of constraint from database
        ),
        (
            'modify_nullable',
            schema,
            table name,
            column name,
            {
                'existing_type': type of the column from database,
                'existing_server_default': default value from database
            },
            nullable from database,
            nullable from models
        ),
        (
            'modify_type',
            schema,
            table name,
            column name,
            {
                'existing_nullable': database nullable,
                'existing_server_default': default value from database
            },
            database column type,
            type of the column from models
        ),
        (
            'modify_default',
            schema,
            table name,
            column name,
            {
                'existing_nullable': database nullable,
                'existing_type': type of the column from database
            },
            connection column default value,
            default from models
        )]

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

        # Alembic <=0.8.4 do not contain logic of comparing Variant type with
        # others.
        if isinstance(meta_type, types.Variant):
            orig_type = meta_col.type
            impl_type = meta_type.load_dialect_impl(ctxt.dialect)
            meta_col.type = impl_type
            try:
                return self.compare_type(ctxt, insp_col, meta_col, insp_type,
                                         impl_type)
            finally:
                meta_col.type = orig_type

        return ctxt.impl.compare_type(insp_col, meta_col)

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
        return self._compare_server_default(ctxt.bind, meta_col, insp_def,
                                            meta_def)

    @utils.DialectFunctionDispatcher.dispatch_for_dialect("*")
    def _compare_server_default(bind, meta_col, insp_def, meta_def):
        pass

    @_compare_server_default.dispatch_for('mysql')
    def _compare_server_default(bind, meta_col, insp_def, meta_def):
        if isinstance(meta_col.type, sqlalchemy.Boolean):
            if meta_def is None or insp_def is None:
                return meta_def != insp_def
            insp_def = insp_def.strip("'")
            return not (
                isinstance(meta_def.arg, expr.True_) and insp_def == "1" or
                isinstance(meta_def.arg, expr.False_) and insp_def == "0"
            )

        if isinstance(meta_col.type, sqlalchemy.String):
            if meta_def is None or insp_def is None:
                return meta_def != insp_def
            insp_def = re.sub(r"^'|'$", "", insp_def)
            return meta_def.arg != insp_def

    def filter_metadata_diff(self, diff):
        """Filter changes before assert in test_models_sync().

        Allow subclasses to whitelist/blacklist changes. By default, no
        filtering is performed, changes are returned as is.

        :param diff: a list of differences (see `compare_metadata()` docs for
            details on format)
        :returns: a list of differences
        """
        return diff

    def test_models_sync(self):
        # drop all objects after a test run
        engine = self.get_engine()
        backend = provision.Backend(engine.name, engine.url)
        self.addCleanup(functools.partial(backend.drop_all_objects, engine))

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
            diff = self.filter_metadata_diff(
                alembic.autogenerate.compare_metadata(mc, self.get_metadata()))
            if diff:
                msg = pprint.pformat(diff, indent=2, width=20)
                self.fail(
                    "Models and migration scripts aren't in sync:\n%s" % msg)
