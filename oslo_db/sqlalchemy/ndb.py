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
"""Core functions for MySQL Cluster (NDB) Support."""

import re

import debtcollector.removals
from sqlalchemy import event, schema
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.types import String as _String
from sqlalchemy.types import to_instance

from oslo_db.sqlalchemy.types import String


engine_regex = re.compile("engine=innodb", re.IGNORECASE)
trans_regex = re.compile("savepoint|rollback|release savepoint", re.IGNORECASE)


@debtcollector.removals.remove(
    message=(
        'Support for the MySQL NDB Cluster storage engine has been deprecated '
        'and will be removed in a future release.'
    ),
    version='12.1.0',
)
def enable_ndb_support(engine):
    """Enable NDB Support.

    Function to flag the MySQL engine dialect to support features specific
    to MySQL Cluster (NDB).
    """
    engine.dialect._oslodb_enable_ndb_support = True


def _ndb_status(engine_or_compiler):
    """Test if NDB Support is enabled.

    Function to test if NDB support is enabled or not.

    .. note::

        This is for internal use only while we deprecate and remove ndb
        support. **Do not use this outside of oslo.db!**
    """
    return getattr(
        engine_or_compiler.dialect,
        '_oslodb_enable_ndb_support',
        False,
    )


@debtcollector.removals.remove(
    message=(
        'Support for the MySQL NDB Cluster storage engine has been deprecated '
        'and will be removed in a future release.'
    ),
    version='12.1.0',
)
def ndb_status(engine_or_compiler):
    """Test if NDB Support is enabled.

    Function to test if NDB support is enabled or not.
    """
    return _ndb_status(engine_or_compiler)


@debtcollector.removals.remove(
    message=(
        'Support for the MySQL NDB Cluster storage engine has been deprecated '
        'and will be removed in a future release.'
    ),
    version='12.1.0',
)
def init_ndb_events(engine):
    """Initialize NDB Events.

    Function starts NDB specific events.
    """
    @event.listens_for(engine, "before_cursor_execute", retval=True)
    def before_cursor_execute(conn, cursor, statement, parameters, context,
                              executemany):
        """Listen for specific SQL strings and replace automatically.

        Function will intercept any raw execute calls and automatically
        convert InnoDB to NDBCLUSTER, drop SAVEPOINT requests, drop
        ROLLBACK requests, and drop RELEASE SAVEPOINT requests.
        """
        if _ndb_status(engine):
            statement = engine_regex.sub("ENGINE=NDBCLUSTER", statement)
            if re.match(trans_regex, statement):
                statement = "SET @oslo_db_ndb_savepoint_rollback_disabled = 0;"

        return statement, parameters


# TODO(stephenfin): This is effectively deprecated and should be removed when
# we remove the rest of this module since it'll be a no-op then.
@compiles(schema.CreateTable, "mysql")
def prefix_inserts(create_table, compiler, **kw):
    """Replace InnoDB with NDBCLUSTER automatically.

    Function will intercept CreateTable() calls and automatically
    convert InnoDB to NDBCLUSTER. Targets compiler events.
    """
    existing = compiler.visit_create_table(create_table, **kw)
    if _ndb_status(compiler):
        existing = engine_regex.sub("ENGINE=NDBCLUSTER", existing)

    return existing


# TODO(stephenfin): This is effectively deprecated and should be removed when
# we remove the rest of this module since it'll be a no-op then.
@compiles(String, "mysql")
def _compile_ndb_string(element, compiler, **kw):
    """Process ndb specific overrides for String.

    Function will intercept mysql_ndb_length and mysql_ndb_type
    arguments to adjust columns automatically.

    mysql_ndb_length argument will adjust the String length
    to the requested value.

    mysql_ndb_type will change the column type to the requested
    data type.
    """
    if not _ndb_status(compiler):
        return compiler.visit_string(element, **kw)

    if element.mysql_ndb_length:
        effective_type = element.adapt(
            _String, length=element.mysql_ndb_length)
        return compiler.visit_string(effective_type, **kw)
    elif element.mysql_ndb_type:
        effective_type = to_instance(element.mysql_ndb_type)
        return compiler.process(effective_type, **kw)
    else:
        return compiler.visit_string(element, **kw)
