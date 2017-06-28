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

from sqlalchemy import String, event, schema
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.types import VARCHAR

engine_regex = re.compile("engine=innodb", re.IGNORECASE)
trans_regex = re.compile("savepoint|rollback|release savepoint", re.IGNORECASE)


def enable_ndb_support(engine):
    """Enable NDB Support.

    Function to flag the MySQL engine dialect to support features specific
    to MySQL Cluster (NDB).
    """
    engine.dialect._oslodb_enable_ndb_support = True


def ndb_status(engine_or_compiler):
    """Test if NDB Support is enabled.

    Function to test if NDB support is enabled or not.
    """
    return getattr(engine_or_compiler.dialect,
                   '_oslodb_enable_ndb_support',
                   False)


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
        if ndb_status(engine):
            statement = engine_regex.sub("ENGINE=NDBCLUSTER", statement)
            if re.match(trans_regex, statement):
                statement = "SET @oslo_db_ndb_savepoint_rollback_disabled = 0;"

        return statement, parameters


@compiles(schema.CreateTable, "mysql")
def prefix_inserts(create_table, compiler, **kw):
    """Replace InnoDB with NDBCLUSTER automatically.

    Function will intercept CreateTable() calls and automatically
    convert InnoDB to NDBCLUSTER. Targets compiler events.
    """
    existing = compiler.visit_create_table(create_table, **kw)
    if ndb_status(compiler):
        existing = engine_regex.sub("ENGINE=NDBCLUSTER", existing)

    return existing


class AutoStringTinyText(String):
    """Class definition for AutoStringTinyText.

    Class is used by compiler function _auto-string_tiny_text().
    """

    pass


@compiles(AutoStringTinyText, 'mysql')
def _auto_string_tiny_text(element, compiler, **kw):
    if ndb_status(compiler):
        return "TINYTEXT"
    else:
        return compiler.visit_string(element, **kw)


class AutoStringText(String):
    """Class definition for AutoStringText.

    Class is used by compiler function _auto_string_text().
    """

    pass


@compiles(AutoStringText, 'mysql')
def _auto_string_text(element, compiler, **kw):
    if ndb_status(compiler):
        return "TEXT"
    else:
        return compiler.visit_string(element, **kw)


class AutoStringSize(String):
    """Class definition for AutoStringSize.

    Class is used by the compiler function _auto_string_size().
    """

    def __init__(self, length, ndb_size, **kw):
        """Initialize and extend the String arguments.

        Function adds the innodb_size and ndb_size arguments to the
        function String().
        """
        super(AutoStringSize, self).__init__(length=length, **kw)
        self.ndb_size = ndb_size
        self.length = length


@compiles(AutoStringSize, 'mysql')
def _auto_string_size(element, compiler, **kw):
    if ndb_status(compiler):
        return compiler.process(VARCHAR(element.ndb_size), **kw)
    else:
        return compiler.visit_string(element, **kw)
