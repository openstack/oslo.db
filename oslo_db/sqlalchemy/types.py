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

import json

from sqlalchemy.dialects import mysql
from sqlalchemy.types import Integer, Text, TypeDecorator, String as _String


class JsonEncodedType(TypeDecorator):
    """Base column type for data serialized as JSON-encoded string in db."""

    type = None
    impl = Text
    cache_ok = True
    """This type is safe to cache."""

    def __init__(self, mysql_as_long=False, mysql_as_medium=False):
        """Initialize JSON-encoding type."""
        super(JsonEncodedType, self).__init__()

        if mysql_as_long and mysql_as_medium:
            raise TypeError("mysql_as_long and mysql_as_medium are mutually "
                            "exclusive")

        if mysql_as_long:
            self.impl = Text().with_variant(mysql.LONGTEXT(), 'mysql')
        elif mysql_as_medium:
            self.impl = Text().with_variant(mysql.MEDIUMTEXT(), 'mysql')

    def process_bind_param(self, value, dialect):
        """Bind parameters to the process."""
        if value is None:
            if self.type is not None:
                # Save default value according to current type to keep the
                # interface consistent.
                value = self.type()
        elif self.type is not None and not isinstance(value, self.type):
            raise TypeError("%s supposes to store %s objects, but %s given"
                            % (self.__class__.__name__,
                               self.type.__name__,
                               type(value).__name__))
        serialized_value = json.dumps(value)
        return serialized_value

    def process_result_value(self, value, dialect):
        """Process result value."""
        if value is not None:
            value = json.loads(value)
        return value


class JsonEncodedDict(JsonEncodedType):
    """Represents dict serialized as json-encoded string in db.

    Note that this type does NOT track mutations. If you want to update it, you
    have to assign existing value to a temporary variable, update, then assign
    back. See this page for more robust work around:
    http://docs.sqlalchemy.org/en/rel_1_0/orm/extensions/mutable.html
    """

    type = dict


class JsonEncodedList(JsonEncodedType):
    """Represents list serialized as json-encoded string in db.

    Note that this type does NOT track mutations. If you want to update it, you
    have to assign existing value to a temporary variable, update, then assign
    back. See this page for more robust work around:
    http://docs.sqlalchemy.org/en/rel_1_0/orm/extensions/mutable.html
    """

    type = list


class SoftDeleteInteger(TypeDecorator):
    """Coerce a bound param to be a proper integer before passing it to DBAPI.

    Some backends like PostgreSQL are very strict about types and do not
    perform automatic type casts, e.g. when trying to INSERT a boolean value
    like ``false`` into an integer column. Coercing of the bound param in DB
    layer by the means of a custom SQLAlchemy type decorator makes sure we
    always pass a proper integer value to a DBAPI implementation.

    This is not a general purpose boolean integer type as it specifically
    allows for arbitrary positive integers outside of the boolean int range
    (0, 1, False, True), so that it's possible to have compound unique
    constraints over multiple columns including ``deleted`` (e.g. to
    soft-delete flavors with the same name in Nova without triggering
    a constraint violation): ``deleted`` is set to be equal to a PK
    int value on deletion, 0 denotes a non-deleted row.

    """

    impl = Integer
    cache_ok = True
    """This type is safe to cache."""

    def process_bind_param(self, value, dialect):
        """Return the binding parameter."""
        if value is None:
            return None
        return int(value)


class String(_String):
    """String subclass that implements oslo_db specific options.

    Initial goal is to support ndb-specific flags.

    mysql_ndb_type is used to override the String with another data type.
    mysql_ndb_size is used to adjust the length of the String.

    """

    def __init__(
            self, length, mysql_ndb_length=None, mysql_ndb_type=None, **kw):
        """Initialize options."""
        super(String, self).__init__(length, **kw)
        self.mysql_ndb_type = mysql_ndb_type
        self.mysql_ndb_length = mysql_ndb_length
