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

from sqlalchemy.types import TypeDecorator, Text
from sqlalchemy.dialects import mysql


class JsonEncodedType(TypeDecorator):
    """Base column type for data serialized as JSON-encoded string in db."""
    type = None
    impl = Text

    def __init__(self, mysql_as_long=False, mysql_as_medium=False):
        super(JsonEncodedType, self).__init__()

        if mysql_as_long and mysql_as_medium:
            raise TypeError("mysql_as_long and mysql_as_medium are mutually "
                            "exclusive")

        if mysql_as_long:
            self.impl = Text().with_variant(mysql.LONGTEXT(), 'mysql')
        elif mysql_as_medium:
            self.impl = Text().with_variant(mysql.MEDIUMTEXT(), 'mysql')

    def process_bind_param(self, value, dialect):
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
