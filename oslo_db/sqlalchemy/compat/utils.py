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
import re

import sqlalchemy


SQLA_VERSION = tuple(
    int(num) if re.match(r'^\d+$', num) else num
    for num in sqlalchemy.__version__.split(".")
)

sqla_100 = SQLA_VERSION >= (1, 0, 0)
sqla_097 = SQLA_VERSION >= (0, 9, 7)
sqla_094 = SQLA_VERSION >= (0, 9, 4)
sqla_090 = SQLA_VERSION >= (0, 9, 0)
sqla_08 = SQLA_VERSION >= (0, 8)


def get_postgresql_enums(conn):
    """Return a list of ENUM type names on a Postgresql backend.

    For SQLAlchemy 0.9 and lower, makes use of the semi-private
    _load_enums() method of the Postgresql dialect.  In SQLAlchemy
    1.0 this feature is supported using get_enums().

    This function may only be called when the given connection
    is against the Postgresql backend.  It will fail for other
    kinds of backends.

    """
    if sqla_100:
        return [e['name'] for e in sqlalchemy.inspect(conn).get_enums()]
    else:
        return conn.dialect._load_enums(conn).keys()
