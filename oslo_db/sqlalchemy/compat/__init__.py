# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

from oslo_utils import versionutils

from sqlalchemy import __version__


_vers = versionutils.convert_version_to_tuple(__version__)
sqla_2 = _vers >= (2, )

native_pre_ping_event_support = _vers >= (2, 0, 5)


def dialect_from_exception_context(ctx):
    if sqla_2:
        # SQLAlchemy 2.0 still has context.engine, however if the
        # exception context is called in the context of a ping handler,
        # engine is not present.  need to use dialect instead
        return ctx.dialect
    else:
        return ctx.engine.dialect


def driver_connection(connection):
    if sqla_2:
        return connection.connection.driver_connection
    else:
        return connection.connection.connection
