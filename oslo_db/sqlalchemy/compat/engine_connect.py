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
"""Provide forwards compatibility for the engine_connect event.

See the "engine_connect" event at
http://docs.sqlalchemy.org/en/rel_0_9/core/events.html.


"""

from sqlalchemy.engine import Engine
from sqlalchemy import event

from oslo_db.sqlalchemy.compat import utils


def engine_connect(engine, listener):
    """Add an engine_connect listener for the given :class:`.Engine`.

    This listener uses the SQLAlchemy
    :meth:`sqlalchemy.event.ConnectionEvents.engine_connect`
    event for 0.9.0 and above, and implements an interim listener
    for 0.8 versions.

    """
    if utils.sqla_090:
        event.listen(engine, "engine_connect", listener)
        return

    assert isinstance(engine, Engine), \
        "engine argument must be an Engine instance, not a Connection"

    if not getattr(engine._connection_cls,
                   '_oslo_engine_connect_wrapper', False):
        engine._oslo_engine_connect_events = []

        class Connection(engine._connection_cls):
            _oslo_engine_connect_wrapper = True

            def __init__(self, *arg, **kw):
                super(Connection, self).__init__(*arg, **kw)

                _oslo_engine_connect_events = getattr(
                    self.engine,
                    '_oslo_engine_connect_events',
                    False)
                if _oslo_engine_connect_events:
                    for fn in _oslo_engine_connect_events:
                        fn(self, kw.get('_branch', False))
        engine._connection_cls = Connection
    engine._oslo_engine_connect_events.append(listener)
