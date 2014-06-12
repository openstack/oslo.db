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
"""Provide forwards compatibility for the handle_error event.

See the "handle_error" event at
http://docs.sqlalchemy.org/en/rel_0_9/core/events.html.


"""
import sys

import six
from sqlalchemy.engine import Engine
from sqlalchemy import event
from sqlalchemy import exc as sqla_exc

from oslo.db.sqlalchemy.compat import utils


def handle_error(engine, listener):
    """Add a handle_error listener for the given :class:`.Engine`.

    This listener uses the SQLAlchemy
    :meth:`sqlalchemy.event.ConnectionEvents.handle_error`
    event, however augments the listener for pre-0.9.7 versions of SQLAlchemy
    in order to support safe re-raise of the exception.

    """

    if utils.sqla_097:
        event.listen(engine, "handle_error", listener)
        return

    assert isinstance(engine, Engine), \
        "engine argument must be an Engine instance, not a Connection"

    # use a Connection-wrapper class to wrap _handle_dbapi_exception.
    if not getattr(engine._connection_cls,
                   '_oslo_handle_error_wrapper', False):
        engine._oslo_handle_error_events = []

        class Connection(engine._connection_cls):
            _oslo_handle_error_wrapper = True

            def _handle_dbapi_exception(self, e, statement, parameters,
                                        cursor, context):

                try:
                    super(Connection, self)._handle_dbapi_exception(
                        e, statement, parameters, cursor, context)
                except Exception as reraised_exception:
                    # all versions:
                    #   _handle_dbapi_exception reraises all DBAPI errors
                    # 0.8 and above:
                    #   reraises all errors unconditionally
                    pass
                else:
                    # 0.7.8:
                    #   _handle_dbapi_exception does not unconditionally
                    #   re-raise
                    reraised_exception = e

                _oslo_handle_error_events = getattr(
                    self.engine,
                    '_oslo_handle_error_events',
                    False)

                newraise = None
                if _oslo_handle_error_events:
                    if isinstance(reraised_exception,
                                  sqla_exc.StatementError):
                        sqlalchemy_exception = reraised_exception
                        original_exception = sqlalchemy_exception.orig
                        self._is_disconnect = is_disconnect = (
                            isinstance(sqlalchemy_exception,
                                       sqla_exc.DBAPIError)
                            and sqlalchemy_exception.connection_invalidated)
                    else:
                        sqlalchemy_exception = None
                        original_exception = reraised_exception
                        is_disconnect = False

                    # new handle_error event
                    ctx = ExceptionContextImpl(
                        original_exception, sqlalchemy_exception,
                        self, cursor, statement,
                        parameters, context, is_disconnect)

                    for fn in _oslo_handle_error_events:
                        try:
                            # handler returns an exception;
                            # call next handler in a chain
                            per_fn = fn(ctx)
                            if per_fn is not None:
                                ctx.chained_exception = newraise = per_fn
                        except Exception as _raised:
                            # handler raises an exception - stop processing
                            newraise = _raised
                            break

                    if sqlalchemy_exception and \
                            self._is_disconnect != ctx.is_disconnect:

                        if not ctx.is_disconnect:
                            raise NotImplementedError(
                                "Can't reset 'disconnect' status of exception "
                                "once it is set with this version of "
                                "SQLAlchemy")

                        sqlalchemy_exception.connection_invalidated = \
                            self._is_disconnect = ctx.is_disconnect
                        if self._is_disconnect:
                            self._do_disconnect(e)

                if newraise:
                    six.reraise(type(newraise), newraise, sys.exc_info()[2])
                else:
                    six.reraise(type(reraised_exception),
                                reraised_exception, sys.exc_info()[2])

            def _do_disconnect(self, e):
                del self._is_disconnect
                if utils.sqla_094:
                    dbapi_conn_wrapper = self.connection
                    self.engine.pool._invalidate(dbapi_conn_wrapper, e)
                    self.invalidate(e)
                else:
                    dbapi_conn_wrapper = self.connection
                    self.invalidate(e)
                    if not hasattr(dbapi_conn_wrapper, '_pool') or \
                            dbapi_conn_wrapper._pool is self.engine.pool:
                        self.engine.dispose()

        engine._connection_cls = Connection
    engine._oslo_handle_error_events.append(listener)


class ExceptionContextImpl(object):
    """Encapsulate information about an error condition in progress.

    This is for forwards compatibility with the
    ExceptionContext interface introduced in SQLAlchemy 0.9.7.

    """

    def __init__(self, exception, sqlalchemy_exception,
                 connection, cursor, statement, parameters,
                 context, is_disconnect):
        self.connection = connection
        self.sqlalchemy_exception = sqlalchemy_exception
        self.original_exception = exception
        self.execution_context = context
        self.statement = statement
        self.parameters = parameters
        self.is_disconnect = is_disconnect

    connection = None
    """The :class:`.Connection` in use during the exception.

    This member is always present.

    """

    cursor = None
    """The DBAPI cursor object.

    May be None.

    """

    statement = None
    """String SQL statement that was emitted directly to the DBAPI.

    May be None.

    """

    parameters = None
    """Parameter collection that was emitted directly to the DBAPI.

    May be None.

    """

    original_exception = None
    """The exception object which was caught.

    This member is always present.

    """

    sqlalchemy_exception = None
    """The :class:`sqlalchemy.exc.StatementError` which wraps the original,
    and will be raised if exception handling is not circumvented by the event.

    May be None, as not all exception types are wrapped by SQLAlchemy.
    For DBAPI-level exceptions that subclass the dbapi's Error class, this
    field will always be present.

    """

    chained_exception = None
    """The exception that was returned by the previous handler in the
    exception chain, if any.

    If present, this exception will be the one ultimately raised by
    SQLAlchemy unless a subsequent handler replaces it.

    May be None.

    """

    execution_context = None
    """The :class:`.ExecutionContext` corresponding to the execution
    operation in progress.

    This is present for statement execution operations, but not for
    operations such as transaction begin/end.  It also is not present when
    the exception was raised before the :class:`.ExecutionContext`
    could be constructed.

    Note that the :attr:`.ExceptionContext.statement` and
    :attr:`.ExceptionContext.parameters` members may represent a
    different value than that of the :class:`.ExecutionContext`,
    potentially in the case where a
    :meth:`.ConnectionEvents.before_cursor_execute` event or similar
    modified the statement/parameters to be sent.

    May be None.

    """

    is_disconnect = None
    """Represent whether the exception as occurred represents a "disconnect"
    condition.

    This flag will always be True or False within the scope of the
    :meth:`.ConnectionEvents.handle_error` handler.

    """
