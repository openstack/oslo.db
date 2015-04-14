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
from sqlalchemy.engine import base as engine_base
from sqlalchemy.engine import Engine
from sqlalchemy import event
from sqlalchemy import exc as sqla_exc

from oslo_db.sqlalchemy.compat import utils


def handle_error(engine, listener):
    """Add a handle_error listener for the given :class:`.Engine`.

    This listener uses the SQLAlchemy
    :meth:`sqlalchemy.event.ConnectionEvents.handle_error`
    event.

    """
    if utils.sqla_100:
        event.listen(engine, "handle_error", listener)
        return

    assert isinstance(engine, Engine), \
        "engine argument must be an Engine instance, not a Connection"

    assert utils.sqla_097

    _rework_connect_and_revalidate_for_events(engine)

    # ctx.engine added per
    # https://bitbucket.org/zzzeek/sqlalchemy/issue/3266/
    def wrap_listener(ctx):
        if isinstance(ctx, engine_base.ExceptionContextImpl):
            ctx.engine = ctx.connection.engine
        return listener(ctx)
    event.listen(engine, "handle_error", wrap_listener)


def _rework_connect_and_revalidate_for_events(engine):
    """Patch the _revalidate_connection() system on Connection.

    This applies 1.0's _revalidate_connection() approach into an 0.9
    version of SQLAlchemy, and consists of three steps:

    1. wrap the pool._creator function, which in 0.9 has a local
    call to sqlalchemy.exc.DBAPIError.instance(), so that this exception is
    again unwrapped back to the original DBAPI-specific Error, then raise
    that.  This is essentially the same as if the dbapi.connect() isn't
    wrapped in the first place, which is how SQLAlchemy 1.0 now functions.

    2. patch the Engine object's raw_connection() method.  In SQLAlchemy 1.0,
    this is now where the error wrapping occurs when a pool connect attempt
    is made.  Here, when raw_connection() is called without a hosting
    Connection, we send exception raises to
    _handle_dbapi_exception_noconnection(), here copied from SQLAlchemy
    1.0, which is an alternate version of Connection._handle_dbapi_exception()
    tailored for an initial connect failure when there is no
    Connection object being dealt with.  This allows the error handler
    events to be called.

    3. patch the Connection class to follow 1.0's behavior for
    _revalidate_connection(); here, the call to engine.raw_connection()
    will pass the raised error to Connection._handle_dbapi_exception(),
    again allowing error handler events to be called.

    """

    _orig_connect = engine.pool._creator

    def connect():
        try:
            return _orig_connect()
        except sqla_exc.DBAPIError as err:
            original_exception = err.orig
            raise original_exception
    engine.pool._creator = connect

    self = engine

    def contextual_connect(close_with_result=False, **kwargs):
        return self._connection_cls(
            self,
            self._wrap_pool_connect(self.pool.connect, None),
            close_with_result=close_with_result,
            **kwargs)

    def _wrap_pool_connect(fn, connection):
        dialect = self.dialect
        try:
            return fn()
        except dialect.dbapi.Error as e:
            if connection is None:
                _handle_dbapi_exception_noconnection(
                    e, dialect, self)
            else:
                six.reraise(*sys.exc_info())

    def raw_connection(_connection=None):
        return self._wrap_pool_connect(
            self.pool.unique_connection, _connection)

    engine.contextual_connect = contextual_connect
    engine._wrap_pool_connect = _wrap_pool_connect
    engine.raw_connection = raw_connection

    class Connection(engine._connection_cls):

        @property
        def connection(self):
            "The underlying DB-API connection managed by this Connection."
            try:
                return self.__connection
            except AttributeError:
                try:
                    return self._revalidate_connection()
                except Exception as e:
                    self._handle_dbapi_exception(e, None, None, None, None)

        def _handle_dbapi_exception(self,
                                    e,
                                    statement,
                                    parameters,
                                    cursor,
                                    context):
            if self.invalidated:
                # 0.9's _handle_dbapi_exception() can't handle
                # a Connection that is invalidated already, meaning
                # its "__connection" attribute is not set.  So if we are
                # in that case, call our "no connection" invalidator.
                # this is fine as we are only supporting handle_error listeners
                # that are applied at the engine level.
                _handle_dbapi_exception_noconnection(
                    e, self.dialect, self.engine)
            else:
                super(Connection, self)._handle_dbapi_exception(
                    e, statement, parameters, cursor, context)

        def _revalidate_connection(self):
            if self._Connection__can_reconnect and self._Connection__invalid:
                if self._Connection__transaction is not None:
                    raise sqla_exc.InvalidRequestError(
                        "Can't reconnect until invalid "
                        "transaction is rolled back")
                self._Connection__connection = self.engine.raw_connection(
                    _connection=self)
                self._Connection__invalid = False
                return self._Connection__connection
            raise sqla_exc.ResourceClosedError("This Connection is closed")

    engine._connection_cls = Connection


def _handle_dbapi_exception_noconnection(e, dialect, engine):

    exc_info = sys.exc_info()

    is_disconnect = dialect.is_disconnect(e, None, None)

    should_wrap = isinstance(e, dialect.dbapi.Error)

    if should_wrap:
        sqlalchemy_exception = sqla_exc.DBAPIError.instance(
            None,
            None,
            e,
            dialect.dbapi.Error,
            connection_invalidated=is_disconnect)
    else:
        sqlalchemy_exception = None

    newraise = None

    ctx = ExceptionContextImpl(
        e, sqlalchemy_exception, engine, None, None, None,
        None, None, is_disconnect)

    if hasattr(engine, '_oslo_handle_error_events'):
        fns = engine._oslo_handle_error_events
    else:
        fns = engine.dispatch.handle_error
    for fn in fns:
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
            is_disconnect != ctx.is_disconnect:
        sqlalchemy_exception.connection_invalidated = \
            is_disconnect = ctx.is_disconnect

    if newraise:
        six.reraise(type(newraise), newraise, exc_info[2])
    elif should_wrap:
        six.reraise(
            type(sqlalchemy_exception), sqlalchemy_exception, exc_info[2])
    else:
        six.reraise(*exc_info)


class ExceptionContextImpl(object):
    """Encapsulate information about an error condition in progress.

    This is for forwards compatibility with the
    ExceptionContext interface introduced in SQLAlchemy 0.9.7.

    It also provides for the "engine" argument added in SQLAlchemy 1.0.0.

    """

    def __init__(self, exception, sqlalchemy_exception,
                 engine, connection, cursor, statement, parameters,
                 context, is_disconnect):
        self.engine = engine
        self.connection = connection
        self.sqlalchemy_exception = sqlalchemy_exception
        self.original_exception = exception
        self.execution_context = context
        self.statement = statement
        self.parameters = parameters
        self.is_disconnect = is_disconnect

    connection = None
    """The :class:`.Connection` in use during the exception.

    This member is present, except in the case of a failure when
    first connecting.


    """

    engine = None
    """The :class:`.Engine` in use during the exception.

    This member should always be present, even in the case of a failure
    when first connecting.

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
