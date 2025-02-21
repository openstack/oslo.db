# Copyright 2010 United States Government as represented by the
# Administrator of the National Aeronautics and Space Administration.
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
"""Core SQLAlchemy connectivity routines.
"""

import functools
import itertools
import logging
import os
import re
import time

import debtcollector.removals
import debtcollector.renames
import sqlalchemy
from sqlalchemy import event
from sqlalchemy import exc
from sqlalchemy import pool
from sqlalchemy import select

from oslo_db import exception

from oslo_db.sqlalchemy import compat
from oslo_db.sqlalchemy import exc_filters
from oslo_db.sqlalchemy import utils

LOG = logging.getLogger(__name__)


def _thread_yield(dbapi_con, con_record):
    """Ensure other greenthreads get a chance to be executed.

    If we use eventlet.monkey_patch(), eventlet.greenthread.sleep(0) will
    execute instead of time.sleep(0).
    Force a context switch. With common database backends (eg MySQLdb and
    sqlite), there is no implicit yield caused by network I/O since they are
    implemented by C libraries that eventlet cannot monkey patch.
    """
    time.sleep(0)


def _connect_ping_listener(connection, branch):
    """Ping the server at connection startup.

    Ping the server at transaction begin and transparently reconnect
    if a disconnect exception occurs.

    This listener is used up until SQLAlchemy 2.0.5.  At 2.0.5, we use the
    ``pool_pre_ping`` parameter instead of this event handler.

    Note the current test suite in test_exc_filters still **tests** this
    handler using all SQLAlchemy versions including 2.0.5 and greater.

    """
    if branch:
        return

    # turn off "close with result".  This can also be accomplished
    # by branching the connection, however just setting the flag is
    # more performant and also doesn't get involved with some
    # connection-invalidation awkardness that occurs (see
    # https://bitbucket.org/zzzeek/sqlalchemy/issue/3215/)
    save_should_close_with_result = connection.should_close_with_result
    connection.should_close_with_result = False
    try:
        # run a SELECT 1.   use a core select() so that
        # any details like that needed by the backend are handled.
        connection.scalar(select(1))
    except exception.DBConnectionError:
        # catch DBConnectionError, which is raised by the filter
        # system.
        # disconnect detected.  The connection is now
        # "invalid", but the pool should be ready to return
        # new connections assuming they are good now.
        # run the select again to re-validate the Connection.
        LOG.exception(
            "Database connection was found disconnected; reconnecting"
        )
        # TODO(ralonsoh): drop this attr check once SQLAlchemy minimum version
        # is 2.0.
        if hasattr(connection, "rollback"):
            connection.rollback()
        connection.scalar(select(1))
    finally:
        connection.should_close_with_result = save_should_close_with_result
        # TODO(ralonsoh): drop this attr check once SQLAlchemy minimum version
        # is 2.0.
        if hasattr(connection, "rollback"):
            connection.rollback()


# SQLAlchemy 2.0 is compatible here, however oslo.db's test suite
# raises for all deprecation errors, so we have to check for 2.0
# and wrap out a parameter that is deprecated
if compat.sqla_2:
    _connect_ping_listener = functools.partial(
        _connect_ping_listener, branch=False
    )


def _setup_logging(connection_debug=0):
    """setup_logging function maps SQL debug level to Python log level.

    Connection_debug is a verbosity of SQL debugging information.
    0=None(default value),
    1=Processed only messages with WARNING level or higher
    50=Processed only messages with INFO level or higher
    100=Processed only messages with DEBUG level
    """
    if connection_debug >= 0:
        logger = logging.getLogger("sqlalchemy.engine")
        if connection_debug == 100:
            logger.setLevel(logging.DEBUG)
        elif connection_debug >= 50:
            logger.setLevel(logging.INFO)
        else:
            logger.setLevel(logging.WARNING)


def _vet_url(url):
    if "+" not in url.drivername and not url.drivername.startswith("sqlite"):
        if url.drivername.startswith("mysql"):
            LOG.warning(
                "URL %r does not contain a '+drivername' portion, "
                "and will make use of a default driver.  "
                "A full dbname+drivername:// protocol is recommended. "
                "For MySQL, it is strongly recommended that mysql+pymysql:// "
                "be specified for maximum service compatibility",
                url,
            )
        else:
            LOG.warning(
                "URL %r does not contain a '+drivername' portion, "
                "and will make use of a default driver.  "
                "A full dbname+drivername:// protocol is recommended.",
                url,
            )


def _create_engine(url, **engine_args):
    engine = sqlalchemy.create_engine(url, **engine_args)
    return engine, engine


def _test_connection(engine, max_retries, retry_interval, _close=True):
    if max_retries == -1:
        attempts = itertools.count()
    else:
        attempts = range(max_retries)
    # See: http://legacy.python.org/dev/peps/pep-3110/#semantic-changes for
    # why we are not using 'de' directly (it can be removed from the local
    # scope).
    de_ref = conn = None
    for attempt in attempts:
        try:
            conn = engine.connect()
        except exception.DBConnectionError as de:
            msg = "SQL connection failed. %s attempts left."
            LOG.warning(msg, max_retries - attempt)
            time.sleep(retry_interval)
            de_ref = de
        else:
            if _close:
                conn.close()
            break
    else:
        if de_ref is not None:
            raise de_ref

    return conn


@debtcollector.renames.renamed_kwarg(
    "idle_timeout",
    "connection_recycle_time",
    replace=True,
)
def create_engine(
    sql_connection,
    sqlite_fk=False,
    mysql_sql_mode=None,
    mysql_wsrep_sync_wait=None,
    connection_recycle_time=3600,
    connection_debug=0,
    max_pool_size=None,
    max_overflow=None,
    pool_timeout=None,
    sqlite_synchronous=True,
    connection_trace=False,
    max_retries=10,
    retry_interval=10,
    thread_checkin=True,
    logging_name=None,
    json_serializer=None,
    json_deserializer=None,
    connection_parameters=None,
    _engine_target=_create_engine,
    _test_connection=_test_connection,
):
    """Return a new SQLAlchemy engine."""

    url = utils.make_url(sql_connection)

    if connection_parameters:
        url = url.update_query_string(connection_parameters, append=True)

    _vet_url(url)

    _native_pre_ping = compat.native_pre_ping_event_support

    engine_args = {
        "pool_recycle": connection_recycle_time,
        "pool_pre_ping": _native_pre_ping,
        "connect_args": {},
        "logging_name": logging_name,
    }

    _setup_logging(connection_debug)

    _init_connection_args(
        url,
        engine_args,
        dict(
            max_pool_size=max_pool_size,
            max_overflow=max_overflow,
            pool_timeout=pool_timeout,
            json_serializer=json_serializer,
            json_deserializer=json_deserializer,
        ),
    )

    engine, engine_event_target = _engine_target(url, **engine_args)

    _init_events(
        engine_event_target,
        mysql_sql_mode=mysql_sql_mode,
        mysql_wsrep_sync_wait=mysql_wsrep_sync_wait,
        sqlite_synchronous=sqlite_synchronous,
        sqlite_fk=sqlite_fk,
        thread_checkin=thread_checkin,
        connection_trace=connection_trace,
    )

    # register alternate exception handler
    exc_filters.register_engine(engine_event_target)

    if not _native_pre_ping:
        # register engine connect handler.

        event.listen(
            engine_event_target, "engine_connect", _connect_ping_listener
        )

    # initial connect + test
    # NOTE(viktors): the current implementation of _test_connection()
    #                does nothing, if max_retries == 0, so we can skip it
    if max_retries:
        _test_connection(engine_event_target, max_retries, retry_interval)

    return engine


@utils.dispatch_for_dialect("*", multiple=True)
def _init_connection_args(url, engine_args, kw):

    # (zzzeek) kw is passed by reference rather than as **kw so that the
    # init_connection_args routines can modify the contents of what
    # will be passed to create_engine, including removing arguments that
    # don't apply.  This allows things such as replacing QueuePool with
    # NUllPool, for example, as the latter pool would reject these parameters.
    max_pool_size = kw.get("max_pool_size", None)
    max_overflow = kw.get("max_overflow", None)
    pool_timeout = kw.get("pool_timeout", None)

    pool_class = url.get_dialect().get_pool_class(url)
    if issubclass(pool_class, pool.QueuePool):
        if max_pool_size is not None:
            engine_args["pool_size"] = max_pool_size
        if max_overflow is not None:
            engine_args["max_overflow"] = max_overflow
        if pool_timeout is not None:
            engine_args["pool_timeout"] = pool_timeout


@_init_connection_args.dispatch_for("sqlite")
def _init_connection_args(url, engine_args, kw):
    pool_class = url.get_dialect().get_pool_class(url)
    if issubclass(pool_class, pool.SingletonThreadPool):
        # singletonthreadpool is used for :memory: connections;
        # replace it with StaticPool.
        engine_args["poolclass"] = pool.StaticPool
        engine_args["connect_args"]["check_same_thread"] = False
        # NOTE(amorin): see https://github.com/python/cpython/issues/118172
        engine_args["connect_args"]["cached_statements"] = 0
    elif issubclass(pool_class, pool.QueuePool):
        # SQLAlchemy 2.0 uses QueuePool for sqlite file DBs; put NullPool
        # back to avoid compatibility issues
        kw.pop("max_pool_size", None)
        kw.pop("max_overflow", None)
        engine_args.pop("max_pool_size", None)
        engine_args.pop("max_overflow", None)
        engine_args["poolclass"] = pool.NullPool


@_init_connection_args.dispatch_for("postgresql")
def _init_connection_args(url, engine_args, kw):
    if "client_encoding" not in url.query:
        # Set encoding using engine_args instead of connect_args since
        # it's supported for PostgreSQL 8.*. More details at:
        # http://docs.sqlalchemy.org/en/rel_0_9/dialects/postgresql.html
        engine_args["client_encoding"] = "utf8"
    engine_args["json_serializer"] = kw.get("json_serializer")
    engine_args["json_deserializer"] = kw.get("json_deserializer")


@_init_connection_args.dispatch_for("mysql")
def _init_connection_args(url, engine_args, kw):
    if "charset" not in url.query:
        engine_args["connect_args"]["charset"] = "utf8"


@_init_connection_args.dispatch_for("mysql+mysqlconnector")
def _init_connection_args(url, engine_args, kw):
    # mysqlconnector engine (<1.0) incorrectly defaults to
    # raise_on_warnings=True
    #  https://bitbucket.org/zzzeek/sqlalchemy/issue/2515
    if "raise_on_warnings" not in url.query:
        engine_args["connect_args"]["raise_on_warnings"] = False


@_init_connection_args.dispatch_for("mysql+mysqldb")
def _init_connection_args(url, engine_args, kw):
    # Those drivers require use_unicode=0 to avoid performance drop due
    # to internal usage of Python unicode objects in the driver
    #  http://docs.sqlalchemy.org/en/rel_0_9/dialects/mysql.html
    if "use_unicode" not in url.query:
        engine_args["connect_args"]["use_unicode"] = 1


@utils.dispatch_for_dialect("*", multiple=True)
def _init_events(engine, thread_checkin=True, connection_trace=False, **kw):
    """Set up event listeners for all database backends."""

    _add_process_guards(engine)

    if connection_trace:
        _add_trace_comments(engine)

    if thread_checkin:
        sqlalchemy.event.listen(engine, "checkin", _thread_yield)


@_init_events.dispatch_for("mysql")
def _init_events(
    engine, mysql_sql_mode=None, mysql_wsrep_sync_wait=None, **kw
):
    """Set up event listeners for MySQL."""

    if mysql_sql_mode is not None or mysql_wsrep_sync_wait is not None:

        @sqlalchemy.event.listens_for(engine, "connect")
        def _set_session_variables(dbapi_con, connection_rec):
            cursor = dbapi_con.cursor()
            if mysql_sql_mode is not None:
                cursor.execute("SET SESSION sql_mode = %s", [mysql_sql_mode])
            if mysql_wsrep_sync_wait is not None:
                cursor.execute(
                    "SET SESSION wsrep_sync_wait = %s", [mysql_wsrep_sync_wait]
                )

    @sqlalchemy.event.listens_for(engine, "first_connect")
    def _check_effective_sql_mode(dbapi_con, connection_rec):
        if mysql_sql_mode is not None or mysql_wsrep_sync_wait is not None:
            _set_session_variables(dbapi_con, connection_rec)

        cursor = dbapi_con.cursor()
        cursor.execute("SHOW VARIABLES LIKE 'sql_mode'")
        realmode = cursor.fetchone()

        if realmode is None:
            LOG.warning("Unable to detect effective SQL mode")
        else:
            realmode = realmode[1]
            LOG.debug("MySQL server mode set to %s", realmode)
            if (
                "TRADITIONAL" not in realmode.upper() and
                "STRICT_ALL_TABLES" not in realmode.upper()
            ):
                LOG.warning(
                    "MySQL SQL mode is '%s', "
                    "consider enabling TRADITIONAL or STRICT_ALL_TABLES",
                    realmode,
                )


@_init_events.dispatch_for("sqlite")
def _init_events(engine, sqlite_synchronous=True, sqlite_fk=False, **kw):
    """Set up event listeners for SQLite.

    This includes several settings made on connections as they are
    created, as well as transactional control extensions.

    """

    def regexp(expr, item):
        reg = re.compile(expr)
        return reg.search(str(item)) is not None

    @sqlalchemy.event.listens_for(engine, "connect")
    def _sqlite_connect_events(dbapi_con, con_record):

        # Add REGEXP functionality on SQLite connections
        dbapi_con.create_function("regexp", 2, regexp)

        if not sqlite_synchronous:
            # Switch sqlite connections to non-synchronous mode
            dbapi_con.execute("PRAGMA synchronous = OFF")

        # Disable pysqlite's emitting of the BEGIN statement entirely.
        # Also stops it from emitting COMMIT before any DDL.
        # below, we emit BEGIN ourselves.
        # see http://docs.sqlalchemy.org/en/rel_0_9/dialects/\
        # sqlite.html#serializable-isolation-savepoints-transactional-ddl
        dbapi_con.isolation_level = None

        if sqlite_fk:
            # Ensures that the foreign key constraints are enforced in SQLite.
            dbapi_con.execute("pragma foreign_keys=ON")

    @sqlalchemy.event.listens_for(engine, "begin")
    def _sqlite_emit_begin(conn):
        # emit our own BEGIN, checking for existing
        # transactional state
        if "in_transaction" not in conn.info:
            conn.execute(sqlalchemy.text("BEGIN"))
            conn.info["in_transaction"] = True

    @sqlalchemy.event.listens_for(engine, "rollback")
    @sqlalchemy.event.listens_for(engine, "commit")
    def _sqlite_end_transaction(conn):
        # remove transactional marker
        conn.info.pop("in_transaction", None)


def _add_process_guards(engine):
    """Add multiprocessing guards.

    Forces a connection to be reconnected if it is detected
    as having been shared to a sub-process.

    """

    @sqlalchemy.event.listens_for(engine, "connect")
    def connect(dbapi_connection, connection_record):
        connection_record.info["pid"] = os.getpid()

    @sqlalchemy.event.listens_for(engine, "checkout")
    def checkout(dbapi_connection, connection_record, connection_proxy):
        pid = os.getpid()
        if connection_record.info["pid"] != pid:
            LOG.debug(
                "Parent process %(orig)s forked (%(newproc)s) with an open "
                "database connection, "
                "which is being discarded and recreated.",
                {"newproc": pid, "orig": connection_record.info["pid"]},
            )
            raise exc.DisconnectionError(
                "Connection record belongs to pid %s, "
                "attempting to check out in pid %s"
                % (connection_record.info["pid"], pid)
            )


def _add_trace_comments(engine):
    """Add trace comments.

    Augment statements with a trace of the immediate calling code
    for a given statement.
    """

    import os
    import sys
    import traceback

    target_paths = {
        os.path.dirname(sys.modules["oslo_db"].__file__),
        os.path.dirname(sys.modules["sqlalchemy"].__file__),
    }
    try:
        skip_paths = {
            os.path.dirname(sys.modules["oslo_db.tests"].__file__),
        }
    except KeyError:
        skip_paths = set()

    @sqlalchemy.event.listens_for(engine, "before_cursor_execute", retval=True)
    def before_cursor_execute(
        conn, cursor, statement, parameters, context, executemany
    ):

        # NOTE(zzzeek) - if different steps per DB dialect are desirable
        # here, switch out on engine.name for now.
        stack = traceback.extract_stack()
        our_line = None

        for idx, (filename, line, method, function) in enumerate(stack):
            for tgt in skip_paths:
                if filename.startswith(tgt):
                    break
            else:
                for tgt in target_paths:
                    if filename.startswith(tgt):
                        our_line = idx
                        break
            if our_line:
                break

        if our_line:
            trace = "; ".join(
                "File: {} ({}) {}".format(line[0], line[1], line[2])
                # include three lines of context.
                for line in stack[our_line - 3: our_line]
            )
            statement = "{}  -- {}".format(statement, trace)

        return statement, parameters
