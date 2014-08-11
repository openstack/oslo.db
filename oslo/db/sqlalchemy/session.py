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

"""Session Handling for SQLAlchemy backend.

Recommended ways to use sessions within this framework:

* Don't use them explicitly; this is like running with ``AUTOCOMMIT=1``.
  `model_query()` will implicitly use a session when called without one
  supplied. This is the ideal situation because it will allow queries
  to be automatically retried if the database connection is interrupted.

  .. note:: Automatic retry will be enabled in a future patch.

  It is generally fine to issue several queries in a row like this. Even though
  they may be run in separate transactions and/or separate sessions, each one
  will see the data from the prior calls. If needed, undo- or rollback-like
  functionality should be handled at a logical level. For an example, look at
  the code around quotas and `reservation_rollback()`.

  Examples:

  .. code-block:: python

    def get_foo(context, foo):
        return (model_query(context, models.Foo).
                filter_by(foo=foo).
                first())

    def update_foo(context, id, newfoo):
        (model_query(context, models.Foo).
                filter_by(id=id).
                update({'foo': newfoo}))

    def create_foo(context, values):
        foo_ref = models.Foo()
        foo_ref.update(values)
        foo_ref.save()
        return foo_ref


* Within the scope of a single method, keep all the reads and writes within
  the context managed by a single session. In this way, the session's
  `__exit__` handler will take care of calling `flush()` and `commit()` for
  you. If using this approach, you should not explicitly call `flush()` or
  `commit()`. Any error within the context of the session will cause the
  session to emit a `ROLLBACK`. Database errors like `IntegrityError` will be
  raised in `session`'s `__exit__` handler, and any try/except within the
  context managed by `session` will not be triggered. And catching other
  non-database errors in the session will not trigger the ROLLBACK, so
  exception handlers should  always be outside the session, unless the
  developer wants to do a partial commit on purpose. If the connection is
  dropped before this is possible, the database will implicitly roll back the
  transaction.

  .. note:: Statements in the session scope will not be automatically retried.

  If you create models within the session, they need to be added, but you
  do not need to call `model.save()`:

  .. code-block:: python

    def create_many_foo(context, foos):
        session = sessionmaker()
        with session.begin():
            for foo in foos:
                foo_ref = models.Foo()
                foo_ref.update(foo)
                session.add(foo_ref)

    def update_bar(context, foo_id, newbar):
        session = sessionmaker()
        with session.begin():
            foo_ref = (model_query(context, models.Foo, session).
                        filter_by(id=foo_id).
                        first())
            (model_query(context, models.Bar, session).
                        filter_by(id=foo_ref['bar_id']).
                        update({'bar': newbar}))

  .. note:: `update_bar` is a trivially simple example of using
     ``with session.begin``. Whereas `create_many_foo` is a good example of
     when a transaction is needed, it is always best to use as few queries as
     possible.

  The two queries in `update_bar` can be better expressed using a single query
  which avoids the need for an explicit transaction. It can be expressed like
  so:

  .. code-block:: python

    def update_bar(context, foo_id, newbar):
        subq = (model_query(context, models.Foo.id).
                filter_by(id=foo_id).
                limit(1).
                subquery())
        (model_query(context, models.Bar).
                filter_by(id=subq.as_scalar()).
                update({'bar': newbar}))

  For reference, this emits approximately the following SQL statement:

  .. code-block:: sql

    UPDATE bar SET bar = ${newbar}
        WHERE id=(SELECT bar_id FROM foo WHERE id = ${foo_id} LIMIT 1);

  .. note:: `create_duplicate_foo` is a trivially simple example of catching an
     exception while using ``with session.begin``. Here create two duplicate
     instances with same primary key, must catch the exception out of context
     managed by a single session:

  .. code-block:: python

    def create_duplicate_foo(context):
        foo1 = models.Foo()
        foo2 = models.Foo()
        foo1.id = foo2.id = 1
        session = sessionmaker()
        try:
            with session.begin():
                session.add(foo1)
                session.add(foo2)
        except exception.DBDuplicateEntry as e:
            handle_error(e)

* Passing an active session between methods. Sessions should only be passed
  to private methods. The private method must use a subtransaction; otherwise
  SQLAlchemy will throw an error when you call `session.begin()` on an existing
  transaction. Public methods should not accept a session parameter and should
  not be involved in sessions within the caller's scope.

  Note that this incurs more overhead in SQLAlchemy than the above means
  due to nesting transactions, and it is not possible to implicitly retry
  failed database operations when using this approach.

  This also makes code somewhat more difficult to read and debug, because a
  single database transaction spans more than one method. Error handling
  becomes less clear in this situation. When this is needed for code clarity,
  it should be clearly documented.

  .. code-block:: python

    def myfunc(foo):
        session = sessionmaker()
        with session.begin():
            # do some database things
            bar = _private_func(foo, session)
        return bar

    def _private_func(foo, session=None):
        if not session:
            session = sessionmaker()
        with session.begin(subtransaction=True):
            # do some other database things
        return bar


There are some things which it is best to avoid:

* Don't keep a transaction open any longer than necessary.

  This means that your ``with session.begin()`` block should be as short
  as possible, while still containing all the related calls for that
  transaction.

* Avoid ``with_lockmode('UPDATE')`` when possible.

  In MySQL/InnoDB, when a ``SELECT ... FOR UPDATE`` query does not match
  any rows, it will take a gap-lock. This is a form of write-lock on the
  "gap" where no rows exist, and prevents any other writes to that space.
  This can effectively prevent any INSERT into a table by locking the gap
  at the end of the index. Similar problems will occur if the SELECT FOR UPDATE
  has an overly broad WHERE clause, or doesn't properly use an index.

  One idea proposed at ODS Fall '12 was to use a normal SELECT to test the
  number of rows matching a query, and if only one row is returned,
  then issue the SELECT FOR UPDATE.

  The better long-term solution is to use
  ``INSERT .. ON DUPLICATE KEY UPDATE``.
  However, this can not be done until the "deleted" columns are removed and
  proper UNIQUE constraints are added to the tables.


Enabling soft deletes:

* To use/enable soft-deletes, the `SoftDeleteMixin` must be added
  to your model class. For example:

  .. code-block:: python

      class NovaBase(models.SoftDeleteMixin, models.ModelBase):
          pass


Efficient use of soft deletes:

* There are two possible ways to mark a record as deleted:
  `model.soft_delete()` and `query.soft_delete()`.

  The `model.soft_delete()` method works with a single already-fetched entry.
  `query.soft_delete()` makes only one db request for all entries that
  correspond to the query.

* In almost all cases you should use `query.soft_delete()`. Some examples:

  .. code-block:: python

        def soft_delete_bar():
            count = model_query(BarModel).find(some_condition).soft_delete()
            if count == 0:
                raise Exception("0 entries were soft deleted")

        def complex_soft_delete_with_synchronization_bar(session=None):
            if session is None:
                session = sessionmaker()
            with session.begin(subtransactions=True):
                count = (model_query(BarModel).
                            find(some_condition).
                            soft_delete(synchronize_session=True))
                            # Here synchronize_session is required, because we
                            # don't know what is going on in outer session.
                if count == 0:
                    raise Exception("0 entries were soft deleted")

* There is only one situation where `model.soft_delete()` is appropriate: when
  you fetch a single record, work with it, and mark it as deleted in the same
  transaction.

  .. code-block:: python

        def soft_delete_bar_model():
            session = sessionmaker()
            with session.begin():
                bar_ref = model_query(BarModel).find(some_condition).first()
                # Work with bar_ref
                bar_ref.soft_delete(session=session)

  However, if you need to work with all entries that correspond to query and
  then soft delete them you should use the `query.soft_delete()` method:

  .. code-block:: python

        def soft_delete_multi_models():
            session = sessionmaker()
            with session.begin():
                query = (model_query(BarModel, session=session).
                            find(some_condition))
                model_refs = query.all()
                # Work with model_refs
                query.soft_delete(synchronize_session=False)
                # synchronize_session=False should be set if there is no outer
                # session and these entries are not used after this.

  When working with many rows, it is very important to use query.soft_delete,
  which issues a single query. Using `model.soft_delete()`, as in the following
  example, is very inefficient.

  .. code-block:: python

        for bar_ref in bar_refs:
            bar_ref.soft_delete(session=session)
        # This will produce count(bar_refs) db requests.

"""

import functools
import itertools
import logging
import re
import time

from oslo.utils import timeutils
import six
from sqlalchemy.interfaces import PoolListener
import sqlalchemy.orm
from sqlalchemy.pool import NullPool, StaticPool
from sqlalchemy.sql.expression import literal_column
from sqlalchemy.sql.expression import select

from oslo.db import exception
from oslo.db.openstack.common.gettextutils import _LW
from oslo.db import options
from oslo.db.sqlalchemy import exc_filters

LOG = logging.getLogger(__name__)


class SqliteForeignKeysListener(PoolListener):
    """Ensures that the foreign key constraints are enforced in SQLite.

    The foreign key constraints are disabled by default in SQLite,
    so the foreign key constraints will be enabled here for every
    database connection
    """
    def connect(self, dbapi_con, con_record):
        dbapi_con.execute('pragma foreign_keys=ON')


def _synchronous_switch_listener(dbapi_conn, connection_rec):
    """Switch sqlite connections to non-synchronous mode."""
    dbapi_conn.execute("PRAGMA synchronous = OFF")


def _add_regexp_listener(dbapi_con, con_record):
    """Add REGEXP function to sqlite connections."""

    def regexp(expr, item):
        reg = re.compile(expr)
        return reg.search(six.text_type(item)) is not None
    dbapi_con.create_function('regexp', 2, regexp)


def _thread_yield(dbapi_con, con_record):
    """Ensure other greenthreads get a chance to be executed.

    If we use eventlet.monkey_patch(), eventlet.greenthread.sleep(0) will
    execute instead of time.sleep(0).
    Force a context switch. With common database backends (eg MySQLdb and
    sqlite), there is no implicit yield caused by network I/O since they are
    implemented by C libraries that eventlet cannot monkey patch.
    """
    time.sleep(0)


def _begin_ping_listener(connection):
    """Ping the server at transaction begin.

    Ping the server at transaction begin and transparently reconnect
    if a disconnect exception occurs.
    """
    try:
        # run a SELECT 1.   use a core select() so that
        # any details like that needed by Oracle, DB2 etc. are handled.
        connection.scalar(select([1]))
    except exception.DBConnectionError:
        # catch DBConnectionError, which is raised by the filter
        # system.
        # disconnect detected.  The connection is now
        # "invalid", but the pool should be ready to return
        # new connections assuming they are good now.
        # run the select again to re-validate the Connection.
        connection.scalar(select([1]))


def _set_session_sql_mode(dbapi_con, connection_rec, sql_mode=None):
    """Set the sql_mode session variable.

    MySQL supports several server modes. The default is None, but sessions
    may choose to enable server modes like TRADITIONAL, ANSI,
    several STRICT_* modes and others.

    Note: passing in '' (empty string) for sql_mode clears
    the SQL mode for the session, overriding a potentially set
    server default.
    """

    cursor = dbapi_con.cursor()
    cursor.execute("SET SESSION sql_mode = %s", [sql_mode])


def _mysql_get_effective_sql_mode(engine):
    """Returns the effective SQL mode for connections from the engine pool.

    Returns ``None`` if the mode isn't available, otherwise returns the mode.

    """
    # Get the real effective SQL mode. Even when unset by
    # our own config, the server may still be operating in a specific
    # SQL mode as set by the server configuration.
    # Also note that the checkout listener will be called on execute to
    # set the mode if it's registered.
    row = engine.execute("SHOW VARIABLES LIKE 'sql_mode'").fetchone()
    if row is None:
        return
    return row[1]


def _mysql_check_effective_sql_mode(engine):
    """Logs a message based on the effective SQL mode for MySQL connections."""
    realmode = _mysql_get_effective_sql_mode(engine)

    if realmode is None:
        LOG.warning(_LW('Unable to detect effective SQL mode'))
        return

    LOG.debug('MySQL server mode set to %s', realmode)
    # 'TRADITIONAL' mode enables several other modes, so
    # we need a substring match here
    if not ('TRADITIONAL' in realmode.upper() or
            'STRICT_ALL_TABLES' in realmode.upper()):
        LOG.warning(_LW("MySQL SQL mode is '%s', "
                        "consider enabling TRADITIONAL or STRICT_ALL_TABLES"),
                    realmode)


def _mysql_set_mode_callback(engine, sql_mode):
    if sql_mode is not None:
        mode_callback = functools.partial(_set_session_sql_mode,
                                          sql_mode=sql_mode)
        sqlalchemy.event.listen(engine, 'connect', mode_callback)
    _mysql_check_effective_sql_mode(engine)


def create_engine(sql_connection, sqlite_fk=False, mysql_sql_mode=None,
                  idle_timeout=3600,
                  connection_debug=0, max_pool_size=None, max_overflow=None,
                  pool_timeout=None, sqlite_synchronous=True,
                  connection_trace=False, max_retries=10, retry_interval=10,
                  thread_checkin=True):
    """Return a new SQLAlchemy engine."""

    connection_dict = sqlalchemy.engine.url.make_url(sql_connection)

    engine_args = {
        "pool_recycle": idle_timeout,
        'convert_unicode': True,
    }

    if connection_debug >= 0:
        # Map SQL debug level to Python log level
        logger = logging.getLogger('sqlalchemy.engine')
        if connection_debug >= 100:
            logger.setLevel(logging.DEBUG)
        elif connection_debug >= 50:
            logger.setLevel(logging.INFO)
        else:
            logger.setLevel(logging.WARNING)

    if "sqlite" in connection_dict.drivername:
        if sqlite_fk:
            engine_args["listeners"] = [SqliteForeignKeysListener()]
        engine_args["poolclass"] = NullPool

        if sql_connection == "sqlite://":
            engine_args["poolclass"] = StaticPool
            engine_args["connect_args"] = {'check_same_thread': False}
    else:
        if max_pool_size is not None:
            engine_args['pool_size'] = max_pool_size
        if max_overflow is not None:
            engine_args['max_overflow'] = max_overflow
        if pool_timeout is not None:
            engine_args['pool_timeout'] = pool_timeout
        if connection_dict.get_dialect().driver == 'mysqlconnector':
            # mysqlconnector engine (<1.0) incorrectly defaults to
            # raise_on_warnings=True
            #  https://bitbucket.org/zzzeek/sqlalchemy/issue/2515
            engine_args['connect_args'] = {'raise_on_warnings': False}

    engine = sqlalchemy.create_engine(sql_connection, **engine_args)

    if thread_checkin:
        sqlalchemy.event.listen(engine, 'checkin', _thread_yield)

    if engine.name == 'mysql':
        if mysql_sql_mode is not None:
            _mysql_set_mode_callback(engine, mysql_sql_mode)
    elif 'sqlite' in connection_dict.drivername:
        if not sqlite_synchronous:
            sqlalchemy.event.listen(engine, 'connect',
                                    _synchronous_switch_listener)
        sqlalchemy.event.listen(engine, 'connect', _add_regexp_listener)

    if connection_trace:
        _add_trace_comments(engine)

    # register alternate exception handler
    exc_filters.register_engine(engine)

    # register on begin handler
    sqlalchemy.event.listen(engine, "begin", _begin_ping_listener)

    # initial connect + test
    _test_connection(engine, max_retries, retry_interval)

    return engine


def _test_connection(engine, max_retries, retry_interval):
    if max_retries == -1:
        attempts = itertools.count()
    else:
        attempts = six.moves.range(max_retries)
    de = None
    for attempt in attempts:
        try:
            return exc_filters.handle_connect_error(engine)
        except exception.DBConnectionError as de:
            msg = _LW('SQL connection failed. %s attempts left.')
            LOG.warning(msg, max_retries - attempt)
            time.sleep(retry_interval)
    else:
        if de is not None:
            six.reraise(type(de), de)


class Query(sqlalchemy.orm.query.Query):
    """Subclass of sqlalchemy.query with soft_delete() method."""
    def soft_delete(self, synchronize_session='evaluate'):
        return self.update({'deleted': literal_column('id'),
                            'updated_at': literal_column('updated_at'),
                            'deleted_at': timeutils.utcnow()},
                           synchronize_session=synchronize_session)


class Session(sqlalchemy.orm.session.Session):
    """Custom Session class to avoid SqlAlchemy Session monkey patching."""


def get_maker(engine, autocommit=True, expire_on_commit=False):
    """Return a SQLAlchemy sessionmaker using the given engine."""
    return sqlalchemy.orm.sessionmaker(bind=engine,
                                       class_=Session,
                                       autocommit=autocommit,
                                       expire_on_commit=expire_on_commit,
                                       query_cls=Query)


def _add_trace_comments(engine):
    """Add trace comments.

    Augment statements with a trace of the immediate calling code
    for a given statement.
    """

    import os
    import sys
    import traceback
    target_paths = set([
        os.path.dirname(sys.modules['oslo.db'].__file__),
        os.path.dirname(sys.modules['sqlalchemy'].__file__)
    ])

    @sqlalchemy.event.listens_for(engine, "before_cursor_execute", retval=True)
    def before_cursor_execute(conn, cursor, statement, parameters, context,
                              executemany):

        # NOTE(zzzeek) - if different steps per DB dialect are desirable
        # here, switch out on engine.name for now.
        stack = traceback.extract_stack()
        our_line = None
        for idx, (filename, line, method, function) in enumerate(stack):
            for tgt in target_paths:
                if filename.startswith(tgt):
                    our_line = idx
                    break
            if our_line:
                break

        if our_line:
            trace = "; ".join(
                "File: %s (%s) %s" % (
                    line[0], line[1], line[2]
                )
                # include three lines of context.
                for line in stack[our_line - 3:our_line]

            )
            statement = "%s  -- %s" % (statement, trace)

        return statement, parameters


class EngineFacade(object):
    """A helper class for removing of global engine instances from oslo.db.

    As a library, oslo.db can't decide where to store/when to create engine
    and sessionmaker instances, so this must be left for a target application.

    On the other hand, in order to simplify the adoption of oslo.db changes,
    we'll provide a helper class, which creates engine and sessionmaker
    on its instantiation and provides get_engine()/get_session() methods
    that are compatible with corresponding utility functions that currently
    exist in target projects, e.g. in Nova.

    engine/sessionmaker instances will still be global (and they are meant to
    be global), but they will be stored in the app context, rather that in the
    oslo.db context.

    Note: using of this helper is completely optional and you are encouraged to
    integrate engine/sessionmaker instances into your apps any way you like
    (e.g. one might want to bind a session to a request context). Two important
    things to remember:

    1. An Engine instance is effectively a pool of DB connections, so it's
       meant to be shared (and it's thread-safe).
    2. A Session instance is not meant to be shared and represents a DB
       transactional context (i.e. it's not thread-safe). sessionmaker is
       a factory of sessions.

    """

    def __init__(self, sql_connection, slave_connection=None,
                 sqlite_fk=False, autocommit=True,
                 expire_on_commit=False, **kwargs):
        """Initialize engine and sessionmaker instances.

        :param sql_connection: the connection string for the database to use
        :type sql_connection: string

        :param slave_connection: the connection string for the 'slave' database
                                 to use. If not provided, the master database
                                 will be used for all operations. Note: this
                                 is meant to be used for offloading of read
                                 operations to asynchronously replicated slaves
                                 to reduce the load on the master database.
        :type slave_connection: string

        :param sqlite_fk: enable foreign keys in SQLite
        :type sqlite_fk: bool

        :param autocommit: use autocommit mode for created Session instances
        :type autocommit: bool

        :param expire_on_commit: expire session objects on commit
        :type expire_on_commit: bool

        Keyword arguments:

        :keyword mysql_sql_mode: the SQL mode to be used for MySQL sessions.
                                 (defaults to TRADITIONAL)
        :keyword idle_timeout: timeout before idle sql connections are reaped
                               (defaults to 3600)
        :keyword connection_debug: verbosity of SQL debugging information.
                                   -1=Off, 0=None, 100=Everything (defaults
                                   to 0)
        :keyword max_pool_size: maximum number of SQL connections to keep open
                                in a pool (defaults to SQLAlchemy settings)
        :keyword max_overflow: if set, use this value for max_overflow with
                               sqlalchemy (defaults to SQLAlchemy settings)
        :keyword pool_timeout: if set, use this value for pool_timeout with
                               sqlalchemy (defaults to SQLAlchemy settings)
        :keyword sqlite_synchronous: if True, SQLite uses synchronous mode
                                     (defaults to True)
        :keyword connection_trace: add python stack traces to SQL as comment
                                   strings (defaults to False)
        :keyword max_retries: maximum db connection retries during startup.
                              (setting -1 implies an infinite retry count)
                              (defaults to 10)
        :keyword retry_interval: interval between retries of opening a sql
                                 connection (defaults to 10)
        :keyword thread_checkin: boolean that indicates that between each
                                 engine checkin event a sleep(0) will occur to
                                 allow other greenthreads to run (defaults to
                                 True)
        """

        super(EngineFacade, self).__init__()

        engine_kwargs = {
            'sqlite_fk': sqlite_fk,
            'mysql_sql_mode': kwargs.get('mysql_sql_mode', 'TRADITIONAL'),
            'idle_timeout': kwargs.get('idle_timeout', 3600),
            'connection_debug': kwargs.get('connection_debug', 0),
            'max_pool_size': kwargs.get('max_pool_size'),
            'max_overflow': kwargs.get('max_overflow'),
            'pool_timeout': kwargs.get('pool_timeout'),
            'sqlite_synchronous': kwargs.get('sqlite_synchronous', True),
            'connection_trace': kwargs.get('connection_trace', False),
            'max_retries': kwargs.get('max_retries', 10),
            'retry_interval': kwargs.get('retry_interval', 10),
            'thread_checkin': kwargs.get('thread_checkin', True)
        }
        maker_kwargs = {
            'autocommit': autocommit,
            'expire_on_commit': expire_on_commit
        }

        self._engine = create_engine(sql_connection=sql_connection,
                                     **engine_kwargs)
        self._session_maker = get_maker(engine=self._engine,
                                        **maker_kwargs)
        if slave_connection:
            self._slave_engine = create_engine(sql_connection=slave_connection,
                                               **engine_kwargs)
            self._slave_session_maker = get_maker(engine=self._slave_engine,
                                                  **maker_kwargs)
        else:
            self._slave_engine = None
            self._slave_session_maker = None

    def get_engine(self, use_slave=False):
        """Get the engine instance (note, that it's shared).

        :param use_slave: if possible, use 'slave' database for this engine.
                          If the connection string for the slave database
                          wasn't provided, 'master' engine will be returned.
                          (defaults to False)
        :type use_slave: bool

        """

        if use_slave and self._slave_engine:
            return self._slave_engine

        return self._engine

    def get_session(self, use_slave=False, **kwargs):
        """Get a Session instance.

        :param use_slave: if possible, use 'slave' database connection for
                          this session. If the connection string for the
                          slave database wasn't provided, a session bound
                          to the 'master' engine will be returned.
                          (defaults to False)
        :type use_slave: bool

        Keyword arugments will be passed to a sessionmaker instance as is (if
        passed, they will override the ones used when the sessionmaker instance
        was created). See SQLAlchemy Session docs for details.

        """

        if use_slave and self._slave_session_maker:
            return self._slave_session_maker(**kwargs)

        return self._session_maker(**kwargs)

    @classmethod
    def from_config(cls, conf,
                    sqlite_fk=False, autocommit=True, expire_on_commit=False):
        """Initialize EngineFacade using oslo.config config instance options.

        :param conf: oslo.config config instance
        :type conf: oslo.config.cfg.ConfigOpts

        :param sqlite_fk: enable foreign keys in SQLite
        :type sqlite_fk: bool

        :param autocommit: use autocommit mode for created Session instances
        :type autocommit: bool

        :param expire_on_commit: expire session objects on commit
        :type expire_on_commit: bool

        """

        conf.register_opts(options.database_opts, 'database')

        return cls(sql_connection=conf.database.connection,
                   slave_connection=conf.database.slave_connection,
                   sqlite_fk=sqlite_fk,
                   autocommit=autocommit,
                   expire_on_commit=expire_on_commit,
                   mysql_sql_mode=conf.database.mysql_sql_mode,
                   idle_timeout=conf.database.idle_timeout,
                   connection_debug=conf.database.connection_debug,
                   max_pool_size=conf.database.max_pool_size,
                   max_overflow=conf.database.max_overflow,
                   pool_timeout=conf.database.pool_timeout,
                   sqlite_synchronous=conf.database.sqlite_synchronous,
                   connection_trace=conf.database.connection_trace,
                   max_retries=conf.database.max_retries,
                   retry_interval=conf.database.retry_interval)
