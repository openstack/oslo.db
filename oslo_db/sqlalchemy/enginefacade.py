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


import contextlib
import functools
import operator
import threading
import warnings

from oslo_config import cfg

from oslo_db import exception
from oslo_db import options
from oslo_db.sqlalchemy import engines
from oslo_db.sqlalchemy import orm


class _symbol(object):
    """represent a fixed symbol."""

    __slots__ = 'name',

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return "symbol(%r)" % self.name


_ASYNC_READER = _symbol('ASYNC_READER')
"""Represent the transaction state of "async reader".

This state indicates that the transaction is a read-only and is
safe to use on an asynchronously updated slave database.

"""

_READER = _symbol('READER')
"""Represent the transaction state of "reader".

This state indicates that the transaction is a read-only and is
only safe to use on a synchronously updated slave database; otherwise
the master database should be used.

"""


_WRITER = _symbol('WRITER')
"""Represent the transaction state of "writer".

This state indicates that the transaction writes data and
should be directed at the master database.

"""


class _Default(object):
    """Mark a value as a default value.

    A value in the local configuration dictionary wrapped with
    _Default() will not take precedence over a value that is specified
    in cfg.CONF.   Values that are set after the fact using configure()
    will supersede those in cfg.CONF.

    """

    __slots__ = 'value',

    _notset = _symbol("NOTSET")

    def __init__(self, value=_notset):
        self.value = value

    @classmethod
    def resolve(cls, value):
        if isinstance(value, _Default):
            v = value.value
            if v is cls._notset:
                return None
            else:
                return v
        else:
            return value

    @classmethod
    def resolve_w_conf(cls, value, conf_namespace, key):
        if isinstance(value, _Default):
            v = getattr(conf_namespace, key, value.value)
            if v is cls._notset:
                return None
            else:
                return v
        else:
            return value

    @classmethod
    def is_set(cls, value):
        return not isinstance(value, _Default) or \
            value.value is not cls._notset

    @classmethod
    def is_set_w_conf(cls, value, conf_namespace, key):
        return not isinstance(value, _Default) or \
            value.value is not cls._notset or \
            hasattr(conf_namespace, key)


class _TransactionFactory(object):
    """A factory for :class:`._TransactionContext` objects.

    By default, there is just one of these, set up
    based on CONF, however instance-level :class:`._TransactionFactory`
    objects can be made, as is the case with the
    :class:`._TestTransactionFactory` subclass used by the oslo.db test suite.

    """
    def __init__(self):
        self._url_cfg = {
            'connection': _Default(),
            'slave_connection': _Default(),
        }
        self._engine_cfg = {
            'sqlite_fk': _Default(False),
            'mysql_sql_mode': _Default('TRADITIONAL'),
            'idle_timeout': _Default(3600),
            'connection_debug': _Default(0),
            'max_pool_size': _Default(),
            'max_overflow': _Default(),
            'pool_timeout': _Default(),
            'sqlite_synchronous': _Default(True),
            'connection_trace': _Default(False),
            'max_retries': _Default(10),
            'retry_interval': _Default(10),
            'thread_checkin': _Default(True)
        }
        self._maker_cfg = {
            'expire_on_commit': _Default(False),
            '__autocommit': True
        }
        self._transaction_ctx_cfg = {
            'rollback_reader_sessions': False,
            }
        self._facade_cfg = {
            'synchronous_reader': True
        }

        # other options that are defined in oslo.db.options.database_opts
        # but do not apply to the standard enginefacade arguments
        # (most seem to apply to api.DBAPI).
        self._ignored_cfg = dict(
            (k, _Default(None)) for k in [
                'db_max_retries', 'db_inc_retry_interval',
                'use_db_reconnect',
                'db_retry_interval', 'min_pool_size',
                'db_max_retry_interval',
                'sqlite_db', 'backend'])

        self._started = False
        self._legacy_facade = None
        self._start_lock = threading.Lock()

    def configure_defaults(self, **kw):
        """Apply default configurational options.

        This method can only be called before any specific
        transaction-beginning methods have been called.

        Configurational options are within a fixed set of keys, and fall
        under three categories: URL configuration, engine configuration,
        and session configuration.  Each key given will be tested against
        these three configuration sets to see which one is applicable; if
        it is not applicable to any set, an exception is raised.

        The configurational options given here act as **defaults**
        when the :class:`._TransactionFactory` is configured using
        a :class:`.oslo.config.cfg.ConfigOpts` object; the options
        present within the :class:`.oslo.config.cfg.ConfigOpts` **take
        precedence** versus the arguments passed here.  By default,
        the :class:`._TransactionFactory` loads in the configuration from
        :data:`oslo.config.cfg.CONF`, after applying the
        :data:`oslo.db.options.database_opts` configurational defaults to it.

        .. seealso::

            :meth:`._TransactionFactory.configure`

        """
        self._configure(True, kw)

    def configure(self, **kw):
        """Apply configurational options.

        This method can only be called before any specific
        transaction-beginning methods have been called.

        Behavior here is the same as that of
        :meth:`._TransactionFactory.configure_defaults`,
        with the exception that values specified here will **supersede** those
        setup in the :class:`.oslo.config.cfg.ConfigOpts` options.

        .. seealso::

            :meth:`._TransactionFactory.configure_defaults`

        """
        self._configure(False, kw)

    def _configure(self, as_defaults, kw):

        if self._started:
            raise TypeError("this TransactionFactory is already started")
        not_supported = []
        for k, v in kw.items():
            for dict_ in (
                    self._url_cfg, self._engine_cfg,
                    self._maker_cfg, self._ignored_cfg,
                    self._facade_cfg, self._transaction_ctx_cfg):
                if k in dict_:
                    dict_[k] = _Default(v) if as_defaults else v
                    break
            else:
                not_supported.append(k)

        if not_supported:
            # would like to raise ValueError here, but there are just
            # too many unrecognized (obsolete?) configuration options
            # coming in from projects
            warnings.warn(
                "Configuration option(s) %r not supported" %
                sorted(not_supported),
                exception.NotSupportedWarning
            )

    def get_legacy_facade(self):
        """Return a :class:`.LegacyEngineFacade` for this factory.

        This facade will make use of the same engine and sessionmaker
        as this factory, however will not share the same transaction context;
        the legacy facade continues to work the old way of returning
        a new Session each time get_session() is called.

        """
        if not self._legacy_facade:
            self._legacy_facade = LegacyEngineFacade(None, _factory=self)
            if not self._started:
                self._start()

        return self._legacy_facade

    def _create_connection(self, mode):
        if not self._started:
            self._start()
        if mode is _WRITER:
            return self._writer_engine.connect()
        elif self.synchronous_reader or mode is _ASYNC_READER:
            return self._reader_engine.connect()
        else:
            return self._writer_engine.connect()

    def _create_session(self, mode, bind=None):
        if not self._started:
            self._start()
        kw = {}
        # don't pass 'bind' if bind is None; the sessionmaker
        # already has a bind to the engine.
        if bind:
            kw['bind'] = bind
        if mode is _WRITER:
            return self._writer_maker(**kw)
        elif self.synchronous_reader or mode is _ASYNC_READER:
            return self._reader_maker(**kw)
        else:
            return self._writer_maker(**kw)

    def _args_for_conf(self, default_cfg, conf):
        if conf is None:
            return dict(
                (key, _Default.resolve(value))
                for key, value in default_cfg.items()
                if _Default.is_set(value)
            )
        else:
            return dict(
                (key, _Default.resolve_w_conf(value, conf.database, key))
                for key, value in default_cfg.items()
                if _Default.is_set_w_conf(value, conf.database, key)
            )

    def _url_args_for_conf(self, conf):
        return self._args_for_conf(self._url_cfg, conf)

    def _engine_args_for_conf(self, conf):
        return self._args_for_conf(self._engine_cfg, conf)

    def _maker_args_for_conf(self, conf):
        return self._args_for_conf(self._maker_cfg, conf)

    def _start(self, conf=False, connection=None, slave_connection=None):
        with self._start_lock:
            # self._started has been checked on the outside
            # when _start() was called.  Within the lock,
            # check the flag once more to detect the case where
            # the start process proceeded while this thread was waiting
            # for the lock.
            if self._started:
                return
            if conf is False:
                conf = cfg.CONF

            # perform register_opts() local to actually using
            # the cfg.CONF to maintain exact compatibility with
            # the EngineFacade design.  This can be changed if needed.
            if conf is not None:
                conf.register_opts(options.database_opts, 'database')

            url_args = self._url_args_for_conf(conf)
            if connection:
                url_args['connection'] = connection
            if slave_connection:
                url_args['slave_connection'] = slave_connection
            engine_args = self._engine_args_for_conf(conf)
            maker_args = self._maker_args_for_conf(conf)
            maker_args['autocommit'] = maker_args.pop('__autocommit')

            self._writer_engine, self._writer_maker = \
                self._setup_for_connection(
                    url_args['connection'],
                    engine_args, maker_args)

            if url_args.get('slave_connection'):
                self._reader_engine, self._reader_maker = \
                    self._setup_for_connection(
                        url_args['slave_connection'],
                        engine_args, maker_args)
            else:
                self._reader_engine, self._reader_maker = \
                    self._writer_engine, self._writer_maker

            self.synchronous_reader = self._facade_cfg['synchronous_reader']

            # set up _started last, so that in case of exceptions
            # we try the whole thing again and report errors
            # correctly
            self._started = True

    def _setup_for_connection(
            self, sql_connection, engine_kwargs, maker_kwargs):
        if sql_connection is None:
            raise exception.CantStartEngineError(
                "No sql_connection parameter is established")
        engine = engines.create_engine(
            sql_connection=sql_connection, **engine_kwargs)
        sessionmaker = orm.get_maker(engine=engine, **maker_kwargs)
        return engine, sessionmaker


class _TestTransactionFactory(_TransactionFactory):
    """A :class:`._TransactionFactory` used by test suites.

    This is a :class:`._TransactionFactory` that can be directly injected
    with an existing engine and sessionmaker.

    Note that while this is used by oslo.db's own tests of
    the enginefacade system, it is also exported for use by
    the test suites of other projects, first as an element of the
    oslo_db.sqlalchemy.test_base module, and secondly may be used by
    external test suites directly.

    Includes a feature to inject itself temporarily as the factory
    within the global :class:`._TransactionContextManager`.

    """
    def __init__(self, engine, maker, apply_global, synchronous_reader):
        self._reader_engine = self._writer_engine = engine
        self._reader_maker = self._writer_maker = maker
        self._started = True
        self._legacy_facade = None
        self.synchronous_reader = synchronous_reader

        self._facade_cfg = _context_manager._factory._facade_cfg
        self._transaction_ctx_cfg = \
            _context_manager._factory._transaction_ctx_cfg
        if apply_global:
            self.existing_factory = _context_manager._factory
            _context_manager._root_factory = self

    def dispose_global(self):
        _context_manager._root_factory = self.existing_factory


class _TransactionContext(object):
    """Represent a single database transaction in progress."""

    def __init__(
            self, factory, global_factory=None,
            rollback_reader_sessions=False):
        """Construct a new :class:`.TransactionContext`.

        :param factory: the :class:`.TransactionFactory` which will
         serve as a source of connectivity.

        :param global_factory: the "global" factory which will be used
         by the global ``_context_manager`` for new ``_TransactionContext``
         objects created under this one.  When left as None the actual
         "global" factory is used.

        :param rollback_reader_sessions: if True, a :class:`.Session` object
         will have its :meth:`.Session.rollback` method invoked at the end
         of a ``@reader`` block, actively rolling back the transaction and
         expiring the objects within, before the :class:`.Session` moves
         on to be closed, which has the effect of releasing connection
         resources back to the connection pool and detaching all objects.
         If False, the :class:`.Session` is
         not affected at the end of a ``@reader`` block; the underlying
         connection referred to by this :class:`.Session` will still
         be released in the enclosing context via the :meth:`.Session.close`
         method, which still ensures that the DBAPI connection is rolled
         back, however the objects associated with the :class:`.Session`
         retain their database-persisted contents after they are detached.

         .. seealso::

            http://docs.sqlalchemy.org/en/rel_0_9/glossary.html#term-released\
            SQLAlchemy documentation on what "releasing resources" means.

        """
        self.factory = factory
        self.global_factory = global_factory
        self.mode = None
        self.session = None
        self.connection = None
        self.transaction = None
        kw = self.factory._transaction_ctx_cfg
        self.rollback_reader_sessions = kw['rollback_reader_sessions']

    @contextlib.contextmanager
    def _connection(self, savepoint=False):
        if self.connection is None:
            try:
                if self.session is not None:
                    # use existing session, which is outer to us
                    self.connection = self.session.connection()
                    if savepoint:
                        with self.connection.begin_nested():
                            yield self.connection
                    else:
                        yield self.connection
                else:
                    # is outermost
                    self.connection = self.factory._create_connection(
                        mode=self.mode)
                    self.transaction = self.connection.begin()
                    try:
                        yield self.connection
                        self._end_connection_transaction(self.transaction)
                    except Exception:
                        self.transaction.rollback()
                        # TODO(zzzeek) do we need save_and_reraise() here,
                        # or do newer eventlets not have issues?  we are using
                        # raw "raise" in many other places in oslo.db already
                        # (and one six.reraise()).
                        raise
                    finally:
                        self.transaction = None
                        self.connection.close()
            finally:
                self.connection = None

        else:
            # use existing connection, which is outer to us
            if savepoint:
                with self.connection.begin_nested():
                    yield self.connection
            else:
                yield self.connection

    @contextlib.contextmanager
    def _session(self, savepoint=False):
        if self.session is None:
            self.session = self.factory._create_session(
                bind=self.connection, mode=self.mode)
            try:
                self.session.begin()
                yield self.session
                self._end_session_transaction(self.session)
            except Exception:
                self.session.rollback()
                # TODO(zzzeek) do we need save_and_reraise() here,
                # or do newer eventlets not have issues?  we are using
                # raw "raise" in many other places in oslo.db already
                # (and one six.reraise()).
                raise
            finally:
                self.session.close()
                self.session = None
        else:
            # use existing session, which is outer to us
            if savepoint:
                with self.session.begin_nested():
                    yield self.session
            else:
                yield self.session

    def _end_session_transaction(self, session):
        if self.mode is _WRITER:
            session.commit()
        elif self.rollback_reader_sessions:
            session.rollback()
        # In the absense of calling session.rollback(),
        # the next call is session.close().  This releases all
        # objects from the session into the detached state, and
        # releases the connection as well; the connection when returned
        # to the pool is either rolled back in any case, or closed fully.

    def _end_connection_transaction(self, transaction):
        if self.mode is _WRITER:
            transaction.commit()
        else:
            transaction.rollback()

    def _produce_block(self, mode, connection, savepoint):
        if mode is _WRITER:
            self._writer()
        elif mode is _ASYNC_READER:
            self._async_reader()
        else:
            self._reader()
        if connection:
            return self._connection(savepoint)
        else:
            return self._session(savepoint)

    def _writer(self):
        if self.mode is None:
            self.mode = _WRITER
        elif self.mode is _READER:
            raise TypeError(
                "Can't upgrade a READER transaction "
                "to a WRITER mid-transaction")
        elif self.mode is _ASYNC_READER:
            raise TypeError(
                "Can't upgrade an ASYNC_READER transaction "
                "to a WRITER mid-transaction")

    def _reader(self):
        if self.mode is None:
            self.mode = _READER
        elif self.mode is _ASYNC_READER:
            raise TypeError(
                "Can't upgrade an ASYNC_READER transaction "
                "to a READER mid-transaction")

    def _async_reader(self):
        if self.mode is None:
            self.mode = _ASYNC_READER


class _TransactionContextTLocal(threading.local):
    def __deepcopy__(self, memo):
        return self


class _TransactionContextManager(object):
    """Provide context-management and decorator patterns for transactions.

    This object integrates user-defined "context" objects with the
    :class:`._TransactionContext` class, on behalf of a
    contained :class:`._TransactionFactory`.

    """

    def __init__(
            self, root=None,
            mode=None,
            independent=False,
            savepoint=False,
            connection=False,
            replace_global_factory=None,
            _is_global_manager=False):

        if root is None:
            self._root = self
            self._root_factory = _TransactionFactory()
        else:
            self._root = root

        self._replace_global_factory = replace_global_factory
        self._is_global_manager = _is_global_manager
        self._mode = mode
        self._independent = independent
        self._savepoint = savepoint
        if self._savepoint and self._independent:
            raise TypeError(
                "setting savepoint and independent makes no sense.")
        self._connection = connection

    @property
    def _factory(self):
        """The :class:`._TransactionFactory` associated with this context."""
        return self._root._root_factory

    def configure(self, **kw):
        """Apply configurational options to the factory.

        This method can only be called before any specific
        transaction-beginning methods have been called.


        """
        self._factory.configure(**kw)

    @property
    def replace(self):
        """Modifier to replace the global transaction factory with this one."""
        return self._clone(replace_global_factory=self._factory)

    @property
    def writer(self):
        """Modifier to set the transaction to WRITER."""
        return self._clone(mode=_WRITER)

    @property
    def reader(self):
        """Modifier to set the transaction to READER."""
        return self._clone(mode=_READER)

    @property
    def independent(self):
        """Modifier to start a transaction independent from any enclosing."""
        return self._clone(independent=True)

    @property
    def savepoint(self):
        """Modifier to start a SAVEPOINT if a transaction already exists."""
        return self._clone(savepoint=True)

    @property
    def connection(self):
        """Modifier to return a core Connection object instead of Session."""
        return self._clone(connection=True)

    @property
    def async(self):
        """Modifier to set a READER operation to ASYNC_READER."""

        if self._mode is _WRITER:
            raise TypeError("Setting async on a WRITER makes no sense")
        return self._clone(mode=_ASYNC_READER)

    def using(self, context):
        """Provide a context manager block that will use the given context."""
        return self._transaction_scope(context)

    def __call__(self, fn):
        """Decorate a function."""

        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            context = args[0]

            with self._transaction_scope(context):
                return fn(*args, **kwargs)

        return wrapper

    def _clone(self, **kw):
        default_kw = {
            "independent": self._independent,
            "mode": self._mode,
            "connection": self._connection
        }
        default_kw.update(kw)
        return _TransactionContextManager(root=self._root, **default_kw)

    @contextlib.contextmanager
    def _transaction_scope(self, context):
        new_transaction = self._independent
        transaction_contexts_by_thread = \
            _transaction_contexts_by_thread(context)

        current = restore = getattr(
            transaction_contexts_by_thread, "current", None)

        use_factory = self._factory
        global_factory = None

        if self._replace_global_factory:
            use_factory = global_factory = self._replace_global_factory
        elif current is not None and current.global_factory:
            global_factory = current.global_factory

            if self._root._is_global_manager:
                use_factory = global_factory

        if current is not None and (
                new_transaction or current.factory is not use_factory
        ):
            current = None

        if current is None:
            current = transaction_contexts_by_thread.current = \
                _TransactionContext(
                    use_factory, global_factory=global_factory,
                    **use_factory._transaction_ctx_cfg)

        try:
            if self._mode is not None:
                with current._produce_block(
                    mode=self._mode,
                    connection=self._connection,
                    savepoint=self._savepoint) as resource:
                    yield resource
            else:
                yield
        finally:
            if restore is None:
                del transaction_contexts_by_thread.current
            elif current is not restore:
                transaction_contexts_by_thread.current = restore


def _context_descriptor(attr=None):
    getter = operator.attrgetter(attr)

    def _property_for_context(context):
        try:
            transaction_context = context.transaction_ctx
        except exception.NoEngineContextEstablished:
            raise exception.NoEngineContextEstablished(
                "No TransactionContext is established for "
                "this %s object within the current thread; "
                "the %r attribute is unavailable."
                % (context, attr)
            )
        else:
            result = getter(transaction_context)
            if result is None:
                raise exception.ContextNotRequestedError(
                    "The '%s' context attribute was requested but "
                    "it has not been established for this context." % attr
                )
            return result
    return property(_property_for_context)


def _transaction_ctx_for_context(context):
    by_thread = _transaction_contexts_by_thread(context)
    try:
        return by_thread.current
    except AttributeError:
        raise exception.NoEngineContextEstablished(
            "No TransactionContext is established for "
            "this %s object within the current thread. "
            % context
        )


def _transaction_contexts_by_thread(context):
    transaction_contexts_by_thread = getattr(
        context, '_enginefacade_context', None)
    if transaction_contexts_by_thread is None:
        transaction_contexts_by_thread = \
            context._enginefacade_context = _TransactionContextTLocal()

    return transaction_contexts_by_thread


def transaction_context_provider(klass):
    """Decorate a class with ``session`` and ``connection`` attributes."""

    setattr(
        klass,
        'transaction_ctx',
        property(_transaction_ctx_for_context))

    # Graft transaction context attributes as context properties
    for attr in ('session', 'connection', 'transaction'):
        setattr(klass, attr, _context_descriptor(attr))

    return klass


_context_manager = _TransactionContextManager(_is_global_manager=True)
"""default context manager."""


def transaction_context():
    """Construct a local transaction context.

    """
    return _TransactionContextManager()


def configure(**kw):
    """Apply configurational options to the global factory.

    This method can only be called before any specific transaction-beginning
    methods have been called.

    .. seealso::

        :meth:`._TransactionFactory.configure`

    """
    _context_manager._factory.configure(**kw)


def get_legacy_facade():
    """Return a :class:`.LegacyEngineFacade` for the global factory.

    This facade will make use of the same engine and sessionmaker
    as this factory, however will not share the same transaction context;
    the legacy facade continues to work the old way of returning
    a new Session each time get_session() is called.

    """
    return _context_manager._factory.get_legacy_facade()


reader = _context_manager.reader
"""The global 'reader' starting point."""


writer = _context_manager.writer
"""The global 'writer' starting point."""


class LegacyEngineFacade(object):
    """A helper class for removing of global engine instances from oslo.db.

    .. deprecated::

        EngineFacade is deprecated.  Please use
        oslo.db.sqlalchemy.enginefacade for new development.

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

    Two important things to remember:

    1. An Engine instance is effectively a pool of DB connections, so it's
       meant to be shared (and it's thread-safe).
    2. A Session instance is not meant to be shared and represents a DB
       transactional context (i.e. it's not thread-safe). sessionmaker is
       a factory of sessions.

    """
    def __init__(self, sql_connection, slave_connection=None,
                 sqlite_fk=False, autocommit=True,
                 expire_on_commit=False, _conf=None, _factory=None, **kwargs):
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
        warnings.warn(
            "EngineFacade is deprecated; please use "
            "oslo.db.sqlalchemy.enginefacade",
            exception.OsloDBDeprecationWarning,
            stacklevel=2)

        if _factory:
            self._factory = _factory
        else:
            self._factory = _TransactionFactory()

            self._factory.configure(
                sqlite_fk=sqlite_fk,
                __autocommit=autocommit,
                expire_on_commit=expire_on_commit,
                **kwargs
            )
            # make sure passed-in urls are favored over that
            # of config
            self._factory._start(
                _conf, connection=sql_connection,
                slave_connection=slave_connection)

    def get_engine(self, use_slave=False):
        """Get the engine instance (note, that it's shared).

        :param use_slave: if possible, use 'slave' database for this engine.
                          If the connection string for the slave database
                          wasn't provided, 'master' engine will be returned.
                          (defaults to False)
        :type use_slave: bool

        """
        if use_slave:
            return self._factory._reader_engine
        else:
            return self._factory._writer_engine

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
        if use_slave:
            return self._factory._reader_maker(**kwargs)
        else:
            return self._factory._writer_maker(**kwargs)

    def get_sessionmaker(self, use_slave=False):
        """Get the sessionmaker instance used to create a Session.

        This can be called for those cases where the sessionmaker() is to
        be temporarily injected with some state such as a specific connection.

        """
        if use_slave:
            return self._factory._reader_maker
        else:
            return self._factory._writer_maker

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

        return cls(
            None,
            sqlite_fk=sqlite_fk,
            autocommit=autocommit,
            expire_on_commit=expire_on_commit, _conf=conf)
