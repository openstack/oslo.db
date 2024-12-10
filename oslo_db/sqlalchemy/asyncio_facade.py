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
from __future__ import annotations

import asyncio
import contextlib
import contextvars
import functools
import inspect
import itertools
import logging
import operator
from typing import AsyncContextManager
from typing import AsyncIterator
from typing import TYPE_CHECKING
from typing import TypeVar
from typing import Union

from oslo_db import exception
from oslo_db.sqlalchemy import engines
from oslo_utils import excutils
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.util.concurrency import await_only
from sqlalchemy.util.concurrency import greenlet_spawn

from . import enginefacade as _sync_facade
from .enginefacade import _AbstractTransactionContext
from .enginefacade import _AbstractTransactionContextManager
from .enginefacade import _AbstractTransactionFactory

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncConnection
    from sqlalchemy.ext.asyncio import AsyncSession

_AES = TypeVar("_AES", bound=Union["AsyncConnection", "AsyncSession", None])

LOG = logging.getLogger(__name__)


def _create_async_engine(url, **engine_args):
    async_engine = create_async_engine(url, **engine_args)
    return async_engine, async_engine.engine


def _test_async_connection(engine, max_retries, retry_interval):
    if max_retries == -1:
        attempts = itertools.count()
    else:
        attempts = range(max_retries)
    # See: http://legacy.python.org/dev/peps/pep-3110/#semantic-changes for
    # why we are not using 'de' directly (it can be removed from the local
    # scope).
    de_ref = None
    for attempt in attempts:
        try:
            conn = engine.connect()
        except exception.DBConnectionError as de:
            msg = "SQL async connection failed. %s attempts left."
            LOG.warning(msg, max_retries - attempt)
            await_only(asyncio.sleep(retry_interval))
            de_ref = de
        else:
            conn.close()
    else:
        if de_ref is not None:
            raise de_ref


class _GreenletAdaptedLock:
    def __init__(self):
        self._lock = asyncio.Lock()

    def __enter__(self):
        await_only(self._lock.acquire())

    def __exit__(self, *arg, **kw):
        self._lock.release()


class _AsyncioTransactionFactory(_AbstractTransactionFactory):
    """A factory for :class:`._AsyncioTransactionContext` objects.

    By default, there is just one of these, set up
    based on CONF, however instance-level :class:`._TransactionFactory`
    objects can be made, as is the case with the
    :class:`._TestTransactionFactory` subclass used by the oslo.db test suite.
    """

    _start_lock: _GreenletAdaptedLock

    _writer_engine: AsyncEngine
    _reader_engine: AsyncEngine
    _writer_maker: async_sessionmaker
    _reader_maker: async_sessionmaker

    def _make_lock(self):
        return _GreenletAdaptedLock()

    if TYPE_CHECKING:

        def get_writer_engine(self) -> AsyncEngine:
            """Return the writer engine for this factory.

            Implies start.
            """
            ...

        def get_reader_engine(self) -> AsyncEngine:
            """Return the reader engine for this factory.

            Implies start.
            """
            ...

        def get_writer_maker(self) -> async_sessionmaker:
            """Return the writer sessionmaker for this factory.

            Implies start.
            """
            ...

        def get_reader_maker(self) -> async_sessionmaker:
            """Return the reader sessionmaker for this factory.

            Implies start.
            """
            ...

    async def dispose_pool_async(self):
        """Call engine.dispose() on underlying AsyncEngine objects."""

        async with self._start_lock._lock:
            if not self._started:
                return

            await self._writer_engine.dispose()
            if self._reader_engine is not self._writer_engine:
                await self._reader_engine.dispose()

    def _setup_for_connection(
        self,
        sql_connection,
        engine_kwargs,
        maker_kwargs,
    ):
        if sql_connection is None:
            raise exception.CantStartEngineError(
                "No async sql_connection parameter is established"
            )

        engine = engines.create_engine(
            sql_connection=sql_connection,
            _engine_target=_create_async_engine,
            _test_connection=_test_async_connection,
            **engine_kwargs,
        )
        for hook in self._facade_cfg["on_engine_create"]:
            hook(engine)
        sessionmaker = async_sessionmaker(bind=engine, **maker_kwargs)
        return engine, sessionmaker

    async def _create_async_connection(self, mode):
        if not self._started:
            await greenlet_spawn(self._start)
        if mode is _sync_facade._WRITER:
            return await self._writer_engine.connect()
        elif mode is _sync_facade._ASYNC_READER or (
            mode is _sync_facade._READER and not self.synchronous_reader
        ):
            return await self._reader_engine.connect()
        else:
            return await self._writer_engine.connect()

    async def _create_async_session(self, mode, bind=None):
        if not self._started:
            await greenlet_spawn(self._start)
        kw = {}
        # don't pass 'bind' if bind is None; the sessionmaker
        # already has a bind to the engine.
        if bind:
            kw["bind"] = bind
        if mode is _sync_facade._WRITER:
            return self._writer_maker(**kw)
        elif mode is _sync_facade._ASYNC_READER or (
            mode is _sync_facade._READER and not self.synchronous_reader
        ):
            return self._reader_maker(**kw)
        else:
            return self._writer_maker(**kw)


class _TransactionContextContextVar:
    __slots__ = ("_context_vars",)

    def __init__(self):
        object.__setattr__(self, "_context_vars", {})

    def _get_context_var(self, key):
        if key not in self._context_vars:
            self._context_vars[key] = var = contextvars.ContextVar(key)
        else:
            var = self._context_vars[key]
        return var

    def __getattr__(self, key):
        context_var = self._get_context_var(key)
        try:
            return context_var.get()
        except LookupError as le:
            raise AttributeError(key) from le

    def __setattr__(self, key, value):
        context_var = self._get_context_var(key)
        context_var.set(value)

    def __delattr__(self, key):
        context_var = self._get_context_var(key)
        context_var.set(None)

    def __deepcopy__(self, memo):
        return self

    def __reduce__(self):
        return _TransactionContextContextVar, ()


def _async_transaction_ctx_for_context(context):
    by_coroutine = _transaction_contexts_by_coroutine(context)
    try:
        return by_coroutine.current
    except AttributeError:
        raise exception.NoEngineContextEstablished(
            "No AsyncTransactionContext is established for "
            f"this {context} object within the current thread. "
            "Ensure this Context class participates in the "
            "async_transaction_context_provider() class decorator."
        )


def _transaction_contexts_by_coroutine(context):
    transaction_contexts_by_coroutine = getattr(
        context, "_asyncio_facade_context", None
    )
    if transaction_contexts_by_coroutine is None:
        transaction_contexts_by_coroutine = context._asyncio_facade_context = (
            _TransactionContextContextVar()
        )

    return transaction_contexts_by_coroutine


class _AsyncTransactionContextManager(
    _AbstractTransactionContextManager[_AES]
):

    def using(self, context) -> AsyncContextManager[_AES]:
        """Provide a context manager block that will use the given context."""
        return self._async_transaction_scope(context)

    @property
    def connection(self) -> _AsyncTransactionContextManager[AsyncConnection]:
        """Modifier to return a core Connection object instead of Session."""
        return self._clone(connection=True)

    def __call__(self, fn):
        """Decorate an awaitable function."""
        argspec = inspect.getfullargspec(fn)
        if argspec.args[0] == "self" or argspec.args[0] == "cls":
            context_index = 1
        else:
            context_index = 0
        context_kw = argspec.args[context_index]

        @functools.wraps(fn)
        async def wrapper(*args, **kwargs):
            context = kwargs.get(context_kw, None)
            if not context:
                context = args[context_index]

            async with self._async_transaction_scope(context):
                return await fn(*args, **kwargs)

        return wrapper

    @contextlib.asynccontextmanager
    async def _async_transaction_scope(self, context) -> AsyncIterator[_AES]:
        transaction_contexts_by_coroutine = _transaction_contexts_by_coroutine(
            context
        )

        current, restore = self._transaction_scope_impl(
            transaction_contexts_by_coroutine
        )

        try:
            if self._mode is not None:
                async with current._produce_asyncio_block(
                    mode=self._mode,
                    connection=self._connection,
                    savepoint=self._savepoint,
                    allow_async=self._allow_async,
                    context=context,
                ) as resource:
                    yield resource
            else:
                yield
        finally:
            if restore is None:
                del transaction_contexts_by_coroutine.current
            elif current is not restore:
                transaction_contexts_by_coroutine.current = restore

    def _create_root_factory(self):
        return _AsyncioTransactionFactory()

    def _create_transaction_context(self, use_factory, global_factory):
        return _AsyncTransactionContext(
            use_factory, global_factory=global_factory
        )


class _AsyncTransactionContext(_AbstractTransactionContext):

    @property
    def async_connection(self):
        return self.connection

    @property
    def async_session(self):
        return self.session

    async def _end_async_session_transaction(self, session):
        if self.mode is _sync_facade._WRITER:
            await session.commit()
        elif self.rollback_reader_sessions:
            await session.rollback()
        # In the absence of calling session.rollback(),
        # the next call is session.close().  This releases all
        # objects from the session into the detached state, and
        # releases the connection as well; the connection when returned
        # to the pool is either rolled back in any case, or closed fully.

    async def _end_async_connection_transaction(self, transaction):
        if self.mode is _sync_facade._WRITER:
            await transaction.commit()
        else:
            await transaction.rollback()

    def _produce_asyncio_block(
        self, mode, connection, savepoint, allow_async=False, context=None
    ):
        if mode is _sync_facade._WRITER:
            self._writer()
        elif mode is _sync_facade._ASYNC_READER:
            self._async_reader()
        else:
            self._reader(allow_async)
        if connection:
            return self._async_connection(savepoint, context=context)
        else:
            return self._async_session(savepoint, context=context)

    @contextlib.asynccontextmanager
    async def _async_connection(self, savepoint=False, context=None):
        if self.connection is None:
            try:
                if self.session is not None:
                    # use existing session, which is outer to us
                    self.connection = await self.session.connection()
                    if savepoint:
                        with self.connection.begin_nested(), self._add_context(
                            self.connection, context
                        ):
                            yield self.connection
                    else:
                        with self._add_context(self.connection, context):
                            yield self.connection
                else:
                    # is outermost
                    self.connection = (
                        await self.factory._create_async_connection(
                            mode=self.mode
                        )
                    )
                    self.transaction = await self.connection.begin()
                    try:
                        with self._add_context(self.connection, context):
                            yield self.connection
                        await self._end_async_connection_transaction(
                            self.transaction
                        )
                    except Exception:
                        await self.transaction.rollback()
                        # TODO(zzzeek) do we need save_and_reraise() here,
                        # or do newer eventlets not have issues?  we are using
                        # raw "raise" in many other places in oslo.db already
                        raise
                    finally:
                        self.transaction = None
                        await self.connection.close()
            finally:
                self.connection = None

        else:
            # use existing connection, which is outer to us
            if savepoint:
                async with self.connection.begin_nested():
                    with self._add_context(self.connection, context):
                        yield self.connection
            else:
                with self._add_context(self.connection, context):
                    yield self.connection

    @contextlib.asynccontextmanager
    async def _async_session(self, savepoint=False, context=None):
        if self.session is None:
            self.session = await self.factory._create_async_session(
                bind=self.connection, mode=self.mode
            )
            try:
                await self.session.begin()
                with self._add_context(self.session, context):
                    yield self.session
                await self._end_async_session_transaction(self.session)
            except Exception:
                with excutils.save_and_reraise_exception():
                    await self.session.rollback()
            finally:
                await self.session.close()
                self.session = None
        else:
            # use existing session, which is outer to us
            if savepoint:
                async with self.session.begin_nested():
                    with self._add_context(self.session, context):
                        yield self.session
            else:
                with self._add_context(self.session, context):
                    yield self.session
                if self.flush_on_subtransaction:
                    await self.session.flush()


def configure(**kw):
    """Apply configurational options to the global factory.

    This method can only be called before any specific transaction-beginning
    methods have been called.

    .. seealso::

        :meth:`._TransactionFactory.configure`

    """
    _async_context_manager._factory.configure(**kw)


def async_transaction_context_provider(klass):
    """Decorate a class with ``session`` and ``connection`` attributes."""

    setattr(
        klass,
        "async_transaction_ctx",
        property(_async_transaction_ctx_for_context),
    )

    # Graft transaction context attributes as context properties
    for attr in ("async_session", "async_connection", "async_transaction"):
        setattr(
            klass,
            attr,
            _sync_facade._context_descriptor(
                attr,
                transaction_ctx_getter=operator.attrgetter(
                    "async_transaction_ctx"
                ),
            ),
        )

    return klass


_async_context_manager: _AsyncTransactionContextManager[AsyncSession] = (
    _AsyncTransactionContextManager(_is_global_manager=True)
)
"""default context manager."""


def transaction_context():
    """Construct a local transaction context."""
    return _AsyncTransactionContextManager()


reader = _async_context_manager.reader
"""The global 'reader' starting point."""


writer = _async_context_manager.writer
"""The global 'writer' starting point."""
