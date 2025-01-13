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

import collections
import contextlib
import copy
import fixtures
import pickle
from unittest import mock
import warnings

from oslo_config import cfg
from oslo_context import context as oslo_context
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy.orm import registry
from sqlalchemy.orm import Session
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import Table

from oslo_db import exception
from oslo_db import options
from oslo_db.sqlalchemy import enginefacade
from oslo_db.sqlalchemy import engines as oslo_engines
from oslo_db.sqlalchemy import orm
from oslo_db.tests import base as test_base
from oslo_db.tests.sqlalchemy import base as db_test_base
from oslo_db import warning

enginefacade.transaction_context_provider(oslo_context.RequestContext)


class SingletonOnName(mock.MagicMock):
    def __init__(self, the_name, **kw):
        super().__init__(
            __eq__=lambda self, other: other._assert_name == self._assert_name,
            _assert_name=the_name,
            **kw
        )

    def __deepcopy__(self, memo):
        return self


class SingletonConnection(SingletonOnName):
    def __init__(self, **kw):
        super().__init__(
            "connection", **kw)
        self.info = {}


class SingletonEngine(SingletonOnName):
    def __init__(self, connection, **kw):
        super().__init__(
            "engine",
            connect=mock.Mock(return_value=connection),
            pool=mock.Mock(),
            url=connection,
            _assert_connection=connection,
            **kw
        )


class NonDecoratedContext:
    """a Context object that's not run through transaction_context_provider."""


class AssertDataSource(collections.namedtuple(
    "AssertDataSource", ["writer", "reader", "async_reader"])):

    def element_for_writer(self, const):
        if const is enginefacade._WRITER:
            return self.writer
        elif const is enginefacade._READER:
            return self.reader
        elif const is enginefacade._ASYNC_READER:
            return self.async_reader
        else:
            assert False, "Unknown constant: %s" % const


class MockFacadeTest(test_base.BaseTestCase):
    """test by applying mocks to internal call-points.

    This applies mocks to
    oslo.db.sqlalchemy.engines.create_engine() and
    oslo.db.sqlalchemy.orm.get_maker(), then mocking a
    _TransactionFactory into
    oslo.db.sqlalchemy.enginefacade._context_manager._root_factory.

    Various scenarios are run against the enginefacade functions, and the
    exact calls made against the mock create_engine(), get_maker(), and
    associated objects are tested exactly against expected calls.

    """

    synchronous_reader = True

    engine_uri = 'some_connection'
    slave_uri = None

    def setUp(self):
        super().setUp()

        writer_conn = SingletonConnection()
        writer_engine = SingletonEngine(writer_conn)
        writer_session = mock.Mock(
            connection=mock.Mock(return_value=writer_conn),
            info={})
        writer_maker = mock.Mock(return_value=writer_session)

        if self.slave_uri:
            async_reader_conn = SingletonConnection()
            async_reader_engine = SingletonEngine(async_reader_conn)
            async_reader_session = mock.Mock(
                connection=mock.Mock(return_value=async_reader_conn),
                info={})
            async_reader_maker = mock.Mock(return_value=async_reader_session)

        else:
            async_reader_conn = writer_conn
            async_reader_engine = writer_engine
            async_reader_session = writer_session
            async_reader_maker = writer_maker

        if self.synchronous_reader:
            reader_conn = writer_conn
            reader_engine = writer_engine
            reader_session = writer_session
            reader_maker = writer_maker
        else:
            reader_conn = async_reader_conn
            reader_engine = async_reader_engine
            reader_session = async_reader_session
            reader_maker = async_reader_maker

        self.connections = AssertDataSource(
            writer_conn, reader_conn, async_reader_conn
        )
        self.engines = AssertDataSource(
            writer_engine, reader_engine, async_reader_engine
        )
        self.sessions = AssertDataSource(
            writer_session, reader_session, async_reader_session
        )
        self.makers = AssertDataSource(
            writer_maker, reader_maker, async_reader_maker
        )

        def get_maker(engine, **kw):
            if engine is writer_engine:
                return self.makers.writer
            elif engine is reader_engine:
                return self.makers.reader
            elif engine is async_reader_engine:
                return self.makers.async_reader
            else:
                assert False

        session_patch = mock.patch.object(
            orm, "get_maker",
            side_effect=get_maker)
        self.get_maker = session_patch.start()
        self.addCleanup(session_patch.stop)

        def create_engine(sql_connection, **kw):
            if sql_connection == self.engine_uri:
                return self.engines.writer
            elif sql_connection == self.slave_uri:
                return self.engines.async_reader
            else:
                assert False

        engine_patch = mock.patch.object(
            oslo_engines, "create_engine", side_effect=create_engine)

        self.create_engine = engine_patch.start()
        self.addCleanup(engine_patch.stop)

        self.factory = enginefacade._TransactionFactory()
        self.factory.configure(
            synchronous_reader=self.synchronous_reader
        )

        self.factory.configure(
            connection=self.engine_uri,
            slave_connection=self.slave_uri
        )

        facade_patcher = mock.patch.object(
            enginefacade._context_manager, "_root_factory", self.factory)
        facade_patcher.start()
        self.addCleanup(facade_patcher.stop)

    def _assert_ctx_connection(self, context, connection):
        self.assertIs(context.connection, connection)

    def _assert_ctx_session(self, context, session):
        self.assertIs(context.session, session)

    def _assert_non_decorated_ctx_connection(self, context, connection):
        transaction_ctx = enginefacade._transaction_ctx_for_context(context)
        self.assertIs(transaction_ctx.connection, connection)

    def _assert_non_decorated_ctx_session(self, context, session):
        transaction_ctx = enginefacade._transaction_ctx_for_context(context)
        self.assertIs(transaction_ctx.session, session)

    @contextlib.contextmanager
    def _assert_engines(self):
        """produce a mock series of engine calls.

        These are expected to match engine-related calls established
        by the test subject.

        """

        writer_conn = SingletonConnection()
        writer_engine = SingletonEngine(writer_conn)
        if self.slave_uri:
            async_reader_conn = SingletonConnection()
            async_reader_engine = SingletonEngine(async_reader_conn)
        else:
            async_reader_conn = writer_conn
            async_reader_engine = writer_engine

        if self.synchronous_reader:
            reader_engine = writer_engine
        else:
            reader_engine = async_reader_engine

        engines = AssertDataSource(
            writer_engine, reader_engine, async_reader_engine)

        def create_engine(sql_connection, **kw):
            if sql_connection == self.engine_uri:
                return engines.writer
            elif sql_connection == self.slave_uri:
                return engines.async_reader
            else:
                assert False

        engine_factory = mock.Mock(side_effect=create_engine)
        engine_factory(
            sql_connection=self.engine_uri,
            **{
                k: mock.ANY for k in self.factory._engine_cfg.keys()
            },
        )
        if self.slave_uri:
            engine_factory(
                sql_connection=self.slave_uri,
                **{
                    k: mock.ANY for k in self.factory._engine_cfg.keys()
                },
            )

        yield AssertDataSource(
            writer_engine, reader_engine, async_reader_engine
        )

        self.assertEqual(
            engine_factory.mock_calls,
            self.create_engine.mock_calls
        )

        for sym in [
            enginefacade._WRITER, enginefacade._READER,
            enginefacade._ASYNC_READER
        ]:
            self.assertEqual(
                engines.element_for_writer(sym).mock_calls,
                self.engines.element_for_writer(sym).mock_calls
            )

    def _assert_async_reader_connection(self, engines, session=None):
        return self._assert_connection(
            engines, enginefacade._ASYNC_READER, session)

    def _assert_reader_connection(self, engines, session=None):
        return self._assert_connection(engines, enginefacade._READER, session)

    def _assert_writer_connection(self, engines, session=None):
        return self._assert_connection(engines, enginefacade._WRITER, session)

    @contextlib.contextmanager
    def _assert_connection(self, engines, writer, session=None):
        """produce a mock series of connection calls.

        These are expected to match connection-related calls established
        by the test subject.

        """
        if session:
            connection = session.connection()
            yield connection
        else:
            connection = engines.element_for_writer(writer).connect()
            trans = connection.begin()
            yield connection
            if writer is enginefacade._WRITER:
                trans.commit()
            else:
                trans.rollback()
            connection.close()

        self.assertEqual(
            connection.mock_calls,
            self.connections.element_for_writer(writer).mock_calls)

    @contextlib.contextmanager
    def _assert_makers(self, engines):

        writer_session = mock.Mock(connection=mock.Mock(
            return_value=engines.writer._assert_connection)
        )
        writer_maker = mock.Mock(return_value=writer_session)

        if self.slave_uri:
            async_reader_session = mock.Mock(connection=mock.Mock(
                return_value=engines.async_reader._assert_connection)
            )
            async_reader_maker = mock.Mock(return_value=async_reader_session)
        else:
            async_reader_session = writer_session
            async_reader_maker = writer_maker

        if self.synchronous_reader:
            reader_maker = writer_maker
        else:
            reader_maker = async_reader_maker

        makers = AssertDataSource(
            writer_maker,
            reader_maker,
            async_reader_maker,
        )

        def get_maker(engine, **kw):
            if engine is engines.writer:
                return makers.writer
            elif engine is engines.reader:
                return makers.reader
            elif engine is engines.async_reader:
                return makers.async_reader
            else:
                assert False

        maker_factories = mock.Mock(side_effect=get_maker)

        maker_factories(
            engine=engines.writer,
            expire_on_commit=False)
        if self.slave_uri:
            maker_factories(
                engine=engines.async_reader,
                expire_on_commit=False)

        yield makers

        self.assertEqual(
            maker_factories.mock_calls,
            self.get_maker.mock_calls)

        for sym in [
            enginefacade._WRITER, enginefacade._READER,
            enginefacade._ASYNC_READER
        ]:
            self.assertEqual(
                makers.element_for_writer(sym).mock_calls,
                self.makers.element_for_writer(sym).mock_calls)

    def _assert_async_reader_session(
            self, makers, connection=None, assert_calls=True):
        return self._assert_session(
            makers, enginefacade._ASYNC_READER, connection, assert_calls)

    def _assert_reader_session(
            self, makers, connection=None, assert_calls=True):
        return self._assert_session(
            makers, enginefacade._READER,
            connection, assert_calls)

    def _assert_writer_session(
            self, makers, connection=None, assert_calls=True):
        return self._assert_session(
            makers, enginefacade._WRITER,
            connection, assert_calls)

    def _emit_sub_writer_session(self, session):
        return self._emit_sub_session(enginefacade._WRITER, session)

    def _emit_sub_reader_session(self, session):
        return self._emit_sub_session(enginefacade._READER, session)

    @contextlib.contextmanager
    def _assert_session(
            self, makers, writer, connection=None, assert_calls=True):
        """produce a mock series of session calls.

        These are expected to match session-related calls established
        by the test subject.

        """

        if connection:
            session = makers.element_for_writer(writer)(bind=connection)
        else:
            session = makers.element_for_writer(writer)()
        session.begin()
        yield session
        if writer is enginefacade._WRITER:
            session.commit()
        elif enginefacade.\
            _context_manager._factory._transaction_ctx_cfg[
                'rollback_reader_sessions']:
            session.rollback()
        session.close()

        if assert_calls:
            self.assertEqual(
                session.mock_calls,
                self.sessions.element_for_writer(writer).mock_calls)

    @contextlib.contextmanager
    def _emit_sub_session(self, writer, session):
        yield session
        if enginefacade._context_manager.\
                _factory._transaction_ctx_cfg['flush_on_subtransaction']:
            session.flush()

    def test_dispose_pool(self):
        facade = enginefacade.transaction_context()

        facade.configure(
            connection=self.engine_uri,
            )

        facade.dispose_pool()
        self.assertFalse(hasattr(facade._factory, '_writer_engine'))

        facade._factory._start()
        facade.dispose_pool()

        self.assertEqual(
            facade._factory._writer_engine.pool.mock_calls,
            [mock.call.dispose()]
        )

    def test_dispose_pool_w_reader(self):
        facade = enginefacade.transaction_context()

        facade.configure(
            connection=self.engine_uri,
            slave_connection=self.slave_uri
        )

        facade.dispose_pool()
        self.assertFalse(hasattr(facade._factory, '_writer_engine'))
        self.assertFalse(hasattr(facade._factory, '_reader_engine'))

        facade._factory._start()
        facade.dispose_pool()

        self.assertEqual(
            facade._factory._writer_engine.pool.mock_calls,
            [mock.call.dispose()]
        )
        self.assertEqual(
            facade._factory._reader_engine.pool.mock_calls,
            [mock.call.dispose()]
        )

    def test_started_flag(self):
        facade = enginefacade.transaction_context()

        self.assertFalse(facade.is_started)
        facade.configure(connection=self.engine_uri)
        facade.writer.get_engine()

        self.assertTrue(facade.is_started)

    def test_started_exception(self):
        facade = enginefacade.transaction_context()

        self.assertFalse(facade.is_started)
        facade.configure(connection=self.engine_uri)
        facade.writer.get_engine()

        exc = self.assertRaises(
            enginefacade.AlreadyStartedError,
            facade.configure,
            connection=self.engine_uri
        )
        self.assertEqual(
            "this TransactionFactory is already started",
            exc.args[0]
        )

    def test_session_reader_decorator(self):
        context = oslo_context.RequestContext()

        @enginefacade.reader
        def go(context):
            context.session.execute("test")
        go(context)

        with self._assert_engines() as engines:
            with self._assert_makers(engines) as makers:
                with self._assert_reader_session(makers) as session:
                    session.execute("test")

    def test_session_reader_decorator_kwarg_call(self):
        context = oslo_context.RequestContext()

        @enginefacade.reader
        def go(context):
            context.session.execute("test")
        go(context=context)

        with self._assert_engines() as engines:
            with self._assert_makers(engines) as makers:
                with self._assert_reader_session(makers) as session:
                    session.execute("test")

    def test_connection_reader_decorator(self):
        context = oslo_context.RequestContext()

        @enginefacade.reader.connection
        def go(context):
            context.connection.execute("test")
        go(context)

        with self._assert_engines() as engines:
            with self._assert_reader_connection(engines) as connection:
                connection.execute("test")

    def test_session_reader_nested_in_connection_reader(self):
        context = oslo_context.RequestContext()

        @enginefacade.reader.connection
        def go1(context):
            context.connection.execute("test1")
            go2(context)

        @enginefacade.reader
        def go2(context):
            context.session.execute("test2")
        go1(context)

        with self._assert_engines() as engines:
            with self._assert_reader_connection(engines) as connection:
                connection.execute("test1")
                with self._assert_makers(engines) as makers:
                    with self._assert_reader_session(
                            makers, connection) as session:
                        session.execute("test2")

    def test_connection_reader_nested_in_session_reader(self):
        context = oslo_context.RequestContext()

        @enginefacade.reader
        def go1(context):
            context.session.execute("test1")
            go2(context)

        @enginefacade.reader.connection
        def go2(context):
            context.connection.execute("test2")

        go1(context)

        with self._assert_engines() as engines:
            with self._assert_makers(engines) as makers:
                with self._assert_reader_session(makers) as session:
                    session.execute("test1")
                    with self._assert_reader_connection(
                            engines, session) as connection:
                        connection.execute("test2")

    def test_session_reader_decorator_nested(self):
        context = oslo_context.RequestContext()

        @enginefacade.reader
        def go1(context):
            context.session.execute("test1")
            go2(context)

        @enginefacade.reader
        def go2(context):
            context.session.execute("test2")
        go1(context)

        with self._assert_engines() as engines:
            with self._assert_makers(engines) as makers:
                with self._assert_reader_session(makers) as session:
                    session.execute("test1")
                    session.execute("test2")

    def test_reader_nested_in_writer_ok(self):
        context = oslo_context.RequestContext()

        @enginefacade.writer
        def go1(context):
            context.session.execute("test1")
            go2(context)

        @enginefacade.reader
        def go2(context):
            context.session.execute("test2")

        go1(context)
        with self._assert_engines() as engines:
            with self._assert_makers(engines) as makers:
                with self._assert_writer_session(makers) as session:
                    session.execute("test1")
                    session.execute("test2")

    def test_writer_nested_in_reader_raises(self):
        context = oslo_context.RequestContext()

        @enginefacade.reader
        def go1(context):
            context.session.execute("test1")
            go2(context)

        @enginefacade.writer
        def go2(context):
            context.session.execute("test2")

        exc = self.assertRaises(
            TypeError, go1, context
        )
        self.assertEqual(
            "Can't upgrade a READER "
            "transaction to a WRITER mid-transaction",
            exc.args[0]
        )

    def test_async_on_writer_raises(self):
        exc = self.assertRaises(
            TypeError, getattr, enginefacade.writer, "async_"
        )
        self.assertEqual(
            "Setting async on a WRITER makes no sense",
            exc.args[0]
        )

    def test_savepoint_and_independent_raises(self):
        exc = self.assertRaises(
            TypeError, getattr, enginefacade.writer.independent, "savepoint"
        )
        self.assertEqual(
            "setting savepoint and independent makes no sense.",
            exc.args[0]
        )

    def test_reader_nested_in_async_reader_raises(self):
        context = oslo_context.RequestContext()

        @enginefacade.reader.async_
        def go1(context):
            context.session.execute("test1")
            go2(context)

        @enginefacade.reader
        def go2(context):
            context.session.execute("test2")

        exc = self.assertRaises(
            TypeError, go1, context
        )
        self.assertEqual(
            "Can't upgrade an ASYNC_READER transaction "
            "to a READER mid-transaction",
            exc.args[0]
        )

    def test_reader_allow_async_nested_in_async_reader(self):
        context = oslo_context.RequestContext()

        @enginefacade.reader.async_
        def go1(context):
            context.session.execute("test1")
            go2(context)

        @enginefacade.reader.allow_async
        def go2(context):
            context.session.execute("test2")

        go1(context)

        with self._assert_engines() as engines:
            with self._assert_makers(engines) as makers:
                with self._assert_async_reader_session(makers) as session:
                    session.execute("test1")
                    session.execute("test2")

    def test_reader_allow_async_nested_in_reader(self):
        context = oslo_context.RequestContext()

        @enginefacade.reader.reader
        def go1(context):
            context.session.execute("test1")
            go2(context)

        @enginefacade.reader.allow_async
        def go2(context):
            context.session.execute("test2")

        go1(context)

        with self._assert_engines() as engines:
            with self._assert_makers(engines) as makers:
                with self._assert_reader_session(makers) as session:
                    session.execute("test1")
                    session.execute("test2")

    def test_reader_allow_async_is_reader_by_default(self):
        context = oslo_context.RequestContext()

        @enginefacade.reader.allow_async
        def go1(context):
            context.session.execute("test1")

        go1(context)

        with self._assert_engines() as engines:
            with self._assert_makers(engines) as makers:
                with self._assert_reader_session(makers) as session:
                    session.execute("test1")

    def test_writer_nested_in_async_reader_raises(self):
        context = oslo_context.RequestContext()

        @enginefacade.reader.async_
        def go1(context):
            context.session.execute("test1")
            go2(context)

        @enginefacade.writer
        def go2(context):
            context.session.execute("test2")

        exc = self.assertRaises(
            TypeError, go1, context
        )
        self.assertEqual(
            "Can't upgrade an ASYNC_READER transaction to a "
            "WRITER mid-transaction",
            exc.args[0]
        )

    def test_reader_then_writer_ok(self):
        context = oslo_context.RequestContext()

        @enginefacade.reader
        def go1(context):
            context.session.execute("test1")

        @enginefacade.writer
        def go2(context):
            context.session.execute("test2")

        go1(context)
        go2(context)

        with self._assert_engines() as engines:
            with self._assert_makers(engines) as makers:
                with self._assert_reader_session(
                        makers, assert_calls=False) as session:
                    session.execute("test1")
                with self._assert_writer_session(makers) as session:
                    session.execute("test2")

    def test_async_reader_then_reader_ok(self):
        context = oslo_context.RequestContext()

        @enginefacade.reader.async_
        def go1(context):
            context.session.execute("test1")

        @enginefacade.reader
        def go2(context):
            context.session.execute("test2")

        go1(context)
        go2(context)

        with self._assert_engines() as engines:
            with self._assert_makers(engines) as makers:
                with self._assert_async_reader_session(
                        makers, assert_calls=False) as session:
                    session.execute("test1")
                with self._assert_reader_session(makers) as session:
                    session.execute("test2")

    def test_using_reader(self):
        context = oslo_context.RequestContext()

        with enginefacade.reader.using(context) as session:
            self._assert_ctx_session(context, session)
            session.execute("test1")

        with self._assert_engines() as engines:
            with self._assert_makers(engines) as makers:
                with self._assert_reader_session(makers) as session:
                    session.execute("test1")

    def test_using_context_present_in_session_info(self):
        context = oslo_context.RequestContext()

        with enginefacade.reader.using(context) as session:
            self.assertEqual(context, session.info['using_context'])
        self.assertIsNone(session.info['using_context'])

    def test_using_context_present_in_connection_info(self):
        context = oslo_context.RequestContext()

        with enginefacade.writer.connection.using(context) as connection:
            self.assertEqual(context, connection.info['using_context'])
        self.assertIsNone(connection.info['using_context'])

    def test_using_reader_rollback_reader_session(self):
        enginefacade.configure(rollback_reader_sessions=True)

        context = oslo_context.RequestContext()

        with enginefacade.reader.using(context) as session:
            self._assert_ctx_session(context, session)
            session.execute("test1")

        with self._assert_engines() as engines:
            with self._assert_makers(engines) as makers:
                with self._assert_reader_session(makers) as session:
                    session.execute("test1")

    def test_using_flush_on_nested(self):
        enginefacade.configure(flush_on_nested=True)

        context = oslo_context.RequestContext()

        with enginefacade.writer.using(context) as session:
            with enginefacade.writer.using(context) as session:
                self._assert_ctx_session(context, session)
                session.execute("test1")

        with self._assert_engines() as engines:
            with self._assert_makers(engines) as makers:
                with self._assert_writer_session(makers) as session:
                    with self._emit_sub_writer_session(
                            session) as session:
                        session.execute("test1")

    def test_using_writer(self):
        context = oslo_context.RequestContext()

        with enginefacade.writer.using(context) as session:
            self._assert_ctx_session(context, session)
            session.execute("test1")

        with self._assert_engines() as engines:
            with self._assert_makers(engines) as makers:
                with self._assert_writer_session(makers) as session:
                    session.execute("test1")

    def test_using_writer_no_descriptors(self):
        context = NonDecoratedContext()

        with enginefacade.writer.using(context) as session:
            self._assert_non_decorated_ctx_session(context, session)
            session.execute("test1")

        with self._assert_engines() as engines:
            with self._assert_makers(engines) as makers:
                with self._assert_writer_session(makers) as session:
                    session.execute("test1")

    def test_using_writer_connection_no_descriptors(self):
        context = NonDecoratedContext()

        with enginefacade.writer.connection.using(context) as connection:
            self._assert_non_decorated_ctx_connection(context, connection)
            connection.execute("test1")

        with self._assert_engines() as engines:
            with self._assert_writer_connection(engines) as conn:
                conn.execute("test1")

    def test_using_reader_connection(self):
        context = oslo_context.RequestContext()

        with enginefacade.reader.connection.using(context) as connection:
            self._assert_ctx_connection(context, connection)
            connection.execute("test1")

        with self._assert_engines() as engines:
            with self._assert_reader_connection(engines) as conn:
                conn.execute("test1")

    def test_using_writer_connection(self):
        context = oslo_context.RequestContext()

        with enginefacade.writer.connection.using(context) as connection:
            self._assert_ctx_connection(context, connection)
            connection.execute("test1")

        with self._assert_engines() as engines:
            with self._assert_writer_connection(engines) as conn:
                conn.execute("test1")

    def test_context_copied_using_existing_writer_connection(self):
        context = oslo_context.RequestContext()

        with enginefacade.writer.connection.using(context) as connection:
            self._assert_ctx_connection(context, connection)
            connection.execute("test1")

            ctx2 = copy.deepcopy(context)

            with enginefacade.reader.connection.using(ctx2) as conn2:
                self.assertIs(conn2, connection)
                self._assert_ctx_connection(ctx2, conn2)

                conn2.execute("test2")

        with self._assert_engines() as engines:
            with self._assert_writer_connection(engines) as conn:
                conn.execute("test1")
                conn.execute("test2")

    def test_context_nodesc_copied_using_existing_writer_connection(self):
        context = NonDecoratedContext()

        with enginefacade.writer.connection.using(context) as connection:
            self._assert_non_decorated_ctx_connection(context, connection)
            connection.execute("test1")

            ctx2 = copy.deepcopy(context)

            with enginefacade.reader.connection.using(ctx2) as conn2:
                self.assertIs(conn2, connection)
                self._assert_non_decorated_ctx_connection(ctx2, conn2)

                conn2.execute("test2")

        with self._assert_engines() as engines:
            with self._assert_writer_connection(engines) as conn:
                conn.execute("test1")
                conn.execute("test2")

    def test_session_context_notrequested_exception(self):
        context = oslo_context.RequestContext()

        with enginefacade.reader.connection.using(context):
            exc = self.assertRaises(
                exception.ContextNotRequestedError,
                getattr, context, 'session'
            )

            self.assertRegex(
                exc.args[0],
                "The 'session' context attribute was requested but it has "
                "not been established for this context."
            )

    def test_connection_context_notrequested_exception(self):
        context = oslo_context.RequestContext()

        with enginefacade.reader.using(context):
            exc = self.assertRaises(
                exception.ContextNotRequestedError,
                getattr, context, 'connection'
            )

            self.assertRegex(
                exc.args[0],
                "The 'connection' context attribute was requested but it has "
                "not been established for this context."
            )

    def test_session_context_exception(self):
        context = oslo_context.RequestContext()
        exc = self.assertRaises(
            exception.NoEngineContextEstablished,
            getattr, context, 'session'
        )

        self.assertRegex(
            exc.args[0],
            "No TransactionContext is established for "
            "this .*RequestContext.* object within the current "
            "thread; the 'session' attribute is unavailable."
        )

    def test_session_context_getattr(self):
        context = oslo_context.RequestContext()
        self.assertIsNone(getattr(context, 'session', None))

    def test_connection_context_exception(self):
        context = oslo_context.RequestContext()
        exc = self.assertRaises(
            exception.NoEngineContextEstablished,
            getattr, context, 'connection'
        )

        self.assertRegex(
            exc.args[0],
            "No TransactionContext is established for "
            "this .*RequestContext.* object within the current "
            "thread; the 'connection' attribute is unavailable."
        )

    def test_connection_context_getattr(self):
        context = oslo_context.RequestContext()
        self.assertIsNone(getattr(context, 'connection', None))

    def test_transaction_context_exception(self):
        context = oslo_context.RequestContext()
        exc = self.assertRaises(
            exception.NoEngineContextEstablished,
            getattr, context, 'transaction'
        )

        self.assertRegex(
            exc.args[0],
            "No TransactionContext is established for "
            "this .*RequestContext.* object within the current "
            "thread; the 'transaction' attribute is unavailable."
        )

    def test_transaction_context_getattr(self):
        context = oslo_context.RequestContext()
        self.assertIsNone(getattr(context, 'transaction', None))

    def test_trans_ctx_context_exception(self):
        context = oslo_context.RequestContext()
        exc = self.assertRaises(
            exception.NoEngineContextEstablished,
            getattr, context, 'transaction_ctx'
        )

        self.assertRegex(
            exc.args[0],
            "No TransactionContext is established for "
            "this .*RequestContext.* object within the current "
            "thread."
        )

    def test_trans_ctx_context_getattr(self):
        context = oslo_context.RequestContext()
        self.assertIsNone(getattr(context, 'transaction_ctx', None))

    def test_multiple_factories(self):
        """Test that the instrumentation applied to a context class is

        independent of a specific _TransactionContextManager
        / _TransactionFactory.

        """
        mgr1 = enginefacade.transaction_context()
        mgr1.configure(
            connection=self.engine_uri,
            slave_connection=self.slave_uri
        )
        mgr2 = enginefacade.transaction_context()
        mgr2.configure(
            connection=self.engine_uri,
            slave_connection=self.slave_uri
        )

        context = oslo_context.RequestContext()

        self.assertRaises(
            exception.NoEngineContextEstablished,
            getattr, context, 'session'
        )
        with mgr1.writer.using(context):
            self.assertIs(context.transaction_ctx.factory, mgr1._factory)
            self.assertIsNot(context.transaction_ctx.factory, mgr2._factory)
            self.assertIsNotNone(context.session)

        self.assertRaises(
            exception.NoEngineContextEstablished,
            getattr, context, 'session'
        )
        with mgr2.writer.using(context):
            self.assertIsNot(context.transaction_ctx.factory, mgr1._factory)
            self.assertIs(context.transaction_ctx.factory, mgr2._factory)
            self.assertIsNotNone(context.session)

    def test_multiple_factories_nested(self):
        """Test that the instrumentation applied to a context class supports

        nested calls among multiple _TransactionContextManager objects.

        """
        mgr1 = enginefacade.transaction_context()
        mgr1.configure(
            connection=self.engine_uri,
            slave_connection=self.slave_uri
        )
        mgr2 = enginefacade.transaction_context()
        mgr2.configure(
            connection=self.engine_uri,
            slave_connection=self.slave_uri
        )

        context = oslo_context.RequestContext()

        with mgr1.writer.using(context):
            self.assertIs(context.transaction_ctx.factory, mgr1._factory)
            self.assertIsNot(context.transaction_ctx.factory, mgr2._factory)

            with mgr2.writer.using(context):
                self.assertIsNot(
                    context.transaction_ctx.factory, mgr1._factory)
                self.assertIs(context.transaction_ctx.factory, mgr2._factory)
                self.assertIsNotNone(context.session)

            # mgr1 is restored
            self.assertIs(context.transaction_ctx.factory, mgr1._factory)
            self.assertIsNot(context.transaction_ctx.factory, mgr2._factory)
            self.assertIsNotNone(context.session)

        self.assertRaises(
            exception.NoEngineContextEstablished,
            getattr, context, 'transaction_ctx'
        )

    def test_context_found_for_bound_method(self):
        context = oslo_context.RequestContext()

        @enginefacade.reader
        def go(self, context):
            context.session.execute("test")
        go(self, context)

        with self._assert_engines() as engines:
            with self._assert_makers(engines) as makers:
                with self._assert_reader_session(makers) as session:
                    session.execute("test")

    def test_context_found_for_class_method(self):
        context = oslo_context.RequestContext()

        class Spam:
            @classmethod
            @enginefacade.reader
            def go(cls, context):
                context.session.execute("test")
        Spam.go(context)

        with self._assert_engines() as engines:
            with self._assert_makers(engines) as makers:
                with self._assert_reader_session(makers) as session:
                    session.execute("test")


class PatchFactoryTest(test_base.BaseTestCase):

    def test_patch_manager(self):
        normal_mgr = enginefacade.transaction_context()
        normal_mgr.configure(connection="sqlite:///foo.db")
        alt_mgr = enginefacade.transaction_context()
        alt_mgr.configure(connection="sqlite:///bar.db")

        @normal_mgr.writer
        def go1(context):
            s1 = context.session
            self.assertEqual(
                s1.bind.url, "sqlite:///foo.db")
            self.assertIs(
                s1.bind,
                normal_mgr._factory._writer_engine)

        @normal_mgr.writer
        def go2(context):
            s1 = context.session

            self.assertEqual(
                s1.bind.url,
                "sqlite:///bar.db")

            self.assertIs(
                normal_mgr._factory._writer_engine,
                alt_mgr._factory._writer_engine
            )

        def create_engine(sql_connection, **kw):
            return mock.Mock(url=sql_connection)

        with mock.patch(
                "oslo_db.sqlalchemy.engines.create_engine", create_engine):
            context = oslo_context.RequestContext()
            go1(context)
            reset = normal_mgr.patch_factory(alt_mgr)
            go2(context)
            reset()
            go1(context)

    def test_patch_factory(self):
        normal_mgr = enginefacade.transaction_context()
        normal_mgr.configure(connection="sqlite:///foo.db")
        alt_mgr = enginefacade.transaction_context()
        alt_mgr.configure(connection="sqlite:///bar.db")

        @normal_mgr.writer
        def go1(context):
            s1 = context.session
            self.assertEqual(
                s1.bind.url, "sqlite:///foo.db")
            self.assertIs(
                s1.bind,
                normal_mgr._factory._writer_engine)

        @normal_mgr.writer
        def go2(context):
            s1 = context.session

            self.assertEqual(
                s1.bind.url,
                "sqlite:///bar.db")

            self.assertIs(
                normal_mgr._factory._writer_engine,
                alt_mgr._factory._writer_engine
            )

        def create_engine(sql_connection, **kw):
            return mock.Mock(url=sql_connection)

        with mock.patch(
                "oslo_db.sqlalchemy.engines.create_engine", create_engine):
            context = oslo_context.RequestContext()
            go1(context)
            reset = normal_mgr.patch_factory(alt_mgr._factory)
            go2(context)
            reset()
            go1(context)

    def test_patch_engine(self):
        normal_mgr = enginefacade.transaction_context()
        normal_mgr.configure(
            connection="sqlite:///foo.db",
            rollback_reader_sessions=True
        )

        @normal_mgr.writer
        def go1(context):
            s1 = context.session
            self.assertEqual(
                s1.bind.url, "sqlite:///foo.db")
            self.assertIs(
                s1.bind,
                normal_mgr._factory._writer_engine)

        @normal_mgr.writer
        def go2(context):
            s1 = context.session

            self.assertEqual(
                s1.bind.url,
                "sqlite:///bar.db")

            self.assertTrue(
                enginefacade._transaction_ctx_for_context(
                    context).rollback_reader_sessions
            )

            # ensure this defaults to True
            self.assertTrue(
                enginefacade._transaction_ctx_for_context(
                    context).factory.synchronous_reader
            )

        def create_engine(sql_connection, **kw):
            return mock.Mock(url=sql_connection)

        with mock.patch(
                "oslo_db.sqlalchemy.engines.create_engine", create_engine):
            mock_engine = create_engine("sqlite:///bar.db")

            context = oslo_context.RequestContext()
            go1(context)
            reset = normal_mgr.patch_engine(mock_engine)
            go2(context)
            self.assertIs(
                normal_mgr._factory._writer_engine, mock_engine)
            reset()
            go1(context)

    def test_patch_not_started(self):
        normal_mgr = enginefacade.transaction_context()
        normal_mgr.configure(
            connection="sqlite:///foo.db",
            rollback_reader_sessions=True
        )

        @normal_mgr.writer
        def go1(context):
            s1 = context.session

            self.assertEqual(
                s1.bind.url,
                "sqlite:///bar.db")

            self.assertTrue(
                enginefacade._transaction_ctx_for_context(
                    context).rollback_reader_sessions
            )

        def create_engine(sql_connection, **kw):
            return mock.Mock(url=sql_connection)

        with mock.patch(
                "oslo_db.sqlalchemy.engines.create_engine", create_engine):
            mock_engine = create_engine("sqlite:///bar.db")

            context = oslo_context.RequestContext()
            reset = normal_mgr.patch_engine(mock_engine)
            go1(context)
            self.assertIs(
                normal_mgr._factory._writer_engine, mock_engine)
            reset()

    def test_new_manager_from_config(self):
        normal_mgr = enginefacade.transaction_context()
        normal_mgr.configure(
            connection="sqlite://",
            sqlite_fk=True,
            mysql_sql_mode="FOOBAR",
            max_overflow=38
        )

        normal_mgr._factory._start()

        copied_mgr = normal_mgr.make_new_manager()

        self.assertTrue(normal_mgr._factory._started)
        self.assertIsNotNone(normal_mgr._factory._writer_engine)

        self.assertIsNot(copied_mgr._factory, normal_mgr._factory)
        self.assertFalse(copied_mgr._factory._started)
        copied_mgr._factory._start()
        self.assertIsNot(
            normal_mgr._factory._writer_engine,
            copied_mgr._factory._writer_engine)

        engine_args = copied_mgr._factory._engine_args_for_conf(None)
        self.assertTrue(engine_args['sqlite_fk'])
        self.assertEqual("FOOBAR", engine_args["mysql_sql_mode"])
        self.assertEqual(38, engine_args["max_overflow"])
        self.assertNotIn("mysql_wsrep_sync_wait", engine_args)

    def test_new_manager_from_options(self):
        """test enginefacade's defaults given a default structure from opts"""

        factory = enginefacade._TransactionFactory()
        cfg.CONF.register_opts(options.database_opts, 'database')
        factory.configure(**dict(cfg.CONF.database.items()))
        engine_args = factory._engine_args_for_conf(None)

        self.assertEqual(None, engine_args["mysql_wsrep_sync_wait"])
        self.assertEqual(True, engine_args["sqlite_synchronous"])
        self.assertEqual("TRADITIONAL", engine_args["mysql_sql_mode"])


class SynchronousReaderWSlaveMockFacadeTest(MockFacadeTest):
    synchronous_reader = True

    engine_uri = 'some_connection'
    slave_uri = 'some_slave_connection'


class AsyncReaderWSlaveMockFacadeTest(MockFacadeTest):
    synchronous_reader = False

    engine_uri = 'some_connection'
    slave_uri = 'some_slave_connection'


class LegacyIntegrationtest(db_test_base._DbTestCase):

    def test_legacy_integration(self):
        legacy_facade = enginefacade.get_legacy_facade()
        self.assertTrue(
            legacy_facade.get_engine() is
            enginefacade._context_manager._factory._writer_engine
        )

        self.assertTrue(
            enginefacade.get_legacy_facade() is legacy_facade
        )

    def test_get_sessionmaker(self):
        legacy_facade = enginefacade.get_legacy_facade()
        self.assertTrue(
            legacy_facade.get_sessionmaker() is
            enginefacade._context_manager._factory._writer_maker
        )

    def test_legacy_facades_from_different_context_managers(self):
        transaction_context1 = enginefacade.transaction_context()
        transaction_context2 = enginefacade.transaction_context()

        transaction_context1.configure(connection='sqlite:///?conn1')
        transaction_context2.configure(connection='sqlite:///?conn2')

        legacy1 = transaction_context1.get_legacy_facade()
        legacy2 = transaction_context2.get_legacy_facade()

        self.assertNotEqual(legacy1, legacy2)

    def test_legacy_not_started(self):

        factory = enginefacade._TransactionFactory()

        self.assertRaises(
            exception.CantStartEngineError,
            factory.get_legacy_facade
        )

        legacy_facade = factory.get_legacy_facade()
        self.assertRaises(
            exception.CantStartEngineError,
            legacy_facade.get_session
        )

        self.assertRaises(
            exception.CantStartEngineError,
            legacy_facade.get_session
        )

        self.assertRaises(
            exception.CantStartEngineError,
            legacy_facade.get_engine
        )


class ThreadingTest(db_test_base._DbTestCase):
    """Test copy/pickle on new threads using real connections and sessions."""

    def _assert_ctx_connection(self, context, connection):
        self.assertIs(context.connection, connection)

    def _assert_ctx_session(self, context, session):
        self.assertIs(context.session, session)

    def _patch_thread_ident(self):
        self.ident = 1

        test_instance = self

        class MockThreadingLocal:
            def __init__(self):
                self.__dict__['state'] = collections.defaultdict(dict)

            def __deepcopy__(self, memo):
                return self

            def __getattr__(self, key):
                ns = self.state[test_instance.ident]
                try:
                    return ns[key]
                except KeyError:
                    raise AttributeError(key)

            def __setattr__(self, key, value):
                ns = self.state[test_instance.ident]
                ns[key] = value

            def __delattr__(self, key):
                ns = self.state[test_instance.ident]
                try:
                    del ns[key]
                except KeyError:
                    raise AttributeError(key)

        return mock.patch.object(
            enginefacade, "_TransactionContextTLocal", MockThreadingLocal)

    def test_thread_ctxmanager_writer(self):
        context = oslo_context.RequestContext()

        with self._patch_thread_ident():
            with enginefacade.writer.using(context) as session:
                self._assert_ctx_session(context, session)

                self.ident = 2

                with enginefacade.reader.using(context) as sess2:
                    # new session
                    self.assertIsNot(sess2, session)

                    # thread local shows the new session
                    self._assert_ctx_session(context, sess2)

                self.ident = 1

                with enginefacade.reader.using(context) as sess3:
                    self.assertIs(sess3, session)
                    self._assert_ctx_session(context, session)

    def test_thread_ctxmanager_writer_connection(self):
        context = oslo_context.RequestContext()

        with self._patch_thread_ident():
            with enginefacade.writer.connection.using(context) as conn:
                self._assert_ctx_connection(context, conn)

                self.ident = 2

                with enginefacade.reader.connection.using(context) as conn2:
                    # new connection
                    self.assertIsNot(conn2, conn)

                    # thread local shows the new connection
                    self._assert_ctx_connection(context, conn2)

                    with enginefacade.reader.connection.using(
                            context) as conn3:
                        # we still get the right connection even though
                        # this context is not the "copied" context
                        self.assertIsNot(conn3, conn)
                        self.assertIs(conn3, conn2)

                self.ident = 1

                with enginefacade.reader.connection.using(context) as conn3:
                    self.assertIs(conn3, conn)
                    self._assert_ctx_connection(context, conn)

    def test_thread_ctxmanager_switch_styles(self):

        @enginefacade.writer.connection
        def go_one(context):
            self.assertRaises(
                exception.ContextNotRequestedError,
                getattr, context, "session"
            )
            self.assertIsNotNone(context.connection)

            self.ident = 2
            go_two(context)

            self.ident = 1
            self.assertRaises(
                exception.ContextNotRequestedError,
                getattr, context, "session"
            )
            self.assertIsNotNone(context.connection)

        @enginefacade.reader
        def go_two(context):
            self.assertRaises(
                exception.ContextNotRequestedError,
                getattr, context, "connection"
            )
            self.assertIsNotNone(context.session)

        context = oslo_context.RequestContext()
        with self._patch_thread_ident():
            go_one(context)

    def test_thread_decorator_writer(self):
        sessions = set()

        @enginefacade.writer
        def go_one(context):
            sessions.add(context.session)

            self.ident = 2
            go_two(context)

            self.ident = 1

            go_three(context)

        @enginefacade.reader
        def go_two(context):
            assert context.session not in sessions

        @enginefacade.reader
        def go_three(context):
            assert context.session in sessions

        context = oslo_context.RequestContext()
        with self._patch_thread_ident():
            go_one(context)

    def test_thread_decorator_writer_connection(self):
        connections = set()

        @enginefacade.writer.connection
        def go_one(context):
            connections.add(context.connection)

            self.ident = 2
            go_two(context)

            self.ident = 1

            go_three(context)

        @enginefacade.reader.connection
        def go_two(context):
            assert context.connection not in connections

        @enginefacade.reader
        def go_three(context):
            assert context.connection in connections

        context = oslo_context.RequestContext()
        with self._patch_thread_ident():
            go_one(context)

    def test_contexts_picklable(self):
        context = oslo_context.RequestContext()

        with enginefacade.writer.using(context) as session:
            self._assert_ctx_session(context, session)

            pickled = pickle.dumps(context)

            unpickled = pickle.loads(pickled)

            with enginefacade.writer.using(unpickled) as session2:
                self._assert_ctx_session(unpickled, session2)

                assert session is not session2


class LiveFacadeTest(db_test_base._DbTestCase):
    """test using live SQL with test-provisioned databases.

    Several of these tests require that multiple transactions run
    simultaenously; as the default SQLite :memory: connection can't achieve
    this, opportunistic test implementations against MySQL and PostgreSQL are
    supplied.

    """

    def setUp(self):
        super().setUp()

        metadata = MetaData()
        user_table = Table(
            'user', metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(30)),
            Column('favorite_color', String(10), default='yellow'),
            mysql_engine='InnoDB'
        )
        self.user_table = user_table
        metadata.create_all(self.engine)
        self.addCleanup(metadata.drop_all, self.engine)

        reg = registry()

        class User:
            def __init__(self, name):
                self.name = name

        reg.map_imperatively(User, user_table)
        self.User = User

    def _assert_ctx_connection(self, context, connection):
        self.assertIs(context.connection, connection)

    def _assert_ctx_session(self, context, session):
        self.assertIs(context.session, session)

    def test_transaction_committed(self):
        context = oslo_context.RequestContext()

        with enginefacade.writer.using(context) as session:
            session.add(self.User(name="u1"))

        session = self.sessionmaker(autocommit=False)
        with session.begin():
            self.assertEqual(
                "u1",
                session.query(self.User.name).scalar()
            )

    def test_transaction_rollback(self):
        context = oslo_context.RequestContext()

        class MyException(Exception):
            pass

        @enginefacade.writer
        def go(context):
            context.session.add(self.User(name="u1"))
            context.session.flush()
            raise MyException("a test")

        self.assertRaises(MyException, go, context)

        session = self.sessionmaker(autocommit=False)
        with session.begin():
            self.assertEqual(
                None,
                session.query(self.User.name).scalar()
            )

    @mock.patch.object(Session, 'commit')
    @mock.patch.object(Session, 'rollback')
    def test_save_and_reraise_when_rollback_exception(self,
                                                      rollback_patch,
                                                      commit_patch):
        context = oslo_context.RequestContext()
        log = self.useFixture(fixtures.FakeLogger())

        class RollbackException(Exception):
            pass

        class CommitException(Exception):
            pass

        commit_patch.side_effect = CommitException()
        rollback_patch.side_effect = RollbackException()

        @enginefacade.writer
        def go_session(context):
            context.session.add(self.User(name="u1"))

        self.assertRaises(RollbackException, go_session, context)
        self.assertIn('CommitException', log.output)

    def test_flush_on_subtransaction(self):
        facade = enginefacade.transaction_context()
        facade.configure(
            connection=self.engine.url,
            flush_on_subtransaction=True)
        facade.patch_engine(self.engine)
        context = oslo_context.RequestContext()

        with facade.writer.using(context):
            with facade.writer.using(context):
                u = self.User(name="u1")
                context.session.add(u)
            self.assertEqual('yellow', u.favorite_color)

    def test_flush_on_subtransaction_default_off(self):
        context = oslo_context.RequestContext()
        facade = enginefacade.transaction_context()
        facade.configure(connection=self.engine.url)
        facade.patch_engine(self.engine)

        with facade.writer.using(context):
            with facade.writer.using(context):
                u = self.User(name="u1")
                context.session.add(u)
            self.assertIsNone(u.favorite_color)
        self.assertEqual('yellow', u.favorite_color)

    def test_context_deepcopy_on_session(self):
        context = oslo_context.RequestContext()
        with enginefacade.writer.using(context) as session:

            ctx2 = copy.deepcopy(context)
            self._assert_ctx_session(ctx2, session)

            with enginefacade.writer.using(ctx2) as s2:
                self.assertIs(session, s2)
                self._assert_ctx_session(ctx2, s2)

                s2.add(self.User(name="u1"))
                s2.flush()

        session = self.sessionmaker(autocommit=False)
        with session.begin():
            self.assertEqual(
                "u1",
                session.query(self.User.name).scalar()
            )

    def test_context_deepcopy_on_connection(self):
        context = oslo_context.RequestContext()
        with enginefacade.writer.connection.using(context) as conn:

            ctx2 = copy.deepcopy(context)
            self._assert_ctx_connection(ctx2, conn)

            with enginefacade.writer.connection.using(ctx2) as conn2:
                self.assertIs(conn, conn2)
                self._assert_ctx_connection(ctx2, conn2)

                conn2.execute(self.user_table.insert().values(name="u1"))

            self._assert_ctx_connection(ctx2, conn2)

        session = self.sessionmaker(autocommit=False)
        with session.begin():
            self.assertEqual(
                "u1",
                session.query(self.User.name).scalar()
            )

    @db_test_base.backend_specific("postgresql", "mysql")
    def test_external_session_transaction(self):
        context = oslo_context.RequestContext()
        with enginefacade.writer.using(context) as session:
            session.add(self.User(name="u1"))
            session.flush()

            with enginefacade.writer.independent.using(context) as s2:
                # transaction() uses a new session
                self.assertIsNot(s2, session)
                self._assert_ctx_session(context, s2)

                # rows within a distinct transaction
                s2.add(self.User(name="u2"))

                # it also takes over the global enginefacade
                # within the context
                with enginefacade.writer.using(context) as s3:
                    self.assertIs(s3, s2)
                    s3.add(self.User(name="u3"))

            self._assert_ctx_session(context, session)

            # rollback the "outer" transaction
            session.rollback()

            # add more state on the "outer" transaction
            session.begin()
            session.add(self.User(name="u4"))

        session = self.sessionmaker(autocommit=False)
        # inner transaction + second part of "outer" transaction were committed
        with session.begin():
            self.assertEqual(
                [("u2",), ("u3",), ("u4", )],
                session.query(
                    self.User.name).order_by(self.User.name).all()
            )

    def test_savepoint_transaction_decorator(self):
        context = oslo_context.RequestContext()

        @enginefacade.writer
        def go1(context):
            session = context.session
            session.add(self.User(name="u1"))
            session.flush()

            try:
                go2(context)
            except Exception:
                pass

            go3(context)

            session.add(self.User(name="u4"))

        @enginefacade.writer.savepoint
        def go2(context):
            session = context.session
            session.add(self.User(name="u2"))
            raise Exception("nope")

        @enginefacade.writer.savepoint
        def go3(context):
            session = context.session
            session.add(self.User(name="u3"))

        go1(context)

        session = self.sessionmaker(autocommit=False)
        # inner transaction + second part of "outer" transaction were committed
        with session.begin():
            self.assertEqual(
                [("u1",), ("u3",), ("u4", )],
                session.query(
                    self.User.name).order_by(self.User.name).all()
            )

    def test_savepoint_transaction(self):
        context = oslo_context.RequestContext()

        with enginefacade.writer.using(context) as session:
            session.add(self.User(name="u1"))
            session.flush()

            try:
                with enginefacade.writer.savepoint.using(context) as session:
                    session.add(self.User(name="u2"))
                    raise Exception("nope")
            except Exception:
                pass

            with enginefacade.writer.savepoint.using(context) as session:
                session.add(self.User(name="u3"))

            session.add(self.User(name="u4"))

        session = self.sessionmaker(autocommit=False)
        # inner transaction + second part of "outer" transaction were committed
        with session.begin():
            self.assertEqual(
                [("u1",), ("u3",), ("u4", )],
                session.query(
                    self.User.name).order_by(self.User.name).all()
            )

    @db_test_base.backend_specific("postgresql", "mysql")
    def test_external_session_transaction_decorator(self):
        context = oslo_context.RequestContext()

        @enginefacade.writer
        def go1(context):
            session = context.session
            session.add(self.User(name="u1"))
            session.flush()

            go2(context, session)

            self._assert_ctx_session(context, session)

            # rollback the "outer" transaction
            session.rollback()

            # add more state on the "outer" transaction
            session.begin()
            session.add(self.User(name="u4"))

        @enginefacade.writer.independent
        def go2(context, session):
            s2 = context.session
            # uses a new session
            self.assertIsNot(s2, session)
            self._assert_ctx_session(context, s2)

            # rows within a distinct transaction
            s2.add(self.User(name="u2"))

            # it also takes over the global enginefacade
            # within the context
            with enginefacade.writer.using(context) as s3:
                self.assertIs(s3, s2)
                s3.add(self.User(name="u3"))

        go1(context)

        session = self.sessionmaker(autocommit=False)
        # inner transaction + second part of "outer" transaction were committed
        with session.begin():
            self.assertEqual(
                [("u2",), ("u3",), ("u4", )],
                session.query(
                    self.User.name).order_by(self.User.name).all()
            )

    @db_test_base.backend_specific("postgresql", "mysql")
    def test_external_connection_transaction(self):
        context = oslo_context.RequestContext()
        with enginefacade.writer.connection.using(context) as connection:
            connection.execute(self.user_table.insert().values(name="u1"))

            # transaction() uses a new Connection
            with enginefacade.writer.independent.connection.\
                    using(context) as c2:
                self.assertIsNot(c2, connection)
                self._assert_ctx_connection(context, c2)

                # rows within a distinct transaction
                c2.execute(self.user_table.insert().values(name="u2"))

                # it also takes over the global enginefacade
                # within the context
                with enginefacade.writer.connection.using(context) as c3:
                    self.assertIs(c2, c3)
                    c3.execute(self.user_table.insert().values(name="u3"))
            self._assert_ctx_connection(context, connection)

            # rollback the "outer" transaction
            transaction_ctx = context.transaction_ctx
            transaction_ctx.transaction.rollback()
            transaction_ctx.transaction = connection.begin()

            # add more state on the "outer" transaction
            connection.execute(self.user_table.insert().values(name="u4"))

        session = self.sessionmaker(autocommit=False)
        with session.begin():
            self.assertEqual(
                [("u2",), ("u3",), ("u4", )],
                session.query(
                    self.User.name).order_by(self.User.name).all()
            )

    @db_test_base.backend_specific("postgresql", "mysql")
    def test_external_writer_in_reader(self):
        context = oslo_context.RequestContext()
        with enginefacade.reader.using(context) as session:
            ping = session.scalar(select(1))
            self.assertEqual(1, ping)

            # we're definitely a reader
            @enginefacade.writer
            def go(ctx):
                pass
            exc = self.assertRaises(TypeError, go, context)
            self.assertEqual(
                "Can't upgrade a READER transaction to a "
                "WRITER mid-transaction",
                exc.args[0])

            # but we can do a writer on a new transaction
            with enginefacade.writer.independent.using(context) as sess2:
                self.assertIsNot(sess2, session)
                self._assert_ctx_session(context, sess2)

                session.add(self.User(name="u1_nocommit"))
                sess2.add(self.User(name="u1_commit"))

            user = session.query(self.User).first()
            self.assertEqual("u1_commit", user.name)

        session = self.sessionmaker(autocommit=False)
        with session.begin():
            self.assertEqual(
                [("u1_commit",)],
                session.query(
                    self.User.name).order_by(self.User.name).all()
            )

    def test_replace_scope(self):
        # "timeout" is an argument accepted by
        # the pysqlite dialect, which we set here to ensure
        # that even in an all-sqlite test, we test that the URL
        # is different in the context we are looking for
        alt_connection = "sqlite:///?timeout=90"

        alt_mgr1 = enginefacade.transaction_context()
        alt_mgr1.configure(
            connection=alt_connection,
        )

        @enginefacade.writer
        def go1(context):
            s1 = context.session
            self.assertEqual(
                s1.bind.url,
                enginefacade._context_manager._factory._writer_engine.url)
            self.assertIs(
                s1.bind,
                enginefacade._context_manager._factory._writer_engine)
            self.assertEqual(s1.bind.url, self.engine.url)

            with alt_mgr1.replace.using(context):
                go2(context)

            go4(context)

        @enginefacade.writer
        def go2(context):
            s2 = context.session

            # factory is not replaced globally...
            self.assertIsNot(
                enginefacade._context_manager._factory._writer_engine,
                alt_mgr1._factory._writer_engine
            )

            # but it is replaced for us
            self.assertIs(s2.bind, alt_mgr1._factory._writer_engine)
            self.assertEqual(
                str(s2.bind.url), alt_connection)

            go3(context)

        @enginefacade.reader
        def go3(context):
            s3 = context.session

            # in a call of a call, we still have the alt URL
            self.assertIs(s3.bind, alt_mgr1._factory._writer_engine)
            self.assertEqual(
                str(s3.bind.url), alt_connection)

        @enginefacade.writer
        def go4(context):
            s4 = context.session

            # outside the "replace" context, all is back to normal
            self.assertIs(s4.bind, self.engine)
            self.assertEqual(
                s4.bind.url, self.engine.url)

        context = oslo_context.RequestContext()
        go1(context)
        self.assertIsNot(
            enginefacade._context_manager._factory._writer_engine,
            alt_mgr1._factory._writer_engine
        )

    def test_replace_scope_only_global_eng(self):
        # "timeout" is an argument accepted by
        # the pysqlite dialect, which we set here to ensure
        # that even in an all-sqlite test, we test that the URL
        # is different in the context we are looking for
        alt_connection1 = "sqlite:///?timeout=90"

        alt_mgr1 = enginefacade.transaction_context()
        alt_mgr1.configure(
            connection=alt_connection1,
        )

        alt_connection2 = "sqlite:///?timeout=120"

        alt_mgr2 = enginefacade.transaction_context()
        alt_mgr2.configure(
            connection=alt_connection2,
        )

        @enginefacade.writer
        def go1(context):
            s1 = context.session
            # global engine
            self.assertEqual(s1.bind.url, self.engine.url)

            # now replace global engine...
            with alt_mgr1.replace.using(context):
                go2(context)

            # and back
            go6(context)

        @enginefacade.writer
        def go2(context):
            s2 = context.session

            # we have the replace-the-global engine
            self.assertEqual(str(s2.bind.url), alt_connection1)
            self.assertIs(s2.bind, alt_mgr1._factory._writer_engine)

            go3(context)

        @alt_mgr2.writer
        def go3(context):
            s3 = context.session

            # we don't use the global engine in the first place.
            # make sure our own factory still used.
            self.assertEqual(str(s3.bind.url), alt_connection2)
            self.assertIs(s3.bind, alt_mgr2._factory._writer_engine)

            go4(context)

        @enginefacade.writer
        def go4(context):
            s4 = context.session

            # we *do* use the global, so we still want the replacement.
            self.assertEqual(str(s4.bind.url), alt_connection1)
            self.assertIs(s4.bind, alt_mgr1._factory._writer_engine)

        @enginefacade.writer
        def go5(context):
            s5 = context.session

            # ...and here also
            self.assertEqual(str(s5.bind.url), alt_connection1)
            self.assertIs(s5.bind, alt_mgr1._factory._writer_engine)

        @enginefacade.writer
        def go6(context):
            s6 = context.session

            # ...but not here!
            self.assertEqual(str(s6.bind.url), str(self.engine.url))
            self.assertIs(s6.bind, self.engine)

        context = oslo_context.RequestContext()
        go1(context)


class MySQLLiveFacadeTest(
    db_test_base._MySQLOpportunisticTestCase, LiveFacadeTest,
):
    pass


class PGLiveFacadeTest(
    db_test_base._PostgreSQLOpportunisticTestCase, LiveFacadeTest,
):
    pass


class ConfigOptionsTest(test_base.BaseTestCase):
    def test_all_options(self):
        """test that everything in CONF.database.iteritems() is accepted.

        There's a handful of options in oslo.db.options that seem to have
        no meaning, but need to be accepted.   In particular, Cinder and
        maybe others are doing exactly this call.

        """

        factory = enginefacade._TransactionFactory()
        cfg.CONF.register_opts(options.database_opts, 'database')
        factory.configure(**dict(cfg.CONF.database.items()))

    def test_options_not_supported(self):
        factory = enginefacade._TransactionFactory()

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")

            factory.configure(
                fake1='x', connection_recycle_time=200, wrong2='y')

        self.assertEqual(1, len(w))
        self.assertTrue(
            issubclass(w[-1].category, warning.NotSupportedWarning))
        self.assertEqual(
            "Configuration option(s) ['fake1', 'wrong2'] not supported",
            str(w[-1].message)
        )

    def test_no_engine(self):
        factory = enginefacade._TransactionFactory()

        self.assertRaises(
            exception.CantStartEngineError,
            factory._create_session, enginefacade._WRITER
        )

        self.assertRaises(
            exception.CantStartEngineError,
            factory._create_session, enginefacade._WRITER
        )


class TestTransactionFactoryCallback(test_base.BaseTestCase):

    def test_setup_for_connection_called_with_profiler(self):
        context_manager = enginefacade.transaction_context()
        context_manager.configure(connection='sqlite://')
        hook = mock.Mock()
        context_manager.append_on_engine_create(hook)
        self.assertEqual(
            [hook], context_manager._factory._facade_cfg['on_engine_create'])

        @context_manager.reader
        def go(context):
            hook.assert_called_once_with(context.session.bind)

        go(oslo_context.RequestContext())

# TODO(zzzeek): test configuration options, e.g. like
# test_sqlalchemy->test_creation_from_config
