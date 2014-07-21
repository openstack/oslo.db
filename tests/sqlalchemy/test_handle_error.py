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

"""Test the compatibility layer for the handle_error() event.

This event is added as of SQLAlchemy 0.9.7; oslo.db provides a compatibility
layer for prior SQLAlchemy versions.

"""
import contextlib

import mock
from oslotest import base as test_base
import sqlalchemy as sqla
from sqlalchemy.sql import column
from sqlalchemy.sql import literal
from sqlalchemy.sql import select
from sqlalchemy.types import Integer
from sqlalchemy.types import TypeDecorator

from oslo.db.sqlalchemy.compat import handle_error


class MyException(Exception):
    pass


class ExceptionReraiseTest(test_base.BaseTestCase):

    def setUp(self):
        super(ExceptionReraiseTest, self).setUp()

        self.engine = engine = sqla.create_engine("sqlite://")
        self.addCleanup(engine.dispose)

    def _fixture(self):
        engine = self.engine

        def err(context):
            if "ERROR ONE" in str(context.statement):
                raise MyException("my exception")
        handle_error(engine, err)

    def test_exception_event_altered(self):
        self._fixture()

        with mock.patch.object(self.engine.dialect.execution_ctx_cls,
            "handle_dbapi_exception") as patched:

            matchee = self.assertRaises(
                MyException,
                self.engine.execute, "SELECT 'ERROR ONE' FROM I_DONT_EXIST"
            )
            self.assertEqual(1, patched.call_count)
            self.assertEqual("my exception", matchee.message)

    def test_exception_event_non_altered(self):
        self._fixture()

        with mock.patch.object(self.engine.dialect.execution_ctx_cls,
            "handle_dbapi_exception") as patched:

            self.assertRaises(
                sqla.exc.DBAPIError,
                self.engine.execute, "SELECT 'ERROR TWO' FROM I_DONT_EXIST"
            )
            self.assertEqual(1, patched.call_count)

    def test_is_disconnect_not_interrupted(self):
        self._fixture()

        with contextlib.nested(
            mock.patch.object(
                self.engine.dialect.execution_ctx_cls,
                "handle_dbapi_exception"
            ),
            mock.patch.object(
                self.engine.dialect, "is_disconnect",
                lambda *args: True
            )
        ) as (handle_dbapi_exception, is_disconnect):
            with self.engine.connect() as conn:
                self.assertRaises(
                    MyException,
                    conn.execute, "SELECT 'ERROR ONE' FROM I_DONT_EXIST"
                )
                self.assertEqual(1, handle_dbapi_exception.call_count)
                self.assertTrue(conn.invalidated)

    def test_no_is_disconnect_not_invalidated(self):
        self._fixture()

        with contextlib.nested(
            mock.patch.object(
                self.engine.dialect.execution_ctx_cls,
                "handle_dbapi_exception"
            ),
            mock.patch.object(
                self.engine.dialect, "is_disconnect",
                lambda *args: False
            )
        ) as (handle_dbapi_exception, is_disconnect):
            with self.engine.connect() as conn:
                self.assertRaises(
                    MyException,
                    conn.execute, "SELECT 'ERROR ONE' FROM I_DONT_EXIST"
                )
                self.assertEqual(1, handle_dbapi_exception.call_count)
                self.assertFalse(conn.invalidated)

    def test_exception_event_ad_hoc_context(self):
        engine = self.engine

        nope = MyException("nope")

        class MyType(TypeDecorator):
            impl = Integer

            def process_bind_param(self, value, dialect):
                raise nope

        listener = mock.Mock(return_value=None)
        handle_error(engine, listener)

        self.assertRaises(
            sqla.exc.StatementError,
            engine.execute,
            select([1]).where(column('foo') == literal('bar', MyType))
        )

        ctx = listener.mock_calls[0][1][0]
        self.assertTrue(ctx.statement.startswith("SELECT 1 "))
        self.assertIs(ctx.is_disconnect, False)
        self.assertIs(ctx.original_exception, nope)
