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

This event is added as of SQLAlchemy 0.9.7; oslo_db provides a compatibility
layer for prior SQLAlchemy versions.

"""

import mock
from oslotest import base as test_base
import sqlalchemy as sqla
from sqlalchemy.sql import column
from sqlalchemy.sql import literal
from sqlalchemy.sql import select
from sqlalchemy.types import Integer
from sqlalchemy.types import TypeDecorator

from oslo_db.sqlalchemy.compat import handle_error
from oslo_db.sqlalchemy.compat import utils
from oslo_db.tests import utils as test_utils


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
            self.assertEqual("my exception", matchee.args[0])

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

        with test_utils.nested(
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

        with test_utils.nested(
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

    def _test_alter_disconnect(self, orig_error, evt_value):
        engine = self.engine

        def evt(ctx):
            ctx.is_disconnect = evt_value
        handle_error(engine, evt)

        # if we are under sqla 0.9.7, and we are expecting to take
        # an "is disconnect" exception and make it not a disconnect,
        # that isn't supported b.c. the wrapped handler has already
        # done the invalidation.
        expect_failure = not utils.sqla_097 and orig_error and not evt_value

        with mock.patch.object(engine.dialect,
                               "is_disconnect",
                               mock.Mock(return_value=orig_error)):

            with engine.connect() as c:
                conn_rec = c.connection._connection_record
                try:
                    c.execute("SELECT x FROM nonexistent")
                    assert False
                except sqla.exc.StatementError as st:
                    self.assertFalse(expect_failure)

                    # check the exception's invalidation flag
                    self.assertEqual(st.connection_invalidated, evt_value)

                    # check the Connection object's invalidation flag
                    self.assertEqual(c.invalidated, evt_value)

                    # this is the ConnectionRecord object; it's invalidated
                    # when its .connection member is None
                    self.assertEqual(conn_rec.connection is None, evt_value)

                except NotImplementedError as ne:
                    self.assertTrue(expect_failure)
                    self.assertEqual(
                        str(ne),
                        "Can't reset 'disconnect' status of exception once it "
                        "is set with this version of SQLAlchemy")

    def test_alter_disconnect_to_true(self):
        self._test_alter_disconnect(False, True)
        self._test_alter_disconnect(True, True)

    def test_alter_disconnect_to_false(self):
        self._test_alter_disconnect(True, False)
        self._test_alter_disconnect(False, False)
