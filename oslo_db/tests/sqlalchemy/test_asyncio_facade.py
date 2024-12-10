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

from oslo_context import context as oslo_context
from oslo_db.sqlalchemy import asyncio_facade
from oslo_db.tests import base as test_base
from sqlalchemy import text

asyncio_facade.async_transaction_context_provider(oslo_context.RequestContext)


class AsyncioFacadeTest(test_base.BaseAsyncioCase):

    def setUp(self):
        super().setUp()
        asyncio_facade.configure(
            connection="sqlite+aiosqlite://",
        )

    def tearDown(self):
        asyncio_facade._async_context_manager._root_factory = (
            asyncio_facade._async_context_manager._create_root_factory()
        )
        super().tearDown()

    async def test_contextmanager_session(self):
        context = oslo_context.RequestContext()
        async with asyncio_facade.reader.using(context) as session:
            result = await session.execute(text("select 1"))
            self.assertEqual(result.all(), [(1,)])

    async def test_contextmanager_connection(self):
        context = oslo_context.RequestContext()
        async with asyncio_facade.reader.connection.using(
            context
        ) as connection:
            result = await connection.execute(text("select 1"))
            self.assertEqual(result.all(), [(1,)])

    async def test_callable_session(self):
        context = oslo_context.RequestContext()

        @asyncio_facade.reader
        async def select_one(context):
            result = await context.async_session.execute(text("select 1"))
            self.assertEqual(result.all(), [(1,)])

        await select_one(context)

    async def test_callable_connection(self):
        context = oslo_context.RequestContext()

        @asyncio_facade.reader.connection
        async def select_one(context):
            result = await context.async_connection.execute(text("select 1"))
            self.assertEqual(result.all(), [(1,)])

        await select_one(context)
