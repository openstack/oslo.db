# Copyright (c) 2016 Openstack Foundation
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

import debtcollector

from oslo_db.sqlalchemy import enginefacade
from oslo_db.sqlalchemy.test_base import backend_specific  # noqa
from oslo_db.sqlalchemy import test_fixtures as db_fixtures
from oslotest import base as test_base


@enginefacade.transaction_context_provider
class Context(object):
    pass

context = Context()


@debtcollector.removals.removed_class(
    "DbTestCase",
    message="Do not import from oslo_db.tests!  "
            "Please use oslo_db.sqlalchemy.test_fixtures directly")
class DbTestCase(db_fixtures.OpportunisticDBTestMixin, test_base.BaseTestCase):

    def setUp(self):
        super(DbTestCase, self).setUp()

        self.engine = enginefacade.writer.get_engine()
        self.sessionmaker = enginefacade.writer.get_sessionmaker()


@debtcollector.removals.removed_class(
    "MySQLOpportunisticTestCase",
    message="Do not import from oslo_db.tests!  "
            "Please use oslo_db.sqlalchemy.test_fixtures directly")
class MySQLOpportunisticTestCase(DbTestCase):
    FIXTURE = db_fixtures.MySQLOpportunisticFixture


@debtcollector.removals.removed_class(
    "PostgreSQLOpportunisticTestCase",
    message="Do not import from oslo_db.tests!  "
            "Please use oslo_db.sqlalchemy.test_fixtures directly")
class PostgreSQLOpportunisticTestCase(DbTestCase):
    FIXTURE = db_fixtures.PostgresqlOpportunisticFixture


# NOTE (zzzeek) These test classes are **private to oslo.db**.  Please
# make use of oslo_db.sqlalchemy.test_fixtures directly.

class _DbTestCase(
        db_fixtures.OpportunisticDBTestMixin, test_base.BaseTestCase):

    def setUp(self):
        super(_DbTestCase, self).setUp()

        self.engine = enginefacade.writer.get_engine()
        self.sessionmaker = enginefacade.writer.get_sessionmaker()


class _MySQLOpportunisticTestCase(_DbTestCase):
    FIXTURE = db_fixtures.MySQLOpportunisticFixture


class _PostgreSQLOpportunisticTestCase(_DbTestCase):
    FIXTURE = db_fixtures.PostgresqlOpportunisticFixture
