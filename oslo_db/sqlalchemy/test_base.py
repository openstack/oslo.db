# Copyright (c) 2013 OpenStack Foundation
# All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import functools

import debtcollector
import debtcollector.moves
import fixtures
import testresources

try:
    from oslotest import base as test_base
except ImportError:
    raise NameError('Oslotest is not installed. Please add oslotest in your'
                    ' test-requirements')


from oslo_utils import reflection

from oslo_db import exception
from oslo_db.sqlalchemy import enginefacade
from oslo_db.sqlalchemy import provision
from oslo_db.sqlalchemy import session


@debtcollector.removals.removed_class(
    "DbFixture",
    message="Please use oslo_db.sqlalchemy.test_fixtures directly")
class DbFixture(fixtures.Fixture):
    """Basic database fixture.

    Allows to run tests on various db backends, such as SQLite, MySQL and
    PostgreSQL. By default use sqlite backend. To override default backend
    uri set env variable OS_TEST_DBAPI_ADMIN_CONNECTION with database admin
    credentials for specific backend.
    """

    DRIVER = "sqlite"

    # these names are deprecated, and are not used by DbFixture.
    # they are here for backwards compatibility with test suites that
    # are referring to them directly.
    DBNAME = PASSWORD = USERNAME = 'openstack_citest'

    def __init__(self, test, skip_on_unavailable_db=True):
        super(DbFixture, self).__init__()
        self.test = test
        self.skip_on_unavailable_db = skip_on_unavailable_db

    def setUp(self):
        super(DbFixture, self).setUp()

        testresources.setUpResources(
            self.test, self.test.resources, testresources._get_result())
        self.addCleanup(
            testresources.tearDownResources,
            self.test, self.test.resources, testresources._get_result()
        )

        if not self.test._has_db_resource():
            msg = self.test._get_db_resource_not_available_reason()
            if self.test.SKIP_ON_UNAVAILABLE_DB:
                self.test.skipTest(msg)
            else:
                self.test.fail(msg)

        if self.test.SCHEMA_SCOPE:
            self.test.engine = self.test.transaction_engine
            self.test.sessionmaker = session.get_maker(
                self.test.transaction_engine)
        else:
            self.test.engine = self.test.db.engine
            self.test.sessionmaker = session.get_maker(self.test.engine)

        self.addCleanup(setattr, self.test, 'sessionmaker', None)
        self.addCleanup(setattr, self.test, 'engine', None)

        self.test.enginefacade = enginefacade._TestTransactionFactory(
            self.test.engine, self.test.sessionmaker, apply_global=True)
        self.addCleanup(self.test.enginefacade.dispose_global)


@debtcollector.removals.removed_class(
    "DbTestCase",
    message="Please use oslo_db.sqlalchemy.test_fixtures directly")
class DbTestCase(test_base.BaseTestCase):
    """Base class for testing of DB code.

    """

    FIXTURE = DbFixture
    SCHEMA_SCOPE = None
    SKIP_ON_UNAVAILABLE_DB = True

    _db_not_available = {}
    _schema_resources = {}
    _database_resources = {}

    def _get_db_resource_not_available_reason(self):
        return self._db_not_available.get(self.FIXTURE.DRIVER, None)

    def _has_db_resource(self):
        return self._database_resources.get(
            self.FIXTURE.DRIVER, None) is not None

    def _resources_for_driver(self, driver, schema_scope, generate_schema):
        # testresources relies on the identity and state of the
        # TestResourceManager objects in play to correctly manage
        # resources, and it also hardcodes to looking at the
        # ".resources" attribute on the test object, even though the
        # setUpResources() function passes the list of resources in,
        # so we have to code the TestResourceManager logic into the
        # .resources attribute and ensure that the same set of test
        # variables always produces the same TestResourceManager objects.

        if driver not in self._database_resources:
            try:
                self._database_resources[driver] = \
                    provision.DatabaseResource(driver,
                                               provision_new_database=True)
            except exception.BackendNotAvailable as bne:
                self._database_resources[driver] = None
                self._db_not_available[driver] = str(bne)

        database_resource = self._database_resources[driver]
        if database_resource is None:
            return []

        if schema_scope:
            key = (driver, schema_scope)
            if key not in self._schema_resources:
                schema_resource = provision.SchemaResource(
                    database_resource, generate_schema)

                transaction_resource = provision.TransactionResource(
                    database_resource, schema_resource)

                self._schema_resources[key] = \
                    transaction_resource

            transaction_resource = self._schema_resources[key]

            return [
                ('transaction_engine', transaction_resource),
                ('db', database_resource),
            ]
        else:
            key = (driver, None)
            if key not in self._schema_resources:
                self._schema_resources[key] = provision.SchemaResource(
                    database_resource, generate_schema, teardown=True)

            schema_resource = self._schema_resources[key]
            return [
                ('schema', schema_resource),
                ('db', database_resource)
            ]

    @property
    def resources(self):
        return self._resources_for_driver(
            self.FIXTURE.DRIVER, self.SCHEMA_SCOPE, self.generate_schema)

    def setUp(self):
        super(DbTestCase, self).setUp()
        self.useFixture(
            self.FIXTURE(
                self, skip_on_unavailable_db=self.SKIP_ON_UNAVAILABLE_DB))

    def generate_schema(self, engine):
        """Generate schema objects to be used within a test.

        The function is separate from the setUp() case as the scope
        of this method is controlled by the provisioning system.  A
        test that specifies SCHEMA_SCOPE may not call this method
        for each test, as the schema may be maintained from a previous run.

        """
        if self.SCHEMA_SCOPE:
            # if SCHEMA_SCOPE is set, then this method definitely
            # has to be implemented.  This is a guard against a test
            # that inadvertently does schema setup within setUp().
            raise NotImplementedError(
                "This test requires schema-level setup to be "
                "implemented within generate_schema().")


@debtcollector.removals.removed_class("OpportunisticTestCase")
class OpportunisticTestCase(DbTestCase):
    """Placeholder for backwards compatibility."""


ALLOWED_DIALECTS = ['sqlite', 'mysql', 'postgresql']


def backend_specific(*dialects):
    """Decorator to skip backend specific tests on inappropriate engines.

    ::dialects: list of dialects names under which the test will be launched.
    """
    def wrap(f):
        @functools.wraps(f)
        def ins_wrap(self):
            if not set(dialects).issubset(ALLOWED_DIALECTS):
                raise ValueError(
                    "Please use allowed dialects: %s" % ALLOWED_DIALECTS)
            if self.engine.name not in dialects:
                msg = ('The test "%s" can be run '
                       'only on %s. Current engine is %s.')
                args = (reflection.get_callable_name(f), ', '.join(dialects),
                        self.engine.name)
                self.skipTest(msg % args)
            else:
                return f(self)
        return ins_wrap
    return wrap


@debtcollector.removals.removed_class("MySQLOpportunisticFixture")
class MySQLOpportunisticFixture(DbFixture):
    DRIVER = 'mysql'


@debtcollector.removals.removed_class("PostgreSQLOpportunisticFixture")
class PostgreSQLOpportunisticFixture(DbFixture):
    DRIVER = 'postgresql'


@debtcollector.removals.removed_class("MySQLOpportunisticTestCase")
class MySQLOpportunisticTestCase(OpportunisticTestCase):
    FIXTURE = MySQLOpportunisticFixture


@debtcollector.removals.removed_class("PostgreSQLOpportunisticTestCase")
class PostgreSQLOpportunisticTestCase(OpportunisticTestCase):
    FIXTURE = PostgreSQLOpportunisticFixture
