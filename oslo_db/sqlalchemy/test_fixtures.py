# Copyright (c) 2016 OpenStack Foundation
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

import fixtures
import os
import testresources
import testscenarios

from oslo_db import exception
from oslo_db.sqlalchemy import enginefacade
from oslo_db.sqlalchemy import provision
from oslo_db.sqlalchemy import utils


class ReplaceEngineFacadeFixture(fixtures.Fixture):
    """A fixture that will plug the engine of one enginefacade into another.

    This fixture can be used by test suites that already have their own non-
    oslo_db database setup / teardown schemes, to plug any URL or test-oriented
    enginefacade as-is into an enginefacade-oriented API.

    For applications that use oslo.db's testing fixtures, the
    ReplaceEngineFacade fixture is used internally.

    E.g.::

        class MyDBTest(TestCase):

            def setUp(self):
                from myapplication.api import main_enginefacade

                my_test_enginefacade = enginefacade.transaction_context()
                my_test_enginefacade.configure(connection=my_test_url)

                self.useFixture(
                    ReplaceEngineFacadeFixture(
                        main_enginefacade, my_test_enginefacade))

    Above, the main_enginefacade object is the normal application level
    one, and my_test_enginefacade is a local one that we've created to
    refer to some testing database.   Throughout the fixture's setup,
    the application level enginefacade will use the engine factory and
    engines of the testing enginefacade, and at fixture teardown will be
    replaced back.

    """
    def __init__(self, enginefacade, replace_with_enginefacade):
        super().__init__()
        self.enginefacade = enginefacade
        self.replace_with_enginefacade = replace_with_enginefacade

    def _setUp(self):
        _reset_facade = self.enginefacade.patch_factory(
            self.replace_with_enginefacade._factory
        )
        self.addCleanup(_reset_facade)


class BaseDbFixture(fixtures.Fixture):
    """Base database provisioning fixture.

    This serves as the base class for the other fixtures, but by itself
    does not implement _setUp(). It provides the basis for the flags
    implemented by the various capability mixins (GenerateSchema,
    DeletesFromSchema, etc.) as well as providing an abstraction over
    the provisioning objects, which are specific to testresources.
    Overall, consumers of this fixture just need to use the right classes
    and the testresources mechanics are taken care of.

    """
    DRIVER = "sqlite"

    _DROP_SCHEMA_PER_TEST = True
    _BUILD_SCHEMA = False
    _BUILD_WITH_MIGRATIONS = False

    _database_resources = {}
    _db_not_available = {}
    _schema_resources = {}

    def __init__(self, driver=None, ident=None):
        super().__init__()
        self.driver = driver or self.DRIVER
        self.ident = ident or "default"
        self.resource_key = (self.driver, self.__class__, self.ident)

    def get_enginefacade(self):
        """Return an enginefacade._TransactionContextManager.

        This is typically a global variable like "context_manager" declared
        in the db/api.py module and is the object returned by
        enginefacade.transaction_context().

        If left not implemented, the global enginefacade manager is used.

        For the case where a project uses per-object or per-test enginefacades
        like Gnocchi, the get_per_test_enginefacade()
        method should also be implemented.


        """
        return enginefacade._context_manager

    def get_per_test_enginefacade(self):
        """Return an enginefacade._TransactionContextManager per test.

        This facade should be the one that the test expects the code to
        use.   Usually this is the same one returned by get_engineafacade()
        which is the default.  For special applications like Gnocchi,
        this can be overridden to provide an instance-level facade.

        """
        return self.get_enginefacade()

    def _get_db_resource_not_available_reason(self):
        return self._db_not_available.get(self.resource_key, None)

    def _has_db_resource(self):
        return self._database_resources.get(
            self.resource_key, None) is not None

    def _generate_schema_resource(self, database_resource):
        return provision.SchemaResource(
            database_resource,
            None if not self._BUILD_SCHEMA
            else self.generate_schema_create_all
            if not self._BUILD_WITH_MIGRATIONS
            else self.generate_schema_migrations,
            self._DROP_SCHEMA_PER_TEST
        )

    def _get_resources(self):
        key = self.resource_key

        # the DatabaseResource and SchemaResource provision objects
        # can be used by testresources as a marker outside of an individual
        # test to indicate that this database / schema can be used across
        # multiple tests.   To make this work, many instances of this
        # fixture have to return the *same* resource object given the same
        # inputs.  so we cache these in class-level dictionaries.

        if key not in self._database_resources:
            _enginefacade = self.get_enginefacade()
            try:
                self._database_resources[key] = \
                    self._generate_database_resource(_enginefacade)
            except exception.BackendNotAvailable as bne:
                self._database_resources[key] = None
                self._db_not_available[key] = str(bne)

        database_resource = self._database_resources[key]

        if database_resource is None:
            return []
        else:
            if key in self._schema_resources:
                schema_resource = self._schema_resources[key]
            else:
                schema_resource = self._schema_resources[key] = \
                    self._generate_schema_resource(database_resource)

            return [
                ('_schema_%s' % self.ident, schema_resource),
                ('_db_%s' % self.ident, database_resource)
            ]


class GeneratesSchema:
    """Mixin defining a fixture as generating a schema using create_all().

    This is a "capability" mixin that works in conjunction with classes
    that include BaseDbFixture as a base.

    """

    _BUILD_SCHEMA = True
    _BUILD_WITH_MIGRATIONS = False

    def generate_schema_create_all(self, engine):
        """A hook which should generate the model schema using create_all().

        This hook is called within the scope of creating the database
        assuming BUILD_WITH_MIGRATIONS is False.

        """


class GeneratesSchemaFromMigrations(GeneratesSchema):
    """Mixin defining a fixture as generating a schema using migrations.

    This is a "capability" mixin that works in conjunction with classes
    that include BaseDbFixture as a base.

    """

    _BUILD_WITH_MIGRATIONS = True

    def generate_schema_migrations(self, engine):
        """A hook which should generate the model schema using migrations.


        This hook is called within the scope of creating the database
        assuming BUILD_WITH_MIGRATIONS is True.

        """


class ResetsData:
    """Mixin defining a fixture that resets schema data without dropping."""

    _DROP_SCHEMA_PER_TEST = False

    def setup_for_reset(self, engine, enginefacade):
        """"Perform setup that may be needed before the test runs."""

    def reset_schema_data(self, engine, enginefacade):
        """Reset the data in the schema."""


class DeletesFromSchema(ResetsData):
    """Mixin defining a fixture that can delete from all tables in place.

    When DeletesFromSchema is present in a fixture,
    _DROP_SCHEMA_PER_TEST is now False; this means that the
    "teardown" flag of provision.SchemaResource will be False, which
    prevents SchemaResource from dropping all objects within the schema
    after each test.

    This is a "capability" mixin that works in conjunction with classes
    that include BaseDbFixture as a base.

    """

    def reset_schema_data(self, engine, facade):
        self.delete_from_schema(engine)

    def delete_from_schema(self, engine):
        """A hook which should delete all data from an existing schema.

        Should *not* drop any objects, just remove data from tables
        that needs to be reset between tests.
        """


class SimpleDbFixture(BaseDbFixture):
    """Fixture which provides an engine from a fixed URL.

    The SimpleDbFixture is generally appropriate only for a SQLite memory
    database, as this database is naturally isolated from other processes and
    does not require management of schemas.   For tests that need to
    run specifically against MySQL or Postgresql, the OpportunisticDbFixture
    is more appropriate.

    The database connection information itself comes from the provisoning
    system, matching the desired driver (typically sqlite) to the default URL
    that provisioning provides for this driver (in the case of sqlite, it's
    the SQLite memory URL, e.g. sqlite://.  For MySQL and Postgresql, it's
    the familiar "openstack_citest" URL on localhost).

    There are a variety of create/drop schemes that can take place:

    * The default is to procure a database connection on setup,
      and at teardown, an instruction is issued to "drop" all
      objects in the schema (e.g. tables, indexes).  The SQLAlchemy
      engine itself remains referenced at the class level for subsequent
      re-use.

    * When the GeneratesSchema or GeneratesSchemaFromMigrations mixins
      are implemented, the appropriate generate_schema method is also
      called when the fixture is set up, by default this is per test.

    * When the DeletesFromSchema mixin is implemented, the generate_schema
      method is now only called **once**, and the "drop all objects"
      system is replaced with the delete_from_schema method.   This
      allows the same database to remain set up with all schema objects
      intact, so that expensive migrations need not be run on every test.

    * The fixture does **not** dispose the engine at the end of a test.
      It is assumed the same engine will be re-used many times across
      many tests.  The AdHocDbFixture extends this one to provide
      engine.dispose() at the end of a test.

    This fixture is intended to work without needing a reference to
    the test itself, and therefore cannot take advantage of the
    OptimisingTestSuite.

    """

    _dependency_resources = {}

    def _get_provisioned_db(self):
        return self._dependency_resources["_db_%s" % self.ident]

    def _generate_database_resource(self, _enginefacade):
        return provision.DatabaseResource(self.driver, _enginefacade,
                                          provision_new_database=False)

    def _setUp(self):
        super()._setUp()

        cls = self.__class__

        if "_db_%s" % self.ident not in cls._dependency_resources:

            resources = self._get_resources()

            # initialize resources the same way that testresources does.
            for name, resource in resources:
                cls._dependency_resources[name] = resource.getResource()

        provisioned_db = self._get_provisioned_db()

        if not self._DROP_SCHEMA_PER_TEST:
            self.setup_for_reset(
                provisioned_db.engine, provisioned_db.enginefacade)

        self.useFixture(ReplaceEngineFacadeFixture(
            self.get_per_test_enginefacade(),
            provisioned_db.enginefacade
            ))

        if not self._DROP_SCHEMA_PER_TEST:
            self.addCleanup(
                self.reset_schema_data,
                provisioned_db.engine, provisioned_db.enginefacade)

        self.addCleanup(self._cleanup)

    def _teardown_resources(self):
        for name, resource in self._get_resources():
            dep = self._dependency_resources.pop(name)
            resource.finishedWith(dep)

    def _cleanup(self):
        pass


class AdHocDbFixture(SimpleDbFixture):
    """"Fixture which creates and disposes a database engine per test.

    Also allows a specific URL to be passed, meaning the fixture can
    be hardcoded to a specific SQLite file.

    For a SQLite, this fixture will create the named database upon setup
    and tear it down upon teardown.   For other databases, the
    database is assumed to exist already and will remain after teardown.

    """
    def __init__(self, url=None):
        if url:
            self.url = utils.make_url(url)
            driver = self.url.get_backend_name()
        else:
            driver = None
            self.url = None

        BaseDbFixture.__init__(
            self, driver=driver,
            ident=provision._random_ident())
        self.url = url

    def _generate_database_resource(self, _enginefacade):
        return provision.DatabaseResource(
            self.driver, _enginefacade, ad_hoc_url=self.url,
            provision_new_database=False)

    def _cleanup(self):
        self._teardown_resources()


class OpportunisticDbFixture(BaseDbFixture):
    """Fixture which uses testresources fully for optimised runs.

    This fixture relies upon the use of the OpportunisticDBTestMixin to supply
    a test.resources attribute, and also works much more effectively when
    combined the testresources.OptimisingTestSuite.   The
    optimize_package_test_loader() function should be
    used at the module and package levels to optimize database
    provisioning across many tests.

    """
    def __init__(self, test, driver=None, ident=None):
        super().__init__(
            driver=driver, ident=ident)
        self.test = test

    def _get_provisioned_db(self):
        return getattr(self.test, "_db_%s" % self.ident)

    def _generate_database_resource(self, _enginefacade):
        return provision.DatabaseResource(
            self.driver, _enginefacade, provision_new_database=True)

    def _setUp(self):
        super()._setUp()

        if not self._has_db_resource():
            return

        provisioned_db = self._get_provisioned_db()

        if not self._DROP_SCHEMA_PER_TEST:
            self.setup_for_reset(
                provisioned_db.engine, provisioned_db.enginefacade)

        self.useFixture(ReplaceEngineFacadeFixture(
            self.get_per_test_enginefacade(),
            provisioned_db.enginefacade
            ))

        if not self._DROP_SCHEMA_PER_TEST:
            self.addCleanup(
                self.reset_schema_data,
                provisioned_db.engine, provisioned_db.enginefacade)


class OpportunisticDBTestMixin:
    """Test mixin that integrates the test suite with testresources.

    There are three goals to this system:

    1. Allow creation of "stub" test suites that will run all the tests    in a
       parent suite against a specific    kind of database (e.g. Mysql,
       Postgresql), where the entire suite will be skipped if that    target
       kind of database is not available to the suite.

    2. provide a test with a process-local, anonymously named schema within a
       target database, so that the test can run concurrently with other tests
       without conflicting data

    3. provide compatibility with the testresources.OptimisingTestSuite, which
       organizes TestCase instances ahead of time into groups that all
       make use of the same type of database, setting up and tearing down
       a database schema once for the scope of any number of tests within.
       This technique is essential when testing against a non-SQLite database
       because building of a schema is expensive, and also is most ideally
       accomplished using the applications schema migration which are
       even more vastly slow than a straight create_all().

    This mixin provides the .resources attribute required by testresources when
    using the OptimisingTestSuite.The .resources attribute then provides a
    collection of testresources.TestResourceManager objects, which are defined
    here in oslo_db.sqlalchemy.provision.   These objects know how to find
    available database backends, build up temporary databases, and invoke
    schema generation and teardown instructions.   The actual "build the schema
    objects" part of the equation, and optionally a "delete from all the
    tables" step, is provided by the implementing application itself.


    """
    SKIP_ON_UNAVAILABLE_DB = True

    FIXTURE = OpportunisticDbFixture

    _collected_resources = None
    _instantiated_fixtures = None

    @property
    def resources(self):
        """Provide a collection of TestResourceManager objects.

        The collection here is memoized, both at the level of the test
        case itself, as well as in the fixture object(s) which provide
        those resources.

        """

        if self._collected_resources is not None:
            return self._collected_resources

        fixtures = self._instantiate_fixtures()
        self._collected_resources = []
        for fixture in fixtures:
            self._collected_resources.extend(fixture._get_resources())
        return self._collected_resources

    def setUp(self):
        self._setup_fixtures()
        super().setUp()

    def _get_default_provisioned_db(self):
        return self._db_default

    def _instantiate_fixtures(self):
        if self._instantiated_fixtures:
            return self._instantiated_fixtures

        self._instantiated_fixtures = utils.to_list(self.generate_fixtures())
        return self._instantiated_fixtures

    def generate_fixtures(self):
        return self.FIXTURE(test=self)

    def _setup_fixtures(self):
        testresources.setUpResources(
            self, self.resources, testresources._get_result())
        self.addCleanup(
            testresources.tearDownResources,
            self, self.resources, testresources._get_result()
        )

        fixtures = self._instantiate_fixtures()
        for fixture in fixtures:
            self.useFixture(fixture)

            if not fixture._has_db_resource():
                msg = fixture._get_db_resource_not_available_reason()
                if self.SKIP_ON_UNAVAILABLE_DB:
                    self.skipTest(msg)
                else:
                    self.fail(msg)


class MySQLOpportunisticFixture(OpportunisticDbFixture):
    DRIVER = 'mysql'


class PostgresqlOpportunisticFixture(OpportunisticDbFixture):
    DRIVER = 'postgresql'


def optimize_package_test_loader(file_):
    """Organize package-level tests into a testresources.OptimizingTestSuite.

    This function provides a unittest-compatible load_tests hook
    for a given package; for per-module, use the
    :func:`.optimize_module_test_loader` function.

    When a unitest or subunit style
    test runner is used, the function will be called in order to
    return a TestSuite containing the tests to run; this function
    ensures that this suite is an OptimisingTestSuite, which will organize
    the production of test resources across groups of tests at once.

    The function is invoked as::

        from oslo_db.sqlalchemy import test_fixtures

        load_tests = test_fixtures.optimize_package_test_loader(__file__)

    The loader *must* be present in the package level __init__.py.

    The function also applies testscenarios expansion to all  test collections.
    This so that an existing test suite that already needs to build
    TestScenarios from a load_tests call can still have this take place when
    replaced with this function.

    """

    this_dir = os.path.dirname(file_)

    def load_tests(loader, found_tests, pattern):
        result = testresources.OptimisingTestSuite()
        result.addTests(found_tests)
        pkg_tests = loader.discover(start_dir=this_dir, pattern=pattern)
        result.addTests(testscenarios.generate_scenarios(pkg_tests))

        return result
    return load_tests


def optimize_module_test_loader():
    """Organize module-level tests into a testresources.OptimizingTestSuite.

    This function provides a unittest-compatible load_tests hook
    for a given module; for per-package, use the
    :func:`.optimize_package_test_loader` function.

    When a unitest or subunit style
    test runner is used, the function will be called in order to
    return a TestSuite containing the tests to run; this function
    ensures that this suite is an OptimisingTestSuite, which will organize
    the production of test resources across groups of tests at once.

    The function is invoked as::

        from oslo_db.sqlalchemy import test_fixtures

        load_tests = test_fixtures.optimize_module_test_loader()

    The loader *must* be present in an individual module, and *not* the
    package level __init__.py.

    The function also applies testscenarios expansion to all  test collections.
    This so that an existing test suite that already needs to build
    TestScenarios from a load_tests call can still have this take place when
    replaced with this function.

    """

    def load_tests(loader, found_tests, pattern):
        result = testresources.OptimisingTestSuite()
        result.addTests(testscenarios.generate_scenarios(found_tests))
        return result
    return load_tests
