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

import fixtures

try:
    from oslotest import base as test_base
except ImportError:
    raise NameError('Oslotest is not installed. Please add oslotest in your'
                    ' test-requirements')


import six

from oslo.db import exception
from oslo.db.sqlalchemy import provision
from oslo.db.sqlalchemy import session
from oslo.db.sqlalchemy import utils


class DbFixture(fixtures.Fixture):
    """Basic database fixture.

    Allows to run tests on various db backends, such as SQLite, MySQL and
    PostgreSQL. By default use sqlite backend. To override default backend
    uri set env variable OS_TEST_DBAPI_CONNECTION with database admin
    credentials for specific backend.
    """

    DRIVER = "sqlite"

    # these names are deprecated, and are not used by DbFixture.
    # they are here for backwards compatibility with test suites that
    # are referring to them directly.
    DBNAME = PASSWORD = USERNAME = 'openstack_citest'

    def __init__(self, test):
        super(DbFixture, self).__init__()

        self.test = test

    def setUp(self):
        super(DbFixture, self).setUp()

        try:
            self.provision = provision.ProvisionedDatabase(self.DRIVER)
            self.addCleanup(self.provision.dispose)
        except exception.BackendNotAvailable:
            msg = '%s backend is not available.' % self.DRIVER
            return self.test.skip(msg)
        else:
            self.test.engine = self.provision.engine
            self.addCleanup(setattr, self.test, 'engine', None)
            self.test.sessionmaker = session.get_maker(self.test.engine)
            self.addCleanup(setattr, self.test, 'sessionmaker', None)


class DbTestCase(test_base.BaseTestCase):
    """Base class for testing of DB code.

    Using `DbFixture`. Intended to be the main database test case to use all
    the tests on a given backend with user defined uri. Backend specific
    tests should be decorated with `backend_specific` decorator.
    """

    FIXTURE = DbFixture

    def setUp(self):
        super(DbTestCase, self).setUp()
        self.useFixture(self.FIXTURE(self))


class OpportunisticTestCase(DbTestCase):
    """Placeholder for backwards compatibility."""

ALLOWED_DIALECTS = ['sqlite', 'mysql', 'postgresql']


def backend_specific(*dialects):
    """Decorator to skip backend specific tests on inappropriate engines.

    ::dialects: list of dialects names under which the test will be launched.
    """
    def wrap(f):
        @six.wraps(f)
        def ins_wrap(self):
            if not set(dialects).issubset(ALLOWED_DIALECTS):
                raise ValueError(
                    "Please use allowed dialects: %s" % ALLOWED_DIALECTS)
            if self.engine.name not in dialects:
                msg = ('The test "%s" can be run '
                       'only on %s. Current engine is %s.')
                args = (utils.get_callable_name(f), ' '.join(dialects),
                        self.engine.name)
                self.skip(msg % args)
            else:
                return f(self)
        return ins_wrap
    return wrap


class MySQLOpportunisticFixture(DbFixture):
    DRIVER = 'mysql'


class PostgreSQLOpportunisticFixture(DbFixture):
    DRIVER = 'postgresql'


class MySQLOpportunisticTestCase(OpportunisticTestCase):
    FIXTURE = MySQLOpportunisticFixture


class PostgreSQLOpportunisticTestCase(OpportunisticTestCase):
    FIXTURE = PostgreSQLOpportunisticFixture
