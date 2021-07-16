# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import warnings

import fixtures
from sqlalchemy import exc as sqla_exc


class WarningsFixture(fixtures.Fixture):
    """Filters out warnings during test runs."""

    def setUp(self):
        super().setUp()
        # Make deprecation warnings only happen once to avoid spamming
        warnings.simplefilter('once', DeprecationWarning)

        warnings.filterwarnings(
            'error', message='Evaluating non-mapped column expression',
            category=sqla_exc.SAWarning)

        # Enable deprecation warnings to capture upcoming SQLAlchemy changes

        warnings.filterwarnings(
            'error',
            category=sqla_exc.SADeprecationWarning)

        # ...but filter everything out until we get around to fixing them
        # FIXME(stephenfin): Remove all of these

        warnings.filterwarnings(
            'once',
            message=r'The Session.autocommit parameter is deprecated .*',
            category=sqla_exc.SADeprecationWarning)

        warnings.filterwarnings(
            'once',
            message=r'The Session.begin.subtransactions flag is deprecated .*',
            category=sqla_exc.SADeprecationWarning)

        warnings.filterwarnings(
            'once',
            message=r'Using non-integer/slice indices on Row is deprecated .*',
            category=sqla_exc.SADeprecationWarning)

        warnings.filterwarnings(
            'once',
            message=r'The Engine.execute\(\) method is considered legacy .*',
            category=sqla_exc.SADeprecationWarning)

        warnings.filterwarnings(
            'once',
            message=r'The Executable.execute\(\) method is considered .*',
            category=sqla_exc.SADeprecationWarning)

        warnings.filterwarnings(
            'once',
            message=r'The Row.keys\(\) method is considered legacy .*',
            category=sqla_exc.SADeprecationWarning)

        warnings.filterwarnings(
            'once',
            message=r'Retrieving row members using strings or other .*',
            category=sqla_exc.SADeprecationWarning)

        warnings.filterwarnings(
            'once',
            message=r'The connection.execute\(\) method in SQLAlchemy 2.0 .*',
            category=sqla_exc.SADeprecationWarning)

        warnings.filterwarnings(
            'once',
            message=r'Calling the mapper\(\) function directly outside .*',
            category=sqla_exc.SADeprecationWarning)

        warnings.filterwarnings(
            'once',
            message=r'The current statement is being autocommitted .*',
            category=sqla_exc.SADeprecationWarning)

        warnings.filterwarnings(
            'once',
            message=r'Calling \.begin\(\) when a transaction is already .*',
            category=sqla_exc.SADeprecationWarning)

        warnings.filterwarnings(
            'once',
            message=r'The Engine.scalar\(\) method is considered legacy .*',
            category=sqla_exc.SADeprecationWarning)

        # ...plus things that aren't our fault

        # FIXME(stephenfin): These are caused by sqlalchemy-migrate, not us,
        # and should be removed when we drop support for that library

        warnings.filterwarnings(
            'ignore',
            message=r'Passing a string to Connection.execute\(\) .*',
            module='migrate',
            category=sqla_exc.SADeprecationWarning)

        self.addCleanup(warnings.resetwarnings)
