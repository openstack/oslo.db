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

        self.addCleanup(warnings.resetwarnings)
