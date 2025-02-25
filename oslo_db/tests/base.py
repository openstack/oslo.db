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

import unittest

from oslotest import base

from oslo_db.tests import fixtures


class BaseTestCase(base.BaseTestCase):
    """Test case base class for all oslo.db unit tests."""

    def setUp(self):
        """Run before each test method to initialize test environment."""
        super().setUp()

        self.warning_fixture = self.useFixture(fixtures.WarningsFixture())


class BaseAsyncioCase(unittest.IsolatedAsyncioTestCase):
    def run(self, result=None):
        # work around stestr sending a result object that's not a
        # unittest.Result
        result.addDuration = lambda test, elapsed: None
        return super().run(result=result)
