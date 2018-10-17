# Copyright 2018 Red Hat, Inc.
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

"""Custom warnings."""


class NotSupportedWarning(Warning):
    """Warn that an argument or call that was passed is not supported.

    This subclasses Warning so that it can be filtered as a distinct
    category.

    .. seealso::

        https://docs.python.org/2/library/warnings.html

    """


class OsloDBDeprecationWarning(DeprecationWarning):
    """Issued per usage of a deprecated API.

    This subclasses DeprecationWarning so that it can be filtered as a distinct
    category.

    .. seealso::

        https://docs.python.org/2/library/warnings.html

    """
