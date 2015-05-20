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
"""compatiblity extensions for SQLAlchemy versions.

Elements within this module provide SQLAlchemy features that have been
added at some point but for which oslo.db provides a compatible versions
for previous SQLAlchemy versions.

"""
from oslo_db.sqlalchemy.compat import handle_error as _h_err

# trying to get: "from oslo_db.sqlalchemy import compat; compat.handle_error"
# flake8 won't let me import handle_error directly
handle_error = _h_err.handle_error

__all__ = ['handle_error']
