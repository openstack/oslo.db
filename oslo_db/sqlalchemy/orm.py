# Copyright 2010 United States Government as represented by the
# Administrator of the National Aeronautics and Space Administration.
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
"""SQLAlchemy ORM connectivity and query structures.
"""

from oslo_utils import timeutils
import sqlalchemy.orm
from sqlalchemy.sql.expression import literal_column

from oslo_db.sqlalchemy import update_match


class Query(sqlalchemy.orm.query.Query):
    """Subclass of sqlalchemy.query with soft_delete() method."""
    def soft_delete(self, synchronize_session='evaluate'):
        return self.update({'deleted': literal_column('id'),
                            'updated_at': literal_column('updated_at'),
                            'deleted_at': timeutils.utcnow()},
                           synchronize_session=synchronize_session)

    def update_returning_pk(self, values, surrogate_key):
        """Perform an UPDATE, returning the primary key of the matched row.

        This is a method-version of
        oslo_db.sqlalchemy.update_match.update_returning_pk(); see that
        function for usage details.

        """
        return update_match.update_returning_pk(self, values, surrogate_key)

    def update_on_match(self, specimen, surrogate_key, values, **kw):
        """Emit an UPDATE statement matching the given specimen.

        This is a method-version of
        oslo_db.sqlalchemy.update_match.update_on_match(); see that function
        for usage details.

        """
        return update_match.update_on_match(
            self, specimen, surrogate_key, values, **kw)


class Session(sqlalchemy.orm.session.Session):
    """oslo.db-specific Session subclass."""


def get_maker(engine, autocommit=True, expire_on_commit=False):
    """Return a SQLAlchemy sessionmaker using the given engine."""
    return sqlalchemy.orm.sessionmaker(bind=engine,
                                       class_=Session,
                                       autocommit=autocommit,
                                       expire_on_commit=expire_on_commit,
                                       query_cls=Query)
