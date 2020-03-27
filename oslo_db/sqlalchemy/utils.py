# Copyright 2010 United States Government as represented by the
# Administrator of the National Aeronautics and Space Administration.
# Copyright 2010-2011 OpenStack Foundation.
# Copyright 2012 Justin Santa Barbara
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

import collections
from collections import abc
import contextlib
import inspect as pyinspect
import itertools
import logging
import re

from alembic.migration import MigrationContext
from alembic.operations import Operations
from oslo_utils import timeutils
import sqlalchemy
from sqlalchemy import Boolean
from sqlalchemy import CheckConstraint
from sqlalchemy import Column
from sqlalchemy.engine import Connectable
from sqlalchemy.engine import reflection
from sqlalchemy.engine import url as sa_url
from sqlalchemy import exc
from sqlalchemy import func
from sqlalchemy import Index
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy.sql.expression import cast
from sqlalchemy.sql.expression import literal_column
from sqlalchemy.sql import text
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy.types import NullType

from oslo_db._i18n import _
from oslo_db import exception
from oslo_db.sqlalchemy import models
from oslo_db.sqlalchemy import ndb

# NOTE(ochuprykov): Add references for backwards compatibility
InvalidSortKey = exception.InvalidSortKey
ColumnError = exception.ColumnError

LOG = logging.getLogger(__name__)

_DBURL_REGEX = re.compile(r"[^:]+://([^:]+):([^@]+)@.+")

_VALID_SORT_DIR = [
    "-".join(x) for x in itertools.product(["asc", "desc"],
                                           ["nullsfirst", "nullslast"])]


def sanitize_db_url(url):
    match = _DBURL_REGEX.match(url)
    if match:
        return '%s****:****%s' % (url[:match.start(1)], url[match.end(2):])
    return url


def get_unique_keys(model):
    """Get a list of sets of unique model keys.

    :param model: the ORM model class
    :rtype: list of sets of strings
    :return: unique model keys or None if unable to find them
    """

    try:
        mapper = inspect(model)
    except exc.NoInspectionAvailable:
        return None
    else:
        local_table = mapper.local_table
        base_table = mapper.base_mapper.local_table

        if local_table is None:
            return None

    # extract result from cache if present
    has_info = hasattr(local_table, 'info')
    if has_info:
        info = local_table.info
        if 'oslodb_unique_keys' in info:
            return info['oslodb_unique_keys']

    res = []
    try:
        constraints = base_table.constraints
    except AttributeError:
        constraints = []
    for constraint in constraints:
        # filter out any CheckConstraints
        if isinstance(constraint, (sqlalchemy.UniqueConstraint,
                                   sqlalchemy.PrimaryKeyConstraint)):
            res.append({c.name for c in constraint.columns})
    try:
        indexes = base_table.indexes
    except AttributeError:
        indexes = []
    for index in indexes:
        if index.unique:
            res.append({c.name for c in index.columns})
    # cache result for next calls with the same model
    if has_info:
        info['oslodb_unique_keys'] = res
    return res


def _stable_sorting_order(model, sort_keys):
    """Check whether the sorting order is stable.

    :return: True if it is stable, False if it's not, None if it's impossible
    to determine.
    """
    keys = get_unique_keys(model)
    if keys is None:
        return None
    sort_keys_set = set(sort_keys)
    for unique_keys in keys:
        if unique_keys.issubset(sort_keys_set):
            return True
    return False


# copy from glance/db/sqlalchemy/api.py
def paginate_query(query, model, limit, sort_keys, marker=None,
                   sort_dir=None, sort_dirs=None):
    """Returns a query with sorting / pagination criteria added.

    Pagination works by requiring a unique sort_key, specified by sort_keys.
    (If sort_keys is not unique, then we risk looping through values.)
    We use the last row in the previous page as the 'marker' for pagination.
    So we must return values that follow the passed marker in the order.
    With a single-valued sort_key, this would be easy: sort_key > X.
    With a compound-values sort_key, (k1, k2, k3) we must do this to repeat
    the lexicographical ordering:
    (k1 > X1) or (k1 == X1 && k2 > X2) or (k1 == X1 && k2 == X2 && k3 > X3)

    We also have to cope with different sort_directions and cases where k2,
    k3, ... are nullable.

    Typically, the id of the last row is used as the client-facing pagination
    marker, then the actual marker object must be fetched from the db and
    passed in to us as marker.

    The "offset" parameter is intentionally avoided. As offset requires a
    full scan through the preceding results each time, criteria-based
    pagination is preferred. See http://use-the-index-luke.com/no-offset
    for further background.

    :param query: the query object to which we should add paging/sorting
    :param model: the ORM model class
    :param limit: maximum number of items to return
    :param sort_keys: array of attributes by which results should be sorted
    :param marker: the last item of the previous page; we returns the next
                    results after this value.
    :param sort_dir: direction in which results should be sorted (asc, desc)
                     suffix -nullsfirst, -nullslast can be added to defined
                     the ordering of null values
    :param sort_dirs: per-column array of sort_dirs, corresponding to sort_keys

    :rtype: sqlalchemy.orm.query.Query
    :return: The query with sorting/pagination added.
    """
    if _stable_sorting_order(model, sort_keys) is False:
        LOG.warning('Unique keys not in sort_keys. '
                    'The sorting order may be unstable.')

    if sort_dir and sort_dirs:
        raise AssertionError('Disallow set sort_dir and '
                             'sort_dirs at the same time.')

    # Default the sort direction to ascending
    if sort_dirs is None and sort_dir is None:
        sort_dir = 'asc'

    # Ensure a per-column sort direction
    if sort_dirs is None:
        sort_dirs = [sort_dir for _sort_key in sort_keys]

    if len(sort_dirs) != len(sort_keys):
        raise AssertionError('sort_dirs and sort_keys must have same length.')

    # Add sorting
    for current_sort_key, current_sort_dir in zip(sort_keys, sort_dirs):
        try:
            inspect(model).all_orm_descriptors[current_sort_key]
        except KeyError:
            raise exception.InvalidSortKey(current_sort_key)
        else:
            sort_key_attr = getattr(model, current_sort_key)

        try:
            main_sort_dir, __, null_sort_dir = current_sort_dir.partition("-")
            sort_dir_func = {
                'asc': sqlalchemy.asc,
                'desc': sqlalchemy.desc,
            }[main_sort_dir]

            null_order_by_stmt = {
                "": None,
                "nullsfirst": sort_key_attr.is_(None),
                "nullslast": sort_key_attr.isnot(None),
            }[null_sort_dir]
        except KeyError:
            raise ValueError(_("Unknown sort direction, "
                               "must be one of: %s") %
                             ", ".join(_VALID_SORT_DIR))

        if null_order_by_stmt is not None:
            query = query.order_by(sqlalchemy.desc(null_order_by_stmt))
        query = query.order_by(sort_dir_func(sort_key_attr))

    # Add pagination
    if marker is not None:
        marker_values = []
        for sort_key in sort_keys:
            v = getattr(marker, sort_key)
            marker_values.append(v)

        # Build up an array of sort criteria as in the docstring
        criteria_list = []
        for i in range(len(sort_keys)):
            crit_attrs = []
            # NOTE: We skip the marker value comparison if marker_values[i] is
            #       None, for two reasons: 1) the comparison operators below
            #       ('<', '>') are not applicable on None value; 2) this is
            #       safe because we can assume the primary key is included in
            #       sort_key, thus checked as (one of) marker values.
            if marker_values[i] is not None:
                for j in range(i):
                    model_attr = getattr(model, sort_keys[j])
                    if marker_values[j] is not None:
                        crit_attrs.append((model_attr == marker_values[j]))

                model_attr = getattr(model, sort_keys[i])
                val = marker_values[i]
                # sqlalchemy doesn't like booleans in < >. bug/1656947
                if isinstance(model_attr.type, Boolean):
                    val = int(val)
                    model_attr = cast(model_attr, Integer)
                if sort_dirs[i].startswith('desc'):
                    crit_attr = (model_attr < val)
                    if sort_dirs[i].endswith('nullsfirst'):
                        crit_attr = sqlalchemy.sql.or_(crit_attr,
                                                       model_attr.is_(None))
                else:
                    crit_attr = (model_attr > val)
                    if sort_dirs[i].endswith('nullslast'):
                        crit_attr = sqlalchemy.sql.or_(crit_attr,
                                                       model_attr.is_(None))
                crit_attrs.append(crit_attr)
                criteria = sqlalchemy.sql.and_(*crit_attrs)
                criteria_list.append(criteria)

        f = sqlalchemy.sql.or_(*criteria_list)
        query = query.filter(f)

    if limit is not None:
        query = query.limit(limit)

    return query


def to_list(x, default=None):
    if x is None:
        return default
    if not isinstance(x, abc.Iterable) or isinstance(x, str):
        return [x]
    elif isinstance(x, list):
        return x
    else:
        return list(x)


def _read_deleted_filter(query, db_model, deleted):
    if 'deleted' not in db_model.__table__.columns:
        raise ValueError(_("There is no `deleted` column in `%s` table. "
                           "Project doesn't use soft-deleted feature.")
                         % db_model.__name__)

    default_deleted_value = db_model.__table__.c.deleted.default.arg
    if deleted:
        query = query.filter(db_model.deleted != default_deleted_value)
    else:
        query = query.filter(db_model.deleted == default_deleted_value)
    return query


def _project_filter(query, db_model, project_id):
    if 'project_id' not in db_model.__table__.columns:
        raise ValueError(_("There is no `project_id` column in `%s` table.")
                         % db_model.__name__)

    if isinstance(project_id, (list, tuple, set)):
        query = query.filter(db_model.project_id.in_(project_id))
    else:
        query = query.filter(db_model.project_id == project_id)

    return query


def model_query(model, session, args=None, **kwargs):
    """Query helper for db.sqlalchemy api methods.

    This accounts for `deleted` and `project_id` fields.

    :param model:        Model to query. Must be a subclass of ModelBase.
    :type model:         models.ModelBase

    :param session:      The session to use.
    :type session:       sqlalchemy.orm.session.Session

    :param args:         Arguments to query. If None - model is used.
    :type args:          tuple

    Keyword arguments:

    :keyword project_id: If present, allows filtering by project_id(s).
                         Can be either a project_id value, or an iterable of
                         project_id values, or None. If an iterable is passed,
                         only rows whose project_id column value is on the
                         `project_id` list will be returned. If None is passed,
                         only rows which are not bound to any project, will be
                         returned.
    :type project_id:    iterable,
                         model.__table__.columns.project_id.type,
                         None type

    :keyword deleted:    If present, allows filtering by deleted field.
                         If True is passed, only deleted entries will be
                         returned, if False - only existing entries.
    :type deleted:       bool


    Usage:

    .. code-block:: python

      from oslo_db.sqlalchemy import utils


      def get_instance_by_uuid(uuid):
          session = get_session()
          with session.begin()
              return (utils.model_query(models.Instance, session=session)
                           .filter(models.Instance.uuid == uuid)
                           .first())

      def get_nodes_stat():
          data = (Node.id, Node.cpu, Node.ram, Node.hdd)

          session = get_session()
          with session.begin()
              return utils.model_query(Node, session=session, args=data).all()

    Also you can create your own helper, based on ``utils.model_query()``.
    For example, it can be useful if you plan to use ``project_id`` and
    ``deleted`` parameters from project's ``context``

    .. code-block:: python

      from oslo_db.sqlalchemy import utils


      def _model_query(context, model, session=None, args=None,
                       project_id=None, project_only=False,
                       read_deleted=None):

          # We suppose, that functions ``_get_project_id()`` and
          # ``_get_deleted()`` should handle passed parameters and
          # context object (for example, decide, if we need to restrict a user
          # to query his own entries by project_id or only allow admin to read
          # deleted entries). For return values, we expect to get
          # ``project_id`` and ``deleted``, which are suitable for the
          # ``model_query()`` signature.
          kwargs = {}
          if project_id is not None:
              kwargs['project_id'] = _get_project_id(context, project_id,
                                                     project_only)
          if read_deleted is not None:
              kwargs['deleted'] = _get_deleted_dict(context, read_deleted)
          session = session or get_session()

          with session.begin():
              return utils.model_query(model, session=session,
                                       args=args, **kwargs)

      def get_instance_by_uuid(context, uuid):
          return (_model_query(context, models.Instance, read_deleted='yes')
                        .filter(models.Instance.uuid == uuid)
                        .first())

      def get_nodes_data(context, project_id, project_only='allow_none'):
          data = (Node.id, Node.cpu, Node.ram, Node.hdd)

          return (_model_query(context, Node, args=data, project_id=project_id,
                               project_only=project_only)
                        .all())

    """

    if not issubclass(model, models.ModelBase):
        raise TypeError(_("model should be a subclass of ModelBase"))

    query = session.query(model) if not args else session.query(*args)
    if 'deleted' in kwargs:
        query = _read_deleted_filter(query, model, kwargs['deleted'])
    if 'project_id' in kwargs:
        query = _project_filter(query, model, kwargs['project_id'])

    return query


def get_table(engine, name):
    """Returns an sqlalchemy table dynamically from db.

    Needed because the models don't work for us in migrations
    as models will be far out of sync with the current data.

    .. warning::

       Do not use this method when creating ForeignKeys in database migrations
       because sqlalchemy needs the same MetaData object to hold information
       about the parent table and the reference table in the ForeignKey. This
       method uses a unique MetaData object per table object so it won't work
       with ForeignKey creation.
    """
    metadata = MetaData()
    metadata.bind = engine
    return Table(name, metadata, autoload=True)


def _get_not_supported_column(col_name_col_instance, column_name):
    try:
        column = col_name_col_instance[column_name]
    except KeyError:
        msg = _("Please specify column %s in col_name_col_instance "
                "param. It is required because column has unsupported "
                "type by SQLite.")
        raise exception.ColumnError(msg % column_name)

    if not isinstance(column, Column):
        msg = _("col_name_col_instance param has wrong type of "
                "column instance for column %s It should be instance "
                "of sqlalchemy.Column.")
        raise exception.ColumnError(msg % column_name)
    return column


def drop_old_duplicate_entries_from_table(engine, table_name,
                                          use_soft_delete, *uc_column_names):
    """Drop all old rows having the same values for columns in uc_columns.

    This method drop (or mark ad `deleted` if use_soft_delete is True) old
    duplicate rows form table with name `table_name`.

    :param engine:          Sqlalchemy engine
    :param table_name:      Table with duplicates
    :param use_soft_delete: If True - values will be marked as `deleted`,
                            if False - values will be removed from table
    :param uc_column_names: Unique constraint columns
    """
    meta = MetaData()
    meta.bind = engine

    table = Table(table_name, meta, autoload=True)
    columns_for_group_by = [table.c[name] for name in uc_column_names]

    columns_for_select = [func.max(table.c.id)]
    columns_for_select.extend(columns_for_group_by)

    duplicated_rows_select = sqlalchemy.sql.select(
        columns_for_select, group_by=columns_for_group_by,
        having=func.count(table.c.id) > 1)

    for row in engine.execute(duplicated_rows_select).fetchall():
        # NOTE(boris-42): Do not remove row that has the biggest ID.
        delete_condition = table.c.id != row[0]
        is_none = None  # workaround for pyflakes
        delete_condition &= table.c.deleted_at == is_none
        for name in uc_column_names:
            delete_condition &= table.c[name] == row[name]

        rows_to_delete_select = sqlalchemy.sql.select(
            [table.c.id]).where(delete_condition)
        for row in engine.execute(rows_to_delete_select).fetchall():
            LOG.info("Deleting duplicated row with id: %(id)s from table: "
                     "%(table)s", dict(id=row[0], table=table_name))

        if use_soft_delete:
            delete_statement = table.update().\
                where(delete_condition).\
                values({
                    'deleted': literal_column('id'),
                    'updated_at': literal_column('updated_at'),
                    'deleted_at': timeutils.utcnow()
                })
        else:
            delete_statement = table.delete().where(delete_condition)
        engine.execute(delete_statement)


def _get_default_deleted_value(table):
    if isinstance(table.c.id.type, Integer):
        return 0
    if isinstance(table.c.id.type, String):
        return ""
    raise exception.ColumnError(_("Unsupported id columns type"))


def _restore_indexes_on_deleted_columns(engine, table_name, indexes):
    table = get_table(engine, table_name)

    real_indexes = get_indexes(engine, table_name)
    existing_index_names = dict(
        [(index['name'], index['column_names']) for index in real_indexes])

    # NOTE(boris-42): Restore indexes on `deleted` column
    for index in indexes:
        if 'deleted' not in index['column_names']:
            continue
        name = index['name']
        if name in existing_index_names:
            column_names = [table.c[c] for c in existing_index_names[name]]
            old_index = Index(name, *column_names, unique=index["unique"])
            old_index.drop(engine)

        column_names = [table.c[c] for c in index['column_names']]
        new_index = Index(index["name"], *column_names, unique=index["unique"])
        new_index.create(engine)


def change_deleted_column_type_to_boolean(engine, table_name,
                                          **col_name_col_instance):
    if engine.name == "sqlite":
        return _change_deleted_column_type_to_boolean_sqlite(
            engine, table_name, **col_name_col_instance)
    indexes = get_indexes(engine, table_name)
    table = get_table(engine, table_name)

    old_deleted = Column('old_deleted', Boolean, default=False)
    old_deleted.create(table, populate_default=False)

    table.update().\
        where(table.c.deleted == table.c.id).\
        values(old_deleted=True).\
        execute()

    table.c.deleted.drop()
    table.c.old_deleted.alter(name="deleted")

    _restore_indexes_on_deleted_columns(engine, table_name, indexes)


def _change_deleted_column_type_to_boolean_sqlite(engine, table_name,
                                                  **col_name_col_instance):
    table = get_table(engine, table_name)
    columns = []
    for column in table.columns:
        column_copy = None
        if column.name != "deleted":
            if isinstance(column.type, NullType):
                column_copy = _get_not_supported_column(col_name_col_instance,
                                                        column.name)
            else:
                column_copy = column.copy()
        else:
            column_copy = Column('deleted', Boolean, default=0)
        columns.append(column_copy)

    constraints = [constraint.copy() for constraint in table.constraints]

    meta = table.metadata
    new_table = Table(table_name + "__tmp__", meta,
                      *(columns + constraints))
    new_table.create()

    indexes = []
    for index in get_indexes(engine, table_name):
        column_names = [new_table.c[c] for c in index['column_names']]
        indexes.append(Index(index["name"], *column_names,
                             unique=index["unique"]))

    c_select = []
    for c in table.c:
        if c.name != "deleted":
            c_select.append(c)
        else:
            c_select.append(table.c.deleted == table.c.id)

    table.drop()
    for index in indexes:
        index.create(engine)

    new_table.rename(table_name)
    new_table.update().\
        where(new_table.c.deleted == new_table.c.id).\
        values(deleted=True).\
        execute()


def change_deleted_column_type_to_id_type(engine, table_name,
                                          **col_name_col_instance):
    if engine.name == "sqlite":
        return _change_deleted_column_type_to_id_type_sqlite(
            engine, table_name, **col_name_col_instance)
    indexes = get_indexes(engine, table_name)
    table = get_table(engine, table_name)

    new_deleted = Column('new_deleted', table.c.id.type,
                         default=_get_default_deleted_value(table))
    new_deleted.create(table, populate_default=True)

    deleted = True  # workaround for pyflakes
    table.update().\
        where(table.c.deleted == deleted).\
        values(new_deleted=table.c.id).\
        execute()
    table.c.deleted.drop()
    table.c.new_deleted.alter(name="deleted")

    _restore_indexes_on_deleted_columns(engine, table_name, indexes)


def _is_deleted_column_constraint(constraint):
    # NOTE(boris-42): There is no other way to check is CheckConstraint
    #                 associated with deleted column.
    if not isinstance(constraint, CheckConstraint):
        return False
    sqltext = str(constraint.sqltext)
    # NOTE(zzzeek): SQLite never reflected CHECK contraints here
    # in any case until version 1.1.   Safe to assume that any CHECK
    # that's talking about the value of "deleted in (something)" is
    # the boolean constraint we're looking to get rid of.
    return bool(re.match(r".*deleted in \(.*\)", sqltext, re.I))


def _change_deleted_column_type_to_id_type_sqlite(engine, table_name,
                                                  **col_name_col_instance):
    # NOTE(boris-42): sqlaclhemy-migrate can't drop column with check
    #                 constraints in sqlite DB and our `deleted` column has
    #                 2 check constraints. So there is only one way to remove
    #                 these constraints:
    #                 1) Create new table with the same columns, constraints
    #                 and indexes. (except deleted column).
    #                 2) Copy all data from old to new table.
    #                 3) Drop old table.
    #                 4) Rename new table to old table name.
    meta = MetaData(bind=engine)
    table = Table(table_name, meta, autoload=True)
    default_deleted_value = _get_default_deleted_value(table)

    columns = []
    for column in table.columns:
        column_copy = None
        if column.name != "deleted":
            if isinstance(column.type, NullType):
                column_copy = _get_not_supported_column(col_name_col_instance,
                                                        column.name)
            else:
                column_copy = column.copy()
        else:
            column_copy = Column('deleted', table.c.id.type,
                                 default=default_deleted_value)
        columns.append(column_copy)

    constraints = []
    for constraint in table.constraints:
        if not _is_deleted_column_constraint(constraint):
            constraints.append(constraint.copy())

    new_table = Table(table_name + "__tmp__", meta,
                      *(columns + constraints))
    new_table.create()

    indexes = []
    for index in get_indexes(engine, table_name):
        column_names = [new_table.c[c] for c in index['column_names']]
        indexes.append(Index(index["name"], *column_names,
                             unique=index["unique"]))

    table.drop()
    for index in indexes:
        index.create(engine)

    new_table.rename(table_name)
    deleted = True  # workaround for pyflakes
    new_table.update().\
        where(new_table.c.deleted == deleted).\
        values(deleted=new_table.c.id).\
        execute()

    # NOTE(boris-42): Fix value of deleted column: False -> "" or 0.
    deleted = False  # workaround for pyflakes
    new_table.update().\
        where(new_table.c.deleted == deleted).\
        values(deleted=default_deleted_value).\
        execute()


def get_db_connection_info(conn_pieces):
    database = conn_pieces.path.strip('/')
    loc_pieces = conn_pieces.netloc.split('@')
    host = loc_pieces[1]

    auth_pieces = loc_pieces[0].split(':')
    user = auth_pieces[0]
    password = ""
    if len(auth_pieces) > 1:
        password = auth_pieces[1].strip()

    return (user, password, database, host)


def get_indexes(engine, table_name):
    """Get all index list from a given table.

    :param engine: sqlalchemy engine
    :param table_name: name of the table
    """

    inspector = reflection.Inspector.from_engine(engine)
    indexes = inspector.get_indexes(table_name)
    return indexes


def index_exists(engine, table_name, index_name):
    """Check if given index exists.

    :param engine:     sqlalchemy engine
    :param table_name: name of the table
    :param index_name: name of the index
    """
    indexes = get_indexes(engine, table_name)
    index_names = [index['name'] for index in indexes]
    return index_name in index_names


def index_exists_on_columns(engine, table_name, columns):
    """Check if an index on given columns exists.

    :param engine: sqlalchemy engine
    :param table_name: name of the table
    :param columns: a list type of columns that will be checked
    """
    if not isinstance(columns, list):
        columns = list(columns)
    for index in get_indexes(engine, table_name):
        if index['column_names'] == columns:
            return True
    return False


def add_index(engine, table_name, index_name, idx_columns):
    """Create an index for given columns.

    :param engine:      sqlalchemy engine
    :param table_name:  name of the table
    :param index_name:  name of the index
    :param idx_columns: tuple with names of columns that will be indexed
    """
    table = get_table(engine, table_name)
    if not index_exists(engine, table_name, index_name):
        index = Index(
            index_name, *[getattr(table.c, col) for col in idx_columns]
        )
        index.create()
    else:
        raise ValueError("Index '%s' already exists!" % index_name)


def drop_index(engine, table_name, index_name):
    """Drop index with given name.

    :param engine:     sqlalchemy engine
    :param table_name: name of the table
    :param index_name: name of the index
    """
    table = get_table(engine, table_name)
    for index in table.indexes:
        if index.name == index_name:
            index.drop()
            break
    else:
        raise ValueError("Index '%s' not found!" % index_name)


def change_index_columns(engine, table_name, index_name, new_columns):
    """Change set of columns that are indexed by given index.

    :param engine:      sqlalchemy engine
    :param table_name:  name of the table
    :param index_name:  name of the index
    :param new_columns: tuple with names of columns that will be indexed
    """
    drop_index(engine, table_name, index_name)
    add_index(engine, table_name, index_name, new_columns)


def column_exists(engine, table_name, column):
    """Check if table has given column.

    :param engine:     sqlalchemy engine
    :param table_name: name of the table
    :param column:     name of the colmn
    """
    t = get_table(engine, table_name)
    return column in t.c


class DialectFunctionDispatcher(object):
    @classmethod
    def dispatch_for_dialect(cls, expr, multiple=False):
        """Provide dialect-specific functionality within distinct functions.

        e.g.::

            @dispatch_for_dialect("*")
            def set_special_option(engine):
                pass

            @set_special_option.dispatch_for("sqlite")
            def set_sqlite_special_option(engine):
                return engine.execute("sqlite thing")

            @set_special_option.dispatch_for("mysql+mysqldb")
            def set_mysqldb_special_option(engine):
                return engine.execute("mysqldb thing")

        After the above registration, the ``set_special_option()`` function
        is now a dispatcher, given a SQLAlchemy ``Engine``, ``Connection``,
        URL string, or ``sqlalchemy.engine.URL`` object::

            eng = create_engine('...')
            result = set_special_option(eng)

        The filter system supports two modes, "multiple" and "single".
        The default is "single", and requires that one and only one function
        match for a given backend.    In this mode, the function may also
        have a return value, which will be returned by the top level
        call.

        "multiple" mode, on the other hand, does not support return
        arguments, but allows for any number of matching functions, where
        each function will be called::

            # the initial call sets this up as a "multiple" dispatcher
            @dispatch_for_dialect("*", multiple=True)
            def set_options(engine):
                # set options that apply to *all* engines

            @set_options.dispatch_for("postgresql")
            def set_postgresql_options(engine):
                # set options that apply to all Postgresql engines

            @set_options.dispatch_for("postgresql+psycopg2")
            def set_postgresql_psycopg2_options(engine):
                # set options that apply only to "postgresql+psycopg2"

            @set_options.dispatch_for("*+pyodbc")
            def set_pyodbc_options(engine):
                # set options that apply to all pyodbc backends

        Note that in both modes, any number of additional arguments can be
        accepted by member functions.  For example, to populate a dictionary of
        options, it may be passed in::

            @dispatch_for_dialect("*", multiple=True)
            def set_engine_options(url, opts):
                pass

            @set_engine_options.dispatch_for("mysql+mysqldb")
            def _mysql_set_default_charset_to_utf8(url, opts):
                opts.setdefault('charset', 'utf-8')

            @set_engine_options.dispatch_for("sqlite")
            def _set_sqlite_in_memory_check_same_thread(url, opts):
                if url.database in (None, 'memory'):
                    opts['check_same_thread'] = False

            opts = {}
            set_engine_options(url, opts)

        The driver specifiers are of the form:
        ``<database | *>[+<driver | *>]``.   That is, database name or "*",
        followed by an optional ``+`` sign with driver or "*".   Omitting
        the driver name implies all drivers for that database.

        """
        if multiple:
            cls = DialectMultiFunctionDispatcher
        else:
            cls = DialectSingleFunctionDispatcher
        return cls().dispatch_for(expr)

    _db_plus_driver_reg = re.compile(r'([^+]+?)(?:\+(.+))?$')

    def dispatch_for(self, expr):
        def decorate(fn):
            dbname, driver = self._parse_dispatch(expr)
            if fn is self:
                fn = fn._last
            self._last = fn
            self._register(expr, dbname, driver, fn)
            return self
        return decorate

    def _parse_dispatch(self, text):
        m = self._db_plus_driver_reg.match(text)
        if not m:
            raise ValueError("Couldn't parse database[+driver]: %r" % text)
        return m.group(1) or '*', m.group(2) or '*'

    def __call__(self, *arg, **kw):
        target = arg[0]
        return self._dispatch_on(
            self._url_from_target(target), target, arg, kw)

    def _url_from_target(self, target):
        if isinstance(target, Connectable):
            return target.engine.url
        elif isinstance(target, str):
            if "://" not in target:
                target_url = sa_url.make_url("%s://" % target)
            else:
                target_url = sa_url.make_url(target)
            return target_url
        elif isinstance(target, sa_url.URL):
            return target
        else:
            raise ValueError("Invalid target type: %r" % target)

    def dispatch_on_drivername(self, drivername):
        """Return a sub-dispatcher for the given drivername.

        This provides a means of calling a different function, such as the
        "*" function, for a given target object that normally refers
        to a sub-function.

        """
        dbname, driver = self._db_plus_driver_reg.match(drivername).group(1, 2)

        def go(*arg, **kw):
            return self._dispatch_on_db_driver(dbname, "*", arg, kw)

        return go

    def _dispatch_on(self, url, target, arg, kw):
        dbname, driver = self._db_plus_driver_reg.match(
            url.drivername).group(1, 2)
        if not driver:
            driver = url.get_dialect().driver

        return self._dispatch_on_db_driver(dbname, driver, arg, kw)

    def _invoke_fn(self, fn, arg, kw):
        return fn(*arg, **kw)


class DialectSingleFunctionDispatcher(DialectFunctionDispatcher):
    def __init__(self):
        self.reg = collections.defaultdict(dict)

    def _register(self, expr, dbname, driver, fn):
        fn_dict = self.reg[dbname]
        if driver in fn_dict:
            raise TypeError("Multiple functions for expression %r" % expr)
        fn_dict[driver] = fn

    def _matches(self, dbname, driver):
        for db in (dbname, '*'):
            subdict = self.reg[db]
            for drv in (driver, '*'):
                if drv in subdict:
                    return subdict[drv]
        else:
            raise ValueError(
                "No default function found for driver: %r" %
                ("%s+%s" % (dbname, driver)))

    def _dispatch_on_db_driver(self, dbname, driver, arg, kw):
        fn = self._matches(dbname, driver)
        return self._invoke_fn(fn, arg, kw)


class DialectMultiFunctionDispatcher(DialectFunctionDispatcher):
    def __init__(self):
        self.reg = collections.defaultdict(
            lambda: collections.defaultdict(list))

    def _register(self, expr, dbname, driver, fn):
        self.reg[dbname][driver].append(fn)

    def _matches(self, dbname, driver):
        if driver != '*':
            drivers = (driver, '*')
        else:
            drivers = ('*', )

        for db in (dbname, '*'):
            subdict = self.reg[db]
            for drv in drivers:
                for fn in subdict[drv]:
                    yield fn

    def _dispatch_on_db_driver(self, dbname, driver, arg, kw):
        for fn in self._matches(dbname, driver):
            if self._invoke_fn(fn, arg, kw) is not None:
                raise TypeError(
                    "Return value not allowed for "
                    "multiple filtered function")


dispatch_for_dialect = DialectFunctionDispatcher.dispatch_for_dialect


def get_non_innodb_tables(connectable, skip_tables=('migrate_version',
                                                    'alembic_version')):
    """Get a list of tables which don't use InnoDB storage engine.

    :param connectable: a SQLAlchemy Engine or a Connection instance
    :param skip_tables: a list of tables which might have a different
                        storage engine
    """
    query_str = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = :database AND
              engine != 'InnoDB'
    """

    params = {}
    if skip_tables:
        params = dict(
            ('skip_%s' % i, table_name)
            for i, table_name in enumerate(skip_tables)
        )

        placeholders = ', '.join(':' + p for p in params)
        query_str += ' AND table_name NOT IN (%s)' % placeholders

    params['database'] = connectable.engine.url.database
    query = text(query_str)
    noninnodb = connectable.execute(query, **params)
    return [i[0] for i in noninnodb]


def get_non_ndbcluster_tables(connectable, skip_tables=None):
    """Get a list of tables which don't use MySQL Cluster (NDB) storage engine.

    :param connectable: a SQLAlchemy Engine or Connection instance
    :param skip_tables: a list of tables which might have a different
                        storage engine
    """
    query_str = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = :database AND
              engine != 'ndbcluster'
    """

    params = {}
    if skip_tables:
        params = dict(
            ('skip_%s' % i, table_name)
            for i, table_name in enumerate(skip_tables)
        )

        placeholders = ', '.join(':' + p for p in params)
        query_str += ' AND table_name NOT IN (%s)' % placeholders

    params['database'] = connectable.engine.url.database
    query = text(query_str)
    nonndbcluster = connectable.execute(query, **params)
    return [i[0] for i in nonndbcluster]


def get_foreign_key_constraint_name(engine, table_name, column_name):
    """Find the name of foreign key in a table, given constrained column name.

    :param engine: a SQLAlchemy engine (or connection)

    :param table_name: name of table which contains the constraint

    :param column_name: name of column that is constrained by the foreign key.

    :return: the name of the first foreign key constraint which constrains
     the given column in the given table.

    """
    insp = inspect(engine)
    for fk in insp.get_foreign_keys(table_name):
        if column_name in fk['constrained_columns']:
            return fk['name']


@contextlib.contextmanager
def suspend_fk_constraints_for_col_alter(
        engine, table_name, column_name, referents=[]):
    """Detect foreign key constraints, drop, and recreate.

    This is used to guard against a column ALTER that on some backends
    cannot proceed unless foreign key constraints are not present.

    e.g.::

        from oslo_db.sqlalchemy.util import (
            suspend_fk_constraints_for_col_alter
        )

        with suspend_fk_constraints_for_col_alter(
            migrate_engine, "user_table",
            referents=[
                "local_user", "nonlocal_user", "project"
            ]):
            user_table.c.domain_id.alter(nullable=False)

    :param engine: a SQLAlchemy engine (or connection)

    :param table_name: target table name.  All foreign key constraints
     that refer to the table_name / column_name will be dropped and recreated.

    :param column_name: target column name.  all foreign key constraints
     which refer to this column, either partially or fully, will be dropped
     and recreated.

    :param referents: sequence of string table names to search for foreign
     key constraints.   A future version of this function may no longer
     require this argument, however for the moment it is required.

    """
    if (
        not ndb.ndb_status(engine)
    ):
        yield
    else:
        with engine.connect() as conn:
            insp = inspect(conn)
            fks = []
            for ref_table_name in referents:
                for fk in insp.get_foreign_keys(ref_table_name):
                    if not fk.get('name'):
                        raise AssertionError("foreign key hasn't a name.")
                    if fk['referred_table'] == table_name and \
                            column_name in fk['referred_columns']:
                        fk['source_table'] = ref_table_name
                        if 'options' not in fk:
                            fk['options'] = {}
                        fks.append(fk)

            ctx = MigrationContext.configure(conn)
            op = Operations(ctx)

            for fk in fks:
                op.drop_constraint(
                    fk['name'], fk['source_table'], type_="foreignkey")
            yield
            for fk in fks:
                op.create_foreign_key(
                    fk['name'], fk['source_table'],
                    fk['referred_table'],
                    fk['constrained_columns'],
                    fk['referred_columns'],
                    onupdate=fk['options'].get('onupdate'),
                    ondelete=fk['options'].get('ondelete'),
                    deferrable=fk['options'].get('deferrable'),
                    initially=fk['options'].get('initially'),
                )


def getargspec(fn):
    """Inspects a function for its argspec.

    This is to handle a difference between py2/3. The Python 2.x getargspec
    call is deprecated in Python 3.x, with the suggestion to use the signature
    call instead.

    To keep compatibility with the results, while avoiding deprecation
    warnings, this instead will use the getfullargspec instead.

    :param fn: The function to inspect.
    :returns: The argspec for the function.
    """
    if hasattr(pyinspect, 'getfullargspec'):
        return pyinspect.getfullargspec(fn)

    return pyinspect.getargspec(fn)


class NonCommittingConnectable(object):
    """A ``Connectable`` substitute which rolls all operations back.

    ``NonCommittingConnectable`` forms the basis of mock
    ``Engine`` and ``Connection`` objects within a test.   It provides
    only that part of the API that should reasonably be used within
    a single-connection test environment (e.g. no engine.dispose(),
    connection.invalidate(), etc. ).   The connection runs both within
    a transaction as well as a savepoint.   The transaction is there
    so that any operations upon the connection can be rolled back.
    If the test calls begin(), a "pseduo" transaction is returned that
    won't actually commit anything.   The subtransaction is there to allow
    a test to successfully call rollback(), however, where all operations
    to that point will be rolled back and the operations can continue,
    simulating a real rollback while still remaining within a transaction
    external to the test.

    """

    _nested_trans = None

    def __init__(self, connection):
        self.connection = connection
        self._trans = connection.begin()
        self._restart_nested()

    def _restart_nested(self):
        if self._nested_trans is not None:
            self._nested_trans.rollback()
        self._nested_trans = self.connection.begin_nested()

    def _dispose(self):
        if not self.connection.closed:
            self._nested_trans.rollback()
            self._trans.rollback()
            self.connection.close()

    def execute(self, obj, *multiparams, **params):
        """Executes the given construct and returns a :class:`.ResultProxy`."""

        return self.connection.execute(obj, *multiparams, **params)

    def scalar(self, obj, *multiparams, **params):
        """Executes and returns the first column of the first row."""

        return self.connection.scalar(obj, *multiparams, **params)


class NonCommittingEngine(NonCommittingConnectable):
    """``Engine`` -specific non committing connectbale."""

    @property
    def url(self):
        return self.connection.engine.url

    @property
    def engine(self):
        return self

    def connect(self):
        return NonCommittingConnection(self.connection)

    @contextlib.contextmanager
    def begin(self):
        conn = self.connect()
        trans = conn.begin()
        try:
            yield conn
        except Exception:
            trans.rollback()
        else:
            trans.commit()


class NonCommittingConnection(NonCommittingConnectable):
    """``Connection`` -specific non committing connectbale."""

    def close(self):
        """Close the 'Connection'.

        In this context, close() is a no-op.

        """
        pass

    def begin(self):
        return NonCommittingTransaction(self, self.connection.begin())

    def __enter__(self):
        return self

    def __exit__(self, *arg):
        pass


class NonCommittingTransaction(object):
    """A wrapper for ``Transaction``.

    This is to accommodate being able to guaranteed start a new
    SAVEPOINT when a transaction is rolled back.

    """
    def __init__(self, provisioned, transaction):
        self.provisioned = provisioned
        self.transaction = transaction

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if type is None:
            try:
                self.commit()
            except Exception:
                self.rollback()
                raise
        else:
            self.rollback()

    def commit(self):
        self.transaction.commit()

    def rollback(self):
        self.transaction.rollback()
        self.provisioned._restart_nested()
