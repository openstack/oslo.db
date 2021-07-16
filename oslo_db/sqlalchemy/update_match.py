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

import copy

from sqlalchemy import inspect
from sqlalchemy import orm
from sqlalchemy import sql
from sqlalchemy import types as sqltypes

from oslo_db.sqlalchemy import utils


def update_on_match(
    query,
    specimen,
    surrogate_key,
    values=None,
    attempts=3,
    include_only=None,
    process_query=None,
    handle_failure=None
):
    """Emit an UPDATE statement matching the given specimen.

    E.g.::

        with enginefacade.writer() as session:
            specimen = MyInstance(
                uuid='ccea54f',
                interface_id='ad33fea',
                vm_state='SOME_VM_STATE',
            )

            values = {
                'vm_state': 'SOME_NEW_VM_STATE'
            }

            base_query = model_query(
                context, models.Instance,
                project_only=True, session=session)

            hostname_query = model_query(
                    context, models.Instance, session=session,
                    read_deleted='no').
                filter(func.lower(models.Instance.hostname) == 'SOMEHOSTNAME')

            surrogate_key = ('uuid', )

            def process_query(query):
                return query.where(~exists(hostname_query))

            def handle_failure(query):
                try:
                    instance = base_query.one()
                except NoResultFound:
                    raise exception.InstanceNotFound(instance_id=instance_uuid)

                if session.query(hostname_query.exists()).scalar():
                    raise exception.InstanceExists(
                        name=values['hostname'].lower())

                # try again
                return False

            persistent_instance = base_query.update_on_match(
                specimen,
                surrogate_key,
                values=values,
                process_query=process_query,
                handle_failure=handle_failure
            )

    The UPDATE statement is constructed against the given specimen
    using those values which are present to construct a WHERE clause.
    If the specimen contains additional values to be ignored, the
    ``include_only`` parameter may be passed which indicates a sequence
    of attributes to use when constructing the WHERE.

    The UPDATE is performed against an ORM Query, which is created from
    the given ``Session``, or alternatively by passing the ```query``
    parameter referring to an existing query.

    Before the query is invoked, it is also passed through the callable
    sent as ``process_query``, if present.  This hook allows additional
    criteria to be added to the query after it is created but before
    invocation.

    The function will then invoke the UPDATE statement and check for
    "success" one or more times, up to a maximum of that passed as
    ``attempts``.

    The initial check for "success" from the UPDATE statement is that the
    number of rows returned matches 1.  If zero rows are matched, then
    the UPDATE statement is assumed to have "failed", and the failure handling
    phase begins.

    The failure handling phase involves invoking the given ``handle_failure``
    function, if any.  This handler can perform additional queries to attempt
    to figure out why the UPDATE didn't match any rows.  The handler,
    upon detection of the exact failure condition, should throw an exception
    to exit; if it doesn't, it has the option of returning True or False,
    where False means the error was not handled, and True means that there
    was not in fact an error, and the function should return successfully.

    If the failure handler is not present, or returns False after ``attempts``
    number of attempts, then the function overall raises CantUpdateException.
    If the handler returns True, then the function returns with no error.

    The return value of the function is a persistent version of the given
    specimen; this may be the specimen itself, if no matching object were
    already present in the session; otherwise, the existing object is
    returned, with the state of the specimen merged into it.  The returned
    persistent object will have the given values populated into the object.

    The object is is returned as "persistent", meaning that it is
    associated with the given
    Session and has an identity key (that is, a real primary key
    value).

    In order to produce this identity key, a strategy must be used to
    determine it as efficiently and safely as possible:

    1. If the given specimen already contained its primary key attributes
       fully populated, then these attributes were used as criteria in the
       UPDATE, so we have the primary key value; it is populated directly.

    2. If the target backend supports RETURNING, then when the update() query
       is performed with a RETURNING clause so that the matching primary key
       is returned atomically.  This currently includes Postgresql, Oracle
       and others (notably not MySQL or SQLite).

    3. If the target backend is MySQL, and the given model uses a
       single-column, AUTO_INCREMENT integer primary key value (as is
       the case for Nova), MySQL's recommended approach of making use
       of ``LAST_INSERT_ID(expr)`` is used to atomically acquire the
       matching primary key value within the scope of the UPDATE
       statement, then it fetched immediately following by using
       ``SELECT LAST_INSERT_ID()``.
       http://dev.mysql.com/doc/refman/5.0/en/information-\
       functions.html#function_last-insert-id

    4. Otherwise, for composite keys on MySQL or other backends such
       as SQLite, the row as UPDATED must be re-fetched in order to
       acquire the primary key value.  The ``surrogate_key``
       parameter is used for this in order to re-fetch the row; this
       is a column name with a known, unique value where
       the object can be fetched.


    """

    if values is None:
        values = {}

    entity = inspect(specimen)
    mapper = entity.mapper
    if [desc['type'] for desc in query.column_descriptions] != \
        [mapper.class_]:
        raise AssertionError("Query does not match given specimen")

    criteria = manufacture_entity_criteria(
        specimen, include_only=include_only, exclude=[surrogate_key])

    query = query.filter(criteria)

    if process_query:
        query = process_query(query)

    surrogate_key_arg = (
        surrogate_key, entity.attrs[surrogate_key].loaded_value)
    pk_value = None

    for attempt in range(attempts):
        try:
            pk_value = query.update_returning_pk(values, surrogate_key_arg)
        except MultiRowsMatched:
            raise
        except NoRowsMatched:
            if handle_failure and handle_failure(query):
                break
        else:
            break
    else:
        raise NoRowsMatched("Zero rows matched for %d attempts" % attempts)

    if pk_value is None:
        pk_value = entity.mapper.primary_key_from_instance(specimen)

    # NOTE(mdbooth): Can't pass the original specimen object here as it might
    # have lists of multiple potential values rather than actual values.
    values = copy.copy(values)
    values[surrogate_key] = surrogate_key_arg[1]
    persistent_obj = manufacture_persistent_object(
        query.session, specimen.__class__(), values, pk_value)

    return persistent_obj


def manufacture_persistent_object(
        session, specimen, values=None, primary_key=None):
    """Make an ORM-mapped object persistent in a Session without SQL.

    The persistent object is returned.

    If a matching object is already present in the given session, the specimen
    is merged into it and the persistent object returned.  Otherwise, the
    specimen itself is made persistent and is returned.

    The object must contain a full primary key, or provide it via the values or
    primary_key parameters.  The object is peristed to the Session in a "clean"
    state with no pending changes.

    :param session: A Session object.

    :param specimen: a mapped object which is typically transient.

    :param values: a dictionary of values to be applied to the specimen,
     in addition to the state that's already on it.  The attributes will be
     set such that no history is created; the object remains clean.

    :param primary_key: optional tuple-based primary key.  This will also
     be applied to the instance if present.


    """
    state = inspect(specimen)
    mapper = state.mapper

    for k, v in values.items():
        orm.attributes.set_committed_value(specimen, k, v)

    pk_attrs = [
        mapper.get_property_by_column(col).key
        for col in mapper.primary_key
    ]

    if primary_key is not None:
        for key, value in zip(pk_attrs, primary_key):
            orm.attributes.set_committed_value(
                specimen,
                key,
                value
            )

    for key in pk_attrs:
        if state.attrs[key].loaded_value is orm.attributes.NO_VALUE:
            raise ValueError("full primary key must be present")

    orm.make_transient_to_detached(specimen)

    if state.key not in session.identity_map:
        session.add(specimen)
        return specimen
    else:
        return session.merge(specimen, load=False)


def manufacture_entity_criteria(entity, include_only=None, exclude=None):
    """Given a mapped instance, produce a WHERE clause.

    The attributes set upon the instance will be combined to produce
    a SQL expression using the mapped SQL expressions as the base
    of comparison.

    Values on the instance may be set as tuples in which case the
    criteria will produce an IN clause.  None is also acceptable as a
    scalar or tuple entry, which will produce IS NULL that is properly
    joined with an OR against an IN expression if appropriate.

    :param entity: a mapped entity.

    :param include_only: optional sequence of keys to limit which
     keys are included.

    :param exclude: sequence of keys to exclude

    """

    state = inspect(entity)
    exclude = set(exclude) if exclude is not None else set()

    existing = dict(
        (attr.key, attr.loaded_value)
        for attr in state.attrs
        if attr.loaded_value is not orm.attributes.NO_VALUE and
        attr.key not in exclude
    )
    if include_only:
        existing = dict(
            (k, existing[k])
            for k in set(existing).intersection(include_only)
        )

    return manufacture_criteria(state.mapper, existing)


def manufacture_criteria(mapped, values):
    """Given a mapper/class and a namespace of values, produce a WHERE clause.

    The class should be a mapped class and the entries in the dictionary
    correspond to mapped attribute names on the class.

    A value may also be a tuple in which case that particular attribute
    will be compared to a tuple using IN.   The scalar value or
    tuple can also contain None which translates to an IS NULL, that is
    properly joined with OR against an IN expression if appropriate.

    :param cls: a mapped class, or actual :class:`.Mapper` object.

    :param values: dictionary of values.

    """

    mapper = inspect(mapped)

    # organize keys using mapped attribute ordering, which is deterministic
    value_keys = set(values)
    keys = [k for k in mapper.column_attrs.keys() if k in value_keys]
    return sql.and_(*[
        _sql_crit(mapper.column_attrs[key].expression, values[key])
        for key in keys
    ])


def _sql_crit(expression, value):
    """Produce an equality expression against the given value.

    This takes into account a value that is actually a collection
    of values, as well as a value of None or collection that contains
    None.

    """

    values = utils.to_list(value, default=(None, ))
    if len(values) == 1:
        if values[0] is None:
            return expression == sql.null()
        else:
            return expression == values[0]
    elif _none_set.intersection(values):
        return sql.or_(
            expression == sql.null(),
            _sql_crit(expression, set(values).difference(_none_set))
        )
    else:
        return expression.in_(values)


def update_returning_pk(query, values, surrogate_key):
    """Perform an UPDATE, returning the primary key of the matched row.

    The primary key is returned using a selection of strategies:

    * if the database supports RETURNING, RETURNING is used to retrieve
      the primary key values inline.

    * If the database is MySQL and the entity is mapped to a single integer
      primary key column, MySQL's last_insert_id() function is used
      inline within the UPDATE and then upon a second SELECT to get the
      value.

    * Otherwise, a "refetch" strategy is used, where a given "surrogate"
      key value (typically a UUID column on the entity) is used to run
      a new SELECT against that UUID.   This UUID is also placed into
      the UPDATE query to ensure the row matches.

    :param query: a Query object with existing criterion, against a single
     entity.

    :param values: a dictionary of values to be updated on the row.

    :param surrogate_key: a tuple of (attrname, value), referring to a
     UNIQUE attribute that will also match the row.  This attribute is used
     to retrieve the row via a SELECT when no optimized strategy exists.

    :return: the primary key, returned as a tuple.
     Is only returned if rows matched is one.  Otherwise, CantUpdateException
     is raised.

    """

    entity = query.column_descriptions[0]['type']
    mapper = inspect(entity).mapper
    session = query.session

    bind = session.connection(mapper=mapper)
    if bind.dialect.implicit_returning:
        pk_strategy = _pk_strategy_returning
    elif bind.dialect.name == 'mysql' and \
        len(mapper.primary_key) == 1 and \
        isinstance(
            mapper.primary_key[0].type, sqltypes.Integer):
        pk_strategy = _pk_strategy_mysql_last_insert_id
    else:
        pk_strategy = _pk_strategy_refetch

    return pk_strategy(query, mapper, values, surrogate_key)


def _assert_single_row(rows_updated):
    if rows_updated == 1:
        return rows_updated
    elif rows_updated > 1:
        raise MultiRowsMatched("%d rows matched; expected one" % rows_updated)
    else:
        raise NoRowsMatched("No rows matched the UPDATE")


def _pk_strategy_refetch(query, mapper, values, surrogate_key):

    surrogate_key_name, surrogate_key_value = surrogate_key
    surrogate_key_col = mapper.attrs[surrogate_key_name].expression

    rowcount = query.\
        filter(surrogate_key_col == surrogate_key_value).\
        update(values, synchronize_session=False)

    _assert_single_row(rowcount)
    # SELECT my_table.id AS my_table_id FROM my_table
    # WHERE my_table.y = ? AND my_table.z = ?
    # LIMIT ? OFFSET ?
    fetch_query = query.session.query(
        *mapper.primary_key).filter(
        surrogate_key_col == surrogate_key_value)

    primary_key = fetch_query.one()

    return primary_key


def _pk_strategy_returning(query, mapper, values, surrogate_key):
    surrogate_key_name, surrogate_key_value = surrogate_key
    surrogate_key_col = mapper.attrs[surrogate_key_name].expression

    update_stmt = _update_stmt_from_query(mapper, query, values)
    update_stmt = update_stmt.where(surrogate_key_col == surrogate_key_value)
    update_stmt = update_stmt.returning(*mapper.primary_key)

    # UPDATE my_table SET x=%(x)s, z=%(z)s WHERE my_table.y = %(y_1)s
    # AND my_table.z = %(z_1)s RETURNING my_table.id
    result = query.session.execute(update_stmt)
    rowcount = result.rowcount
    _assert_single_row(rowcount)
    primary_key = tuple(result.first())

    return primary_key


def _pk_strategy_mysql_last_insert_id(query, mapper, values, surrogate_key):

    surrogate_key_name, surrogate_key_value = surrogate_key
    surrogate_key_col = mapper.attrs[surrogate_key_name].expression

    surrogate_pk_col = mapper.primary_key[0]
    update_stmt = _update_stmt_from_query(mapper, query, values)
    update_stmt = update_stmt.where(surrogate_key_col == surrogate_key_value)
    update_stmt = update_stmt.values(
        {surrogate_pk_col: sql.func.last_insert_id(surrogate_pk_col)})

    # UPDATE my_table SET id=last_insert_id(my_table.id),
    # x=%s, z=%s WHERE my_table.y = %s AND my_table.z = %s
    result = query.session.execute(update_stmt)
    rowcount = result.rowcount
    _assert_single_row(rowcount)
    # SELECT last_insert_id() AS last_insert_id_1
    primary_key = query.session.scalar(sql.func.last_insert_id()),

    return primary_key


def _update_stmt_from_query(mapper, query, values):
    upd_values = dict(
        (
            mapper.column_attrs[key], value
        ) for key, value in values.items()
    )
    primary_table = inspect(query.column_descriptions[0]['entity']).local_table
    where_criteria = query.whereclause
    update_stmt = sql.update(
        primary_table,
    ).where(
        where_criteria,
    ).values(upd_values)
    return update_stmt


_none_set = frozenset([None])


class CantUpdateException(Exception):
    pass


class NoRowsMatched(CantUpdateException):
    pass


class MultiRowsMatched(CantUpdateException):
    pass
