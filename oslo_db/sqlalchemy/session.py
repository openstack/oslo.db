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

"""Session Handling for SQLAlchemy backend.

Recommended ways to use sessions within this framework:

* Use the ``enginefacade`` system for connectivity, session and
  transaction management:

  .. code-block:: python

    from oslo_db.sqlalchemy import enginefacade

    @enginefacade.reader
    def get_foo(context, foo):
        return (model_query(models.Foo, context.session).
                filter_by(foo=foo).
                first())

    @enginefacade.writer
    def update_foo(context, id, newfoo):
        (model_query(models.Foo, context.session).
                filter_by(id=id).
                update({'foo': newfoo}))

    @enginefacade.writer
    def create_foo(context, values):
        foo_ref = models.Foo()
        foo_ref.update(values)
        foo_ref.save(context.session)
        return foo_ref

  In the above system, transactions are committed automatically, and
  are shared among all dependent database methods.   Ensure
  that methods which "write" data are enclosed within @writer blocks.

  .. note:: Statements in the session scope will not be automatically retried.

* If you create models within the session, they need to be added, but you
  do not need to call `model.save()`:

  .. code-block:: python

    @enginefacade.writer
    def create_many_foo(context, foos):
        for foo in foos:
            foo_ref = models.Foo()
            foo_ref.update(foo)
            context.session.add(foo_ref)

    @enginefacade.writer
    def update_bar(context, foo_id, newbar):
        foo_ref = (model_query(models.Foo, context.session).
                    filter_by(id=foo_id).
                    first())
        (model_query(models.Bar, context.session).
                    filter_by(id=foo_ref['bar_id']).
                    update({'bar': newbar}))

  The two queries in `update_bar` can alternatively be expressed using
  a single query, which may be more efficient depending on scenario:

  .. code-block:: python

    @enginefacade.writer
    def update_bar(context, foo_id, newbar):
        subq = (model_query(models.Foo.id, context.session).
                filter_by(id=foo_id).
                limit(1).
                subquery())
        (model_query(models.Bar, context.session).
                filter_by(id=subq.as_scalar()).
                update({'bar': newbar}))

  For reference, this emits approximately the following SQL statement:

  .. code-block:: sql

    UPDATE bar SET bar = '${newbar}'
        WHERE id=(SELECT bar_id FROM foo WHERE id = '${foo_id}' LIMIT 1);

  .. note:: `create_duplicate_foo` is a trivially simple example of catching an
     exception while using a savepoint. Here we create two duplicate
     instances with same primary key, must catch the exception out of context
     managed by a single session:

  .. code-block:: python

    @enginefacade.writer
    def create_duplicate_foo(context):
        foo1 = models.Foo()
        foo2 = models.Foo()
        foo1.id = foo2.id = 1
        try:
            with context.session.begin_nested():
                session.add(foo1)
                session.add(foo2)
        except exception.DBDuplicateEntry as e:
            handle_error(e)

* The enginefacade system eliminates the need to decide when sessions need
  to be passed between methods.   All methods should instead share a common
  context object; the enginefacade system will maintain the transaction
  across method calls.

  .. code-block:: python

    @enginefacade.writer
    def myfunc(context, foo):
        # do some database things
        bar = _private_func(context, foo)
        return bar

    def _private_func(context, foo):
        with enginefacade.using_writer(context) as session:
            # do some other database things
            session.add(SomeObject())
        return bar


* Avoid ``with_lockmode('UPDATE')`` when possible.

  FOR UPDATE is not compatible with MySQL/Galera.   Instead, an "opportunistic"
  approach should be used, such that if an UPDATE fails, the entire
  transaction should be retried.  The @wrap_db_retry decorator is one
  such system that can be used to achieve this.

Enabling soft deletes:

* To use/enable soft-deletes, `SoftDeleteMixin` may be used. For example:

  .. code-block:: python

      class NovaBase(models.SoftDeleteMixin, models.ModelBase):
          pass


Efficient use of soft deletes:

* While there is a ``model.soft_delete()`` method, prefer
  ``query.soft_delete()``. Some examples:

  .. code-block:: python

        @enginefacade.writer
        def soft_delete_bar(context):
            # synchronize_session=False will prevent the ORM from attempting
            # to search the Session for instances matching the DELETE;
            # this is typically not necessary for small operations.
            count = model_query(BarModel, context.session).\\
                find(some_condition).soft_delete(synchronize_session=False)
            if count == 0:
                raise Exception("0 entries were soft deleted")

        @enginefacade.writer
        def complex_soft_delete_with_synchronization_bar(context):
            # use synchronize_session='evaluate' when you'd like to attempt
            # to update the state of the Session to match that of the DELETE.
            # This is potentially helpful if the operation is complex and
            # continues to work with instances that were loaded, though
            # not usually needed.
            count = (model_query(BarModel, context.session).
                        find(some_condition).
                        soft_delete(synchronize_session='evaulate'))
            if count == 0:
                raise Exception("0 entries were soft deleted")


"""

from oslo_db.sqlalchemy import enginefacade
from oslo_db.sqlalchemy import engines
from oslo_db.sqlalchemy import orm

EngineFacade = enginefacade.LegacyEngineFacade
create_engine = engines.create_engine
get_maker = orm.get_maker
Query = orm.Query
Session = orm.Session


__all__ = ["EngineFacade", "create_engine", "get_maker", "Query", "Session"]
