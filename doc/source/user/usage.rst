=======
 Usage
=======

To use oslo.db in a project:

Session Handling
================

Session handling is achieved using the :mod:`oslo_db.sqlalchemy.enginefacade`
system.   This module presents a function decorator as well as a
context manager approach to delivering :class:`.session.Session` as well as
:class:`.Connection` objects to a function or block.

Both calling styles require the use of a context object.   This object may
be of any class, though when used with the decorator form, requires
special instrumentation.

The context manager form is as follows:

.. code:: python


    from oslo_db.sqlalchemy import enginefacade


    class MyContext(object):
        "User-defined context class."


    def some_reader_api_function(context):
        with enginefacade.reader.using(context) as session:
            return session.query(SomeClass).all()


    def some_writer_api_function(context, x, y):
        with enginefacade.writer.using(context) as session:
            session.add(SomeClass(x, y))


    def run_some_database_calls():
        context = MyContext()

        results = some_reader_api_function(context)
        some_writer_api_function(context, 5, 10)


The decorator form accesses attributes off the user-defined context
directly; the context must be decorated with the
:func:`oslo_db.sqlalchemy.enginefacade.transaction_context_provider`
decorator.   Each function must receive the context argument:

.. code:: python


    from oslo_db.sqlalchemy import enginefacade

    @enginefacade.transaction_context_provider
    class MyContext(object):
        "User-defined context class."

    @enginefacade.reader
    def some_reader_api_function(context):
        return context.session.query(SomeClass).all()


    @enginefacade.writer
    def some_writer_api_function(context, x, y):
        context.session.add(SomeClass(x, y))


    def run_some_database_calls():
        context = MyContext()

        results = some_reader_api_function(context)
        some_writer_api_function(context, 5, 10)


``connection`` modifier can be used when a :class:`.session.Session` object is not
needed, e.g. when `SQLAlchemy Core <http://docs.sqlalchemy.org/en/latest/core/>`_
is preferred:

.. code:: python

    @enginefacade.reader.connection
    def _refresh_from_db(context, cache):
        sel = sa.select(table.c.id, table.c.name)
        res = context.connection.execute(sel).fetchall()
        cache.id_cache = {r[1]: r[0] for r in res}
        cache.str_cache = {r[0]: r[1] for r in res}


.. note::  The ``context.session`` and ``context.connection`` attributes
   must be accessed within the scope of an appropriate writer/reader block
   (either the decorator or contextmanager approach). An AttributeError is
   raised otherwise.


The decorator form can also be used with class and instance methods which
implicitly receive the first positional argument:

.. code:: python

    class DatabaseAccessLayer(object):

        @classmethod
        @enginefacade.reader
        def some_reader_api_function(cls, context):
            return context.session.query(SomeClass).all()

        @enginefacade.writer
        def some_writer_api_function(self, context, x, y):
            context.session.add(SomeClass(x, y))

.. note:: Note that enginefacade decorators must be applied **before**
   `classmethod`, otherwise you will get a ``TypeError`` at import time
   (as enginefacade will try to use ``inspect.getargspec()`` on a descriptor,
   not on a bound method, please refer to the `Data Model
   <https://docs.python.org/3/reference/datamodel.html#data-model>`_ section
   of the Python Language Reference for details).


The scope of transaction and connectivity for both approaches is managed
transparently.   The configuration for the connection comes from the standard
:obj:`oslo_config.cfg.CONF` collection.  Additional configurations can be
established for the enginefacade using the
:func:`oslo_db.sqlalchemy.enginefacade.configure` function, before any use of
the database begins:

.. code:: python

    from oslo_db.sqlalchemy import enginefacade

    enginefacade.configure(
        sqlite_fk=True,
        max_retries=5,
        mysql_sql_mode='ANSI'
    )


Base class for models usage
===========================

.. code:: python

    from oslo_db.sqlalchemy import models


    class ProjectSomething(models.TimestampMixin,
                           models.ModelBase):
        id = Column(Integer, primary_key=True)
        ...


DB API backend support
======================

.. code:: python

    from oslo_config import cfg
    from oslo_db import api as db_api


    _BACKEND_MAPPING = {'sqlalchemy': 'project.db.sqlalchemy.api'}

    IMPL = db_api.DBAPI.from_config(cfg.CONF, backend_mapping=_BACKEND_MAPPING)

    def get_engine():
        return IMPL.get_engine()

    def get_session():
        return IMPL.get_session()

    # DB-API method
    def do_something(somethind_id):
        return IMPL.do_something(somethind_id)

DB migration extensions
=======================

Available extensions for `oslo_db.migration`.

.. list-plugins:: oslo_db.sqlalchemy.migration
    :detailed:
