=======
 Usage
=======

To use oslo.db in a project:

Session Handling
================

.. code:: python

    from oslo.config import cfg
    from oslo.db.sqlalchemy import session as db_session

    _FACADE = None

    def _create_facade_lazily():
        global _FACADE
        if _FACADE is None:
            _FACADE = db_session.EngineFacade.from_config(cfg.CONF)
        return _FACADE

    def get_engine():
        facade = _create_facade_lazily()
        return facade.get_engine()

    def get_session(**kwargs):
        facade = _create_facade_lazily()
        return facade.get_session(**kwargs)


Base class for models usage
===========================

.. code:: python

    from oslo.db import models


    class ProjectSomething(models.TimestampMixin,
                           models.ModelBase):
        id = Column(Integer, primary_key=True)
        ...


DB API backend support
======================

.. code:: python

    from oslo.config import cfg
    from oslo.db import api as db_api


    _BACKEND_MAPPING = {'sqlalchemy': 'project.db.sqlalchemy.api'}

    IMPL = db_api.DBAPI.from_config(cfg.CONF, backend_mapping=_BACKEND_MAPPING)

    def get_engine():
        return IMPL.get_engine()

    def get_session():
        return IMPL.get_session()

    # DB-API method
    def do_something(somethind_id):
        return IMPL.do_something(somethind_id)
