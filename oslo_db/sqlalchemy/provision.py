# Copyright 2014 Red Hat
# Copyright 2013 Mirantis.inc
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

"""Provision test environment for specific DB backends"""

import abc
import logging
import os
import random
import re
import string

import sqlalchemy
from sqlalchemy import schema
from sqlalchemy import sql
import testresources

from oslo_db import exception
from oslo_db.sqlalchemy import enginefacade
from oslo_db.sqlalchemy import utils

LOG = logging.getLogger(__name__)


class ProvisionedDatabase:
    """Represents a database engine pointing to a DB ready to run tests.

    backend: an instance of :class:`.Backend`

    enginefacade: an instance of :class:`._TransactionFactory`

    engine: a SQLAlchemy :class:`.Engine`

    db_token: if provision_new_database were used, this is the randomly
              generated name of the database.  Note that with SQLite memory
              connections, this token is ignored.   For a database that
              wasn't actually created, will be None.

    """

    __slots__ = 'backend', 'enginefacade', 'engine', 'db_token'

    def __init__(self, backend, enginefacade, engine, db_token):
        self.backend = backend
        self.enginefacade = enginefacade
        self.engine = engine
        self.db_token = db_token


class Schema:
    """"Represents a database schema that has or will be populated.

    This is a marker object as required by testresources but otherwise
    serves no purpose.

    """
    __slots__ = 'database',


class BackendResource(testresources.TestResourceManager):
    def __init__(self, database_type, ad_hoc_url=None):
        super().__init__()
        self.database_type = database_type
        self.backend = Backend.backend_for_database_type(self.database_type)
        self.ad_hoc_url = ad_hoc_url
        if ad_hoc_url is None:
            self.backend = Backend.backend_for_database_type(
                self.database_type)
        else:
            self.backend = Backend(self.database_type, ad_hoc_url)
            self.backend._verify()

    def make(self, dependency_resources):
        return self.backend

    def clean(self, resource):
        self.backend._dispose()

    def isDirty(self):
        return False


class DatabaseResource(testresources.TestResourceManager):
    """Database resource which connects and disconnects to a URL.

    For SQLite, this means the database is created implicitly, as a result
    of SQLite's usual behavior.  If the database is a file-based URL,
    it will remain after the resource has been torn down.

    For all other kinds of databases, the resource indicates to connect
    and disconnect from that database.

    """

    def __init__(self, database_type, _enginefacade=None,
                 provision_new_database=True, ad_hoc_url=None):
        super().__init__()
        self.database_type = database_type
        self.provision_new_database = provision_new_database

        # NOTE(zzzeek) the _enginefacade is an optional argument
        # here in order to accomodate Neutron's current direct use
        # of the DatabaseResource object.  Within oslo_db's use,
        # the "enginefacade" will always be passed in from the
        # test and/or fixture.
        if _enginefacade:
            self._enginefacade = _enginefacade
        else:
            self._enginefacade = enginefacade._context_manager
        self.resources = [
            ('backend', BackendResource(database_type, ad_hoc_url))
        ]

    def make(self, dependency_resources):
        backend = dependency_resources['backend']
        _enginefacade = self._enginefacade.make_new_manager()

        if self.provision_new_database:
            db_token = _random_ident()
            url = backend.provisioned_database_url(db_token)
            LOG.info(
                "CREATE BACKEND %s TOKEN %s", backend.engine.url, db_token)
            backend.create_named_database(db_token, conditional=True)
        else:
            db_token = None
            url = backend.url

        _enginefacade.configure(
            logging_name="{}@{}".format(self.database_type, db_token))

        _enginefacade._factory._start(connection=url)
        engine = _enginefacade._factory._writer_engine
        return ProvisionedDatabase(backend, _enginefacade, engine, db_token)

    def clean(self, resource):
        if self.provision_new_database:
            LOG.info(
                "DROP BACKEND %s TOKEN %s",
                resource.backend.engine, resource.db_token)
            resource.backend.drop_named_database(resource.db_token)

    def isDirty(self):
        return False


class SchemaResource(testresources.TestResourceManager):

    def __init__(self, database_resource, generate_schema, teardown=False):
        super().__init__()
        self.generate_schema = generate_schema
        self.teardown = teardown
        self.resources = [
            ('database', database_resource)
        ]

    def clean(self, resource):
        LOG.info(
            "DROP ALL OBJECTS, BACKEND %s",
            resource.database.engine.url)
        resource.database.backend.drop_all_objects(
            resource.database.engine)

    def make(self, dependency_resources):
        if self.generate_schema:
            self.generate_schema(dependency_resources['database'].engine)
        return Schema()

    def isDirty(self):
        if self.teardown:
            return True
        else:
            return False


class Backend:
    """Represent a particular database backend that may be provisionable.

    The ``Backend`` object maintains a database type (e.g. database without
    specific driver type, such as "sqlite", "postgresql", etc.),
    a target URL, a base ``Engine`` for that URL object that can be used
    to provision databases and a ``BackendImpl`` which knows how to perform
    operations against this type of ``Engine``.

    """

    backends_by_database_type = {}

    def __init__(self, database_type, url):
        self.database_type = database_type
        self.url = url
        self.verified = False
        self.engine = None
        self.impl = BackendImpl.impl(database_type)
        self.current_dbs = set()

    @classmethod
    def backend_for_database_type(cls, database_type):
        """Return the ``Backend`` for the given database type.

        """
        try:
            backend = cls.backends_by_database_type[database_type]
        except KeyError:
            raise exception.BackendNotAvailable(
                "Backend '%s' is unavailable: No such backend" % database_type)
        else:
            return backend._verify()

    @classmethod
    def all_viable_backends(cls):
        """Return an iterator of all ``Backend`` objects that are present

        and provisionable.

        """

        for backend in cls.backends_by_database_type.values():
            try:
                yield backend._verify()
            except exception.BackendNotAvailable:
                pass

    def _verify(self):
        """Verify that this ``Backend`` is available and provisionable.

        :return: this ``Backend``

        :raises: ``BackendNotAvailable`` if the backend is not available.

        """
        if not self.verified:
            try:
                eng = self._ensure_backend_available(self.url)
            except exception.BackendNotAvailable as bne:
                self._no_engine_reason = str(bne)
                raise
            else:
                self.engine = eng
            finally:
                self.verified = True
        if self.engine is None:
            raise exception.BackendNotAvailable(self._no_engine_reason)
        return self

    @classmethod
    def _ensure_backend_available(cls, url):
        url = utils.make_url(url)
        try:
            eng = sqlalchemy.create_engine(url)
        except ImportError as i_e:
            # SQLAlchemy performs an "import" of the DBAPI module
            # within create_engine().  So if mysql etc.
            # isn't installed, we get an ImportError here.
            LOG.info(
                "The %(dbapi)s backend is unavailable: %(err)s",
                dict(dbapi=url.drivername, err=i_e))
            raise exception.BackendNotAvailable(
                "Backend '%s' is unavailable: No DBAPI installed" %
                url.drivername)
        else:
            try:
                conn = eng.connect()
            except sqlalchemy.exc.DBAPIError as d_e:
                # upon connect, SQLAlchemy calls dbapi.connect().  This
                # usually raises OperationalError and should always at
                # least raise a SQLAlchemy-wrapped DBAPI Error.
                LOG.info(
                    "The %(dbapi)s backend is unavailable: %(err)s",
                    dict(dbapi=url.drivername, err=d_e)
                )
                raise exception.BackendNotAvailable(
                    "Backend '%s' is unavailable: Could not connect" %
                    url.drivername)
            else:
                conn.close()
                return eng

    def _dispose(self):
        """Dispose main resources of this backend."""
        self.impl.dispose(self.engine)

    def create_named_database(self, ident, conditional=False):
        """Create a database with the given name."""

        if not conditional or ident not in self.current_dbs:
            self.current_dbs.add(ident)
            self.impl.create_named_database(
                self.engine, ident, conditional=conditional)

    def drop_named_database(self, ident, conditional=False):
        """Drop a database with the given name."""

        self.impl.drop_named_database(
            self.engine, ident,
            conditional=conditional)
        self.current_dbs.discard(ident)

    def drop_all_objects(self, engine):
        """Drop all database objects.

        Drops all database objects remaining on the default schema of the
        given engine.

        """
        self.impl.drop_all_objects(engine)

    def database_exists(self, ident):
        """Return True if a database of the given name exists."""

        return self.impl.database_exists(self.engine, ident)

    def provisioned_database_url(self, ident):
        """Given the identifier of an anoymous database, return a URL.

        For hostname-based URLs, this typically involves switching just the
        'database' portion of the URL with the given name and creating
        a URL.

        For SQLite URLs, the identifier may be used to create a filename
        or may be ignored in the case of a memory database.

        """
        return self.impl.provisioned_database_url(self.url, ident)

    @classmethod
    def _setup(cls):
        """Initial startup feature will scan the environment for configured

        URLs and place them into the list of URLs we will use for provisioning.

        This searches through OS_TEST_DBAPI_ADMIN_CONNECTION for URLs.  If
        not present, we set up URLs based on the "opportunstic" convention,
        e.g. username+password = "openstack_citest".

        The provisioning system will then use or discard these URLs as they
        are requested, based on whether or not the target database is actually
        found to be available.

        """
        configured_urls = os.getenv('OS_TEST_DBAPI_ADMIN_CONNECTION', None)
        if configured_urls:
            configured_urls = configured_urls.split(";")
        else:
            configured_urls = [
                impl.create_opportunistic_driver_url()
                for impl in BackendImpl.all_impls()
            ]

        for url_str in configured_urls:
            url = utils.make_url(url_str)
            m = re.match(r'([^+]+?)(?:\+(.+))?$', url.drivername)
            database_type = m.group(1)
            Backend.backends_by_database_type[database_type] = \
                Backend(database_type, url)


class BackendImpl(metaclass=abc.ABCMeta):
    """Provide database-specific implementations of key provisioning

    functions.

    ``BackendImpl`` is owned by a ``Backend`` instance which delegates
    to it for all database-specific features.

    """

    default_engine_kwargs = {}

    supports_drop_fk = True

    def dispose(self, engine):
        LOG.info("DISPOSE ENGINE %s", engine)
        engine.dispose()

    @classmethod
    def all_impls(cls):
        """Return an iterator of all possible BackendImpl objects.

        These are BackendImpls that are implemented, but not
        necessarily provisionable.

        """
        for database_type in cls.impl.reg:
            if database_type == '*':
                continue
            yield BackendImpl.impl(database_type)

    @utils.dispatch_for_dialect("*")
    def impl(drivername):
        """Return a ``BackendImpl`` instance corresponding to the

        given driver name.

        This is a dispatched method which will refer to the constructor
        of implementing subclasses.

        """
        raise NotImplementedError(
            "No provision impl available for driver: %s" % drivername)

    def __init__(self, drivername):
        self.drivername = drivername

    @abc.abstractmethod
    def create_opportunistic_driver_url(self):
        """Produce a string url known as the 'opportunistic' URL.

        This URL is one that corresponds to an established OpenStack
        convention for a pre-established database login, which, when
        detected as available in the local environment, is automatically
        used as a test platform for a specific type of driver.

        """

    @abc.abstractmethod
    def create_named_database(self, engine, ident, conditional=False):
        """Create a database with the given name."""

    @abc.abstractmethod
    def drop_named_database(self, engine, ident, conditional=False):
        """Drop a database with the given name."""

    def drop_all_objects(self, engine):
        """Drop all database objects.

        Drops all database objects remaining on the default schema of the
        given engine.

        Per-db implementations will also need to drop items specific to those
        systems, such as sequences, custom types (e.g. pg ENUM), etc.

        """

        with engine.begin() as conn:
            inspector = sqlalchemy.inspect(engine)
            metadata = schema.MetaData()
            tbs = []
            all_fks = []

            for table_name in inspector.get_table_names():
                fks = []
                for fk in inspector.get_foreign_keys(table_name):
                    # note that SQLite reflection does not have names
                    # for foreign keys until SQLAlchemy 1.0
                    if not fk['name']:
                        continue
                    fks.append(
                        schema.ForeignKeyConstraint((), (), name=fk['name'])
                        )
                table = schema.Table(table_name, metadata, *fks)
                tbs.append(table)
                all_fks.extend(fks)

            if self.supports_drop_fk:
                for fkc in all_fks:
                    conn.execute(schema.DropConstraint(fkc))

            for table in tbs:
                conn.execute(schema.DropTable(table))

            self.drop_additional_objects(conn)

    def drop_additional_objects(self, conn):
        pass

    def provisioned_database_url(self, base_url, ident):
        """Return a provisioned database URL.

        Given the URL of a particular database backend and the string
        name of a particular 'database' within that backend, return
        an URL which refers directly to the named database.

        For hostname-based URLs, this typically involves switching just the
        'database' portion of the URL with the given name and creating
        an engine.

        For URLs that instead deal with DSNs, the rules may be more custom;
        for example, the engine may need to connect to the root URL and
        then emit a command to switch to the named database.

        """
        url = utils.make_url(base_url)
        url = url.set(database=ident)

        return url


@BackendImpl.impl.dispatch_for("mysql")
class MySQLBackendImpl(BackendImpl):

    def create_opportunistic_driver_url(self):
        return "mysql+pymysql://openstack_citest:openstack_citest@localhost/"

    def create_named_database(self, engine, ident, conditional=False):
        with engine.begin() as conn:
            if not conditional or not self.database_exists(conn, ident):
                conn.exec_driver_sql("CREATE DATABASE %s" % ident)

    def drop_named_database(self, engine, ident, conditional=False):
        with engine.begin() as conn:
            if not conditional or self.database_exists(conn, ident):
                conn.exec_driver_sql("DROP DATABASE %s" % ident)

    def database_exists(self, engine, ident):
        s = sql.text("SHOW DATABASES LIKE :ident")
        return bool(engine.scalar(s, {'ident': ident}))


@BackendImpl.impl.dispatch_for("sqlite")
class SQLiteBackendImpl(BackendImpl):

    supports_drop_fk = False

    def dispose(self, engine):
        LOG.info("DISPOSE ENGINE %s", engine)
        engine.dispose()
        url = engine.url
        self._drop_url_file(url, True)

    def _drop_url_file(self, url, conditional):
        filename = url.database
        if filename and (not conditional or os.access(filename, os.F_OK)):
            os.remove(filename)

    def create_opportunistic_driver_url(self):
        return "sqlite://"

    def create_named_database(self, engine, ident, conditional=False):
        url = self.provisioned_database_url(engine.url, ident)
        filename = url.database
        if filename and (not conditional or not os.access(filename, os.F_OK)):
            eng = sqlalchemy.create_engine(url)
            eng.connect().close()

    def drop_named_database(self, engine, ident, conditional=False):
        url = self.provisioned_database_url(engine.url, ident)
        filename = url.database
        if filename and (not conditional or os.access(filename, os.F_OK)):
            os.remove(filename)

    def database_exists(self, engine, ident):
        url = self._provisioned_database_url(engine.url, ident)
        filename = url.database
        return not filename or os.access(filename, os.F_OK)

    def provisioned_database_url(self, base_url, ident):
        if base_url.database:
            return utils.make_url("sqlite:////tmp/%s.db" % ident)
        else:
            return base_url


@BackendImpl.impl.dispatch_for("postgresql")
class PostgresqlBackendImpl(BackendImpl):
    def create_opportunistic_driver_url(self):
        return "postgresql+psycopg2://openstack_citest:openstack_citest@localhost/postgres"  # noqa: E501

    def create_named_database(self, engine, ident, conditional=False):
        with engine.connect().execution_options(
            isolation_level="AUTOCOMMIT",
        ) as conn:
            if not conditional or not self.database_exists(conn, ident):
                conn.exec_driver_sql("CREATE DATABASE %s" % ident)

    def drop_named_database(self, engine, ident, conditional=False):
        with engine.connect().execution_options(
            isolation_level="AUTOCOMMIT",
        ) as conn:
            self._close_out_database_users(conn, ident)
            if conditional:
                conn.exec_driver_sql("DROP DATABASE IF EXISTS %s" % ident)
            else:
                conn.exec_driver_sql("DROP DATABASE %s" % ident)

    def drop_additional_objects(self, conn):
        enums = [e['name'] for e in sqlalchemy.inspect(conn).get_enums()]

        for e in enums:
            conn.exec_driver_sql("DROP TYPE %s" % e)

    def database_exists(self, engine, ident):
        return bool(
            engine.scalar(
                sqlalchemy.text(
                    "SELECT datname FROM pg_database WHERE datname=:name"
                ),
                {'name': ident},
            )
        )

    def _close_out_database_users(self, conn, ident):
        """Attempt to guarantee a database can be dropped.

        Optional feature which guarantees no connections with our
        username are attached to the DB we're going to drop.

        This method has caveats; for one, the 'pid' column was named
        'procpid' prior to Postgresql 9.2.  But more critically,
        prior to 9.2 this operation required superuser permissions,
        even if the connections we're closing are under the same username
        as us.   In more recent versions this restriction has been
        lifted for same-user connections.

        """
        if conn.dialect.server_version_info >= (9, 2):
            conn.execute(
                sqlalchemy.text(
                    "SELECT pg_terminate_backend(pid) "
                    "FROM pg_stat_activity "
                    "WHERE usename=current_user AND "
                    "pid != pg_backend_pid() AND "
                    "datname=:dname"
                ),
                {'dname': ident},
            )


def _random_ident():
    return ''.join(random.choice(string.ascii_lowercase) for i in range(10))


Backend._setup()
