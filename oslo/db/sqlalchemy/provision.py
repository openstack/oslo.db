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
import argparse
import logging
import os
import random
import re
import string

import six
from six import moves
import sqlalchemy
from sqlalchemy.engine import url as sa_url

from oslo.db._i18n import _LI
from oslo.db import exception
from oslo.db.sqlalchemy import session
from oslo.db.sqlalchemy import utils

LOG = logging.getLogger(__name__)


class ProvisionedDatabase(object):
    """Represent a single database node that can be used for testing in

    a serialized fashion.

    ``ProvisionedDatabase`` includes features for full lifecycle management
    of a node, in a way that is context-specific.   Depending on how the
    test environment runs, ``ProvisionedDatabase`` should know if it needs
    to create and drop databases or if it is making use of a database that
    is maintained by an external process.

    """

    def __init__(self, database_type):
        self.backend = Backend.backend_for_database_type(database_type)
        self.db_token = _random_ident()

        self.backend.create_named_database(self.db_token)
        self.engine = self.backend.provisioned_engine(self.db_token)

    def dispose(self):
        self.engine.dispose()
        self.backend.drop_named_database(self.db_token)


class Backend(object):
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
        Backend.backends_by_database_type[database_type] = self

    @classmethod
    def backend_for_database_type(cls, database_type):
        """Return and verify the ``Backend`` for the given database type.

        Creates the engine if it does not already exist and raises
        ``BackendNotAvailable`` if it cannot be produced.

        :return: a base ``Engine`` that allows provisioning of databases.

        :raises: ``BackendNotAvailable``, if an engine for this backend
         cannot be produced.

        """
        try:
            backend = cls.backends_by_database_type[database_type]
        except KeyError:
            raise exception.BackendNotAvailable(database_type)
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
            except exception.BackendNotAvailable:
                raise
            else:
                self.engine = eng
            finally:
                self.verified = True
        if self.engine is None:
            raise exception.BackendNotAvailable(self.database_type)
        return self

    @classmethod
    def _ensure_backend_available(cls, url):
        url = sa_url.make_url(str(url))
        try:
            eng = sqlalchemy.create_engine(url)
        except ImportError as i_e:
            # SQLAlchemy performs an "import" of the DBAPI module
            # within create_engine().  So if ibm_db_sa, cx_oracle etc.
            # isn't installed, we get an ImportError here.
            LOG.info(
                _LI("The %(dbapi)s backend is unavailable: %(err)s"),
                dict(dbapi=url.drivername, err=i_e))
            raise exception.BackendNotAvailable("No DBAPI installed")
        else:
            try:
                conn = eng.connect()
            except sqlalchemy.exc.DBAPIError as d_e:
                # upon connect, SQLAlchemy calls dbapi.connect().  This
                # usually raises OperationalError and should always at
                # least raise a SQLAlchemy-wrapped DBAPI Error.
                LOG.info(
                    _LI("The %(dbapi)s backend is unavailable: %(err)s"),
                    dict(dbapi=url.drivername, err=d_e)
                )
                raise exception.BackendNotAvailable("Could not connect")
            else:
                conn.close()
                return eng

    def create_named_database(self, ident):
        """Create a database with the given name."""

        self.impl.create_named_database(self.engine, ident)

    def drop_named_database(self, ident, conditional=False):
        """Drop a database with the given name."""

        self.impl.drop_named_database(
            self.engine, ident,
            conditional=conditional)

    def database_exists(self, ident):
        """Return True if a database of the given name exists."""

        return self.impl.database_exists(self.engine, ident)

    def provisioned_engine(self, ident):
        """Given the URL of a particular database backend and the string

        name of a particular 'database' within that backend, return
        an Engine instance whose connections will refer directly to the
        named database.

        For hostname-based URLs, this typically involves switching just the
        'database' portion of the URL with the given name and creating
        an engine.

        For URLs that instead deal with DSNs, the rules may be more custom;
        for example, the engine may need to connect to the root URL and
        then emit a command to switch to the named database.

        """
        return self.impl.provisioned_engine(self.url, ident)

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
            url = sa_url.make_url(url_str)
            m = re.match(r'([^+]+?)(?:\+(.+))?$', url.drivername)
            database_type, drivertype = m.group(1, 2)
            Backend(database_type, url)


@six.add_metaclass(abc.ABCMeta)
class BackendImpl(object):
    """Provide database-specific implementations of key provisioning

    functions.

    ``BackendImpl`` is owned by a ``Backend`` instance which delegates
    to it for all database-specific features.

    """

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

        This URL is one that corresponds to an established Openstack
        convention for a pre-established database login, which, when
        detected as available in the local environment, is automatically
        used as a test platform for a specific type of driver.

        """

    @abc.abstractmethod
    def create_named_database(self, engine, ident):
        """Create a database with the given name."""

    @abc.abstractmethod
    def drop_named_database(self, engine, ident, conditional=False):
        """Drop a database with the given name."""

    def provisioned_engine(self, base_url, ident):
        """Return a provisioned engine.

        Given the URL of a particular database backend and the string
        name of a particular 'database' within that backend, return
        an Engine instance whose connections will refer directly to the
        named database.

        For hostname-based URLs, this typically involves switching just the
        'database' portion of the URL with the given name and creating
        an engine.

        For URLs that instead deal with DSNs, the rules may be more custom;
        for example, the engine may need to connect to the root URL and
        then emit a command to switch to the named database.

        """

        url = sa_url.make_url(str(base_url))
        url.database = ident
        return session.create_engine(
            url,
            logging_name="%s@%s" % (self.drivername, ident))


@BackendImpl.impl.dispatch_for("mysql")
class MySQLBackendImpl(BackendImpl):
    def create_opportunistic_driver_url(self):
        return "mysql://openstack_citest:openstack_citest@localhost/"

    def create_named_database(self, engine, ident):
        with engine.connect() as conn:
            conn.execute("CREATE DATABASE %s" % ident)

    def drop_named_database(self, engine, ident, conditional=False):
        with engine.connect() as conn:
            if not conditional or self.database_exists(conn, ident):
                conn.execute("DROP DATABASE %s" % ident)

    def database_exists(self, engine, ident):
        return bool(engine.scalar("SHOW DATABASES LIKE '%s'" % ident))


@BackendImpl.impl.dispatch_for("sqlite")
class SQLiteBackendImpl(BackendImpl):
    def create_opportunistic_driver_url(self):
        return "sqlite://"

    def create_named_database(self, engine, ident):
        url = self._provisioned_database_url(engine.url, ident)
        eng = sqlalchemy.create_engine(url)
        eng.connect().close()

    def provisioned_engine(self, base_url, ident):
        return session.create_engine(
            self._provisioned_database_url(base_url, ident))

    def drop_named_database(self, engine, ident, conditional=False):
        url = self._provisioned_database_url(engine.url, ident)
        filename = url.database
        if filename and (not conditional or os.access(filename, os.F_OK)):
            os.remove(filename)

    def database_exists(self, engine, ident):
        url = self._provisioned_database_url(engine.url, ident)
        filename = url.database
        return not filename or os.access(filename, os.F_OK)

    def _provisioned_database_url(self, base_url, ident):
        if base_url.database:
            return sa_url.make_url("sqlite:////tmp/%s.db" % ident)
        else:
            return base_url


@BackendImpl.impl.dispatch_for("postgresql")
class PostgresqlBackendImpl(BackendImpl):
    def create_opportunistic_driver_url(self):
        return "postgresql://openstack_citest:openstack_citest"\
            "@localhost/postgres"

    def create_named_database(self, engine, ident):
        with engine.connect().execution_options(
                isolation_level="AUTOCOMMIT") as conn:
            conn.execute("CREATE DATABASE %s" % ident)

    def drop_named_database(self, engine, ident, conditional=False):
        with engine.connect().execution_options(
                isolation_level="AUTOCOMMIT") as conn:
            self._close_out_database_users(conn, ident)
            if conditional:
                conn.execute("DROP DATABASE IF EXISTS %s" % ident)
            else:
                conn.execute("DROP DATABASE %s" % ident)

    def database_exists(self, engine, ident):
        return bool(
            engine.scalar(
                sqlalchemy.text(
                    "select datname from pg_database "
                    "where datname=:name"), name=ident)
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
                    "select pg_terminate_backend(pid) "
                    "from pg_stat_activity "
                    "where usename=current_user and "
                    "pid != pg_backend_pid() "
                    "and datname=:dname"
                ), dname=ident)


def _random_ident():
    return ''.join(
        random.choice(string.ascii_lowercase)
        for i in moves.range(10))


def _echo_cmd(args):
    idents = [_random_ident() for i in moves.range(args.instances_count)]
    print("\n".join(idents))


def _create_cmd(args):
    idents = [_random_ident() for i in moves.range(args.instances_count)]

    for backend in Backend.all_viable_backends():
        for ident in idents:
            backend.create_named_database(ident)

    print("\n".join(idents))


def _drop_cmd(args):
    for backend in Backend.all_viable_backends():
        for ident in args.instances:
            backend.drop_named_database(ident, args.conditional)

Backend._setup()


def main(argv=None):
    """Command line interface to create/drop databases.

    ::create: Create test database with random names.
    ::drop: Drop database created by previous command.
    ::echo: create random names and display them; don't create.
    """
    parser = argparse.ArgumentParser(
        description='Controller to handle database creation and dropping'
        ' commands.',
        epilog='Typically called by the test runner, e.g. shell script, '
        'testr runner via .testr.conf, or other system.')
    subparsers = parser.add_subparsers(
        help='Subcommands to manipulate temporary test databases.')

    create = subparsers.add_parser(
        'create',
        help='Create temporary test databases.')
    create.set_defaults(which=_create_cmd)
    create.add_argument(
        'instances_count',
        type=int,
        help='Number of databases to create.')

    drop = subparsers.add_parser(
        'drop',
        help='Drop temporary test databases.')
    drop.set_defaults(which=_drop_cmd)
    drop.add_argument(
        'instances',
        nargs='+',
        help='List of databases uri to be dropped.')
    drop.add_argument(
        '--conditional',
        action="store_true",
        help="Check if database exists first before dropping"
    )

    echo = subparsers.add_parser(
        'echo',
        help="Create random database names and display only."
    )
    echo.set_defaults(which=_echo_cmd)
    echo.add_argument(
        'instances_count',
        type=int,
        help='Number of identifiers to create.')

    args = parser.parse_args(argv)

    cmd = args.which
    cmd(args)


if __name__ == "__main__":
    main()
