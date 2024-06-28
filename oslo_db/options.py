#  Licensed under the Apache License, Version 2.0 (the "License"); you may
#  not use this file except in compliance with the License. You may obtain
#  a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#  License for the specific language governing permissions and limitations
#  under the License.

from oslo_config import cfg


database_opts = [
    cfg.BoolOpt(
        'sqlite_synchronous',
        default=True,
        help='If True, SQLite uses synchronous mode.',
    ),
    cfg.StrOpt(
        'backend',
        default='sqlalchemy',
        help='The back end to use for the database.',
    ),
    cfg.StrOpt(
        'connection',
        help=(
            'The SQLAlchemy connection string to use to connect to '
            'the database.'
        ),
        secret=True,
    ),
    cfg.StrOpt(
        'slave_connection',
        secret=True,
        help=(
            'The SQLAlchemy connection string to use to connect to the '
            'slave database.'
        ),
    ),
    cfg.StrOpt(
        'asyncio_connection',
        help=(
            'The SQLAlchemy asyncio connection string to use to connect to '
            'the database.'
        ),
        secret=True,
    ),
    cfg.StrOpt(
        'asyncio_slave_connection',
        help=(
            'The SQLAlchemy asyncio connection string to use to connect to '
            'the slave database.'
        ),
        secret=True,
    ),
    cfg.StrOpt(
        'mysql_sql_mode',
        default='TRADITIONAL',
        help=(
            'The SQL mode to be used for MySQL sessions. '
            'This option, including the default, overrides any '
            'server-set SQL mode. To use whatever SQL mode '
            'is set by the server configuration, '
            'set this to no value. Example: mysql_sql_mode='
        ),
    ),
    cfg.IntOpt(
        'mysql_wsrep_sync_wait',
        default=None,
        help=(
            'For Galera only, configure wsrep_sync_wait causality '
            'checks on new connections.  Default is None, meaning don\'t '
            'configure any setting.'
        ),
    ),
    cfg.IntOpt(
        'connection_recycle_time',
        default=3600,
        help=(
            'Connections which have been present in the connection '
            'pool longer than this number of seconds will be replaced '
            'with a new one the next time they are checked out from '
            'the pool.'
        ),
    ),
    cfg.IntOpt(
        'max_pool_size',
        default=5,
        help=(
            'Maximum number of SQL connections to keep open in a pool. '
            'Setting a value of 0 indicates no limit.'
        ),
    ),
    cfg.IntOpt(
        'max_retries',
        default=10,
        help=(
            'Maximum number of database connection retries during startup. '
            'Set to -1 to specify an infinite retry count.'
        ),
    ),
    cfg.IntOpt(
        'retry_interval',
        default=10,
        help='Interval between retries of opening a SQL connection.',
    ),
    cfg.IntOpt(
        'max_overflow',
        default=50,
        help='If set, use this value for max_overflow with SQLAlchemy.',
    ),
    cfg.IntOpt(
        'connection_debug',
        default=0,
        min=0,
        max=100,
        help=(
            'Verbosity of SQL debugging information: 0=None, '
            '100=Everything.'
        ),
    ),
    cfg.BoolOpt(
        'connection_trace',
        default=False,
        help='Add Python stack traces to SQL as comment strings.',
    ),
    cfg.IntOpt(
        'pool_timeout',
        help='If set, use this value for pool_timeout with SQLAlchemy.',
    ),
    cfg.BoolOpt(
        'use_db_reconnect',
        default=False,
        help=(
            'Enable the experimental use of database reconnect '
            'on connection lost.'
        ),
    ),
    cfg.IntOpt(
        'db_retry_interval',
        default=1,
        help='Seconds between retries of a database transaction.',
    ),
    cfg.BoolOpt(
        'db_inc_retry_interval',
        default=True,
        help=(
            'If True, increases the interval between retries '
            'of a database operation up to db_max_retry_interval.'
        ),
    ),
    cfg.IntOpt(
        'db_max_retry_interval',
        default=10,
        help=(
            'If db_inc_retry_interval is set, the '
            'maximum seconds between retries of a '
            'database operation.'
        ),
    ),
    cfg.IntOpt(
        'db_max_retries',
        default=20,
        help=(
            'Maximum retries in case of connection error or deadlock '
            'error before error is '
            'raised. Set to -1 to specify an infinite retry '
            'count.'
        ),
    ),
    cfg.StrOpt(
        'connection_parameters',
        default='',
        help=(
            'Optional URL parameters to append onto the connection '
            'URL at connect time; specify as '
            'param1=value1&param2=value2&...'
        ),
    ),
]


def set_defaults(
    conf,
    connection=None,
    max_pool_size=None,
    max_overflow=None,
    pool_timeout=None,
):
    """Set defaults for configuration variables.

    Overrides default options values.

    :param conf: Config instance specified to set default options in it. Using
     of instances instead of a global config object prevents conflicts between
     options declaration.
    :type conf: oslo.config.cfg.ConfigOpts instance.

    :keyword connection: SQL connection string.
        Valid SQLite URL forms are:
        * sqlite:///:memory: (or, sqlite://)
        * sqlite:///relative/path/to/file.db
        * sqlite:////absolute/path/to/file.db
    :type connection: str

    :keyword max_pool_size: maximum connections pool size. The size of the pool
     to be maintained, defaults to 5. This is the largest number of connections
     that will be kept persistently in the pool. Note that the pool begins with
     no connections; once this number of connections is requested, that number
     of connections will remain.
    :type max_pool_size: int
    :default max_pool_size: 5

    :keyword max_overflow: The maximum overflow size of the pool. When the
     number of checked-out connections reaches the size set in pool_size,
     additional connections will be returned up to this limit. When those
     additional connections are returned to the pool, they are disconnected and
     discarded. It follows then that the total number of simultaneous
     connections the pool will allow is pool_size + max_overflow, and the total
     number of "sleeping" connections the pool will allow is pool_size.
     max_overflow can be set to -1 to indicate no overflow limit; no limit will
     be placed on the total number of concurrent connections. Defaults to 10,
     will be used if value of the parameter in `None`.
    :type max_overflow: int
    :default max_overflow: None

    :keyword pool_timeout: The number of seconds to wait before giving up on
     returning a connection. Defaults to 30, will be used if value of the
     parameter is `None`.
    :type pool_timeout: int
    :default pool_timeout: None
    """

    conf.register_opts(database_opts, group='database')

    if connection is not None:
        conf.set_default('connection', connection, group='database')
    if max_pool_size is not None:
        conf.set_default('max_pool_size', max_pool_size, group='database')
    if max_overflow is not None:
        conf.set_default('max_overflow', max_overflow, group='database')
    if pool_timeout is not None:
        conf.set_default('pool_timeout', pool_timeout, group='database')


def list_opts():
    """Returns a list of oslo.config options available in the library.

    The returned list includes all oslo.config options which may be registered
    at runtime by the library.

    Each element of the list is a tuple. The first element is the name of the
    group under which the list of elements in the second element will be
    registered. A group name of None corresponds to the [DEFAULT] group in
    config files.

    The purpose of this is to allow tools like the Oslo sample config file
    generator to discover the options exposed to users by this library.

    :returns: a list of (group_name, opts) tuples
    """
    return [('database', database_opts)]
