============
Installation
============

At the command line::

    $ pip install oslo.db

You will also need to install at least one SQL backend::

    $ pip install psycopg2

Or::

    $ pip install PyMySQL

Or::

    $ pip install pysqlite


Using with PostgreSQL
---------------------

If you are using PostgreSQL make sure to install the PostgreSQL client
development package for your distro. On Ubuntu this is done as follows::

    $ sudo apt-get install libpq-dev
    $ pip install psycopg2

The installation of psycopg2 will fail if libpq-dev is not installed first.
Note that even in a virtual environment the libpq-dev will be installed
system wide.


Using with MySQL-python
-----------------------

PyMySQL is a default MySQL DB API driver for oslo.db, as well as for the whole
OpenStack. But you still can use MySQL-python as an alternative DB API driver.
For MySQL-python you must install the MySQL client development package for
your distro. On Ubuntu this is done as follows::

    $ sudo apt-get install libmysqlclient-dev
    $ pip install MySQL-python

The installation of MySQL-python will fail if libmysqlclient-dev is not
installed first. Note that even in a virtual environment the MySQL package will
be installed system wide.
