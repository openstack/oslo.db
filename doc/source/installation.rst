============
Installation
============

At the command line::

    $ pip install oslo.db

You will also need to install at least one SQL backend::

    $ pip install MySQL-python

Or::

    $ pip install pysqlite

Using with MySQL
----------------

If using MySQL make sure to install the MySQL client development package for
your distro. On Ubuntu this is done as follows::

    $ sudo apt-get install libmysqlclient-dev
    $ pip install MySQL-python

The installation of MySQL-python will fail if libmysqlclient-dev is not
installed first. Note that even in a virtual environment the MySQL package will
be installed system wide.
