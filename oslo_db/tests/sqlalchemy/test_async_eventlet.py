# Copyright (c) 2014 Rackspace Hosting
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

"""Unit tests for SQLAlchemy and eventlet interaction."""

import logging
import unittest

from oslo_utils import importutils
import sqlalchemy as sa
from sqlalchemy import orm

from oslo_db import exception as db_exc
from oslo_db.sqlalchemy import models
from oslo_db import tests
from oslo_db.tests.sqlalchemy import base as test_base


class EventletTestMixin(object):
    def setUp(self):
        super(EventletTestMixin, self).setUp()

        BASE = orm.declarative_base()

        class TmpTable(BASE, models.ModelBase):
            __tablename__ = 'test_async_eventlet'
            id = sa.Column('id', sa.Integer, primary_key=True, nullable=False)
            foo = sa.Column('foo', sa.Integer)
            __table_args__ = (
                sa.UniqueConstraint('foo', name='uniq_foo'),
            )

        self.test_table = TmpTable
        TmpTable.__table__.create(self.engine)
        self.addCleanup(lambda: TmpTable.__table__.drop(self.engine))

    @unittest.skipIf(not tests.should_run_eventlet_tests(),
                     'eventlet tests disabled unless TEST_EVENTLET=1')
    def test_concurrent_transaction(self):
        # Cause sqlalchemy to log executed SQL statements.  Useful to
        # determine exactly what and when was sent to DB.
        sqla_logger = logging.getLogger('sqlalchemy.engine')
        sqla_logger.setLevel(logging.INFO)
        self.addCleanup(sqla_logger.setLevel, logging.NOTSET)

        def operate_on_row(name, ready=None, proceed=None):
            logging.debug('%s starting', name)
            _session = self.sessionmaker()
            with _session.begin():
                logging.debug('%s ready', name)

                # Modify the same row, inside transaction
                tbl = self.test_table()
                tbl.update({'foo': 10})
                tbl.save(_session)

                if ready is not None:
                    ready.send()
                if proceed is not None:
                    logging.debug('%s waiting to proceed', name)
                    proceed.wait()
                logging.debug('%s exiting transaction', name)
            logging.debug('%s terminating', name)
            return True

        eventlet = importutils.try_import('eventlet')
        if eventlet is None:
            return self.skipTest('eventlet is required for this test')

        a_ready = eventlet.event.Event()
        a_proceed = eventlet.event.Event()
        b_proceed = eventlet.event.Event()

        # thread A opens transaction
        logging.debug('spawning A')
        a = eventlet.spawn(operate_on_row, 'A',
                           ready=a_ready, proceed=a_proceed)
        logging.debug('waiting for A to enter transaction')
        a_ready.wait()

        # thread B opens transaction on same row
        logging.debug('spawning B')
        b = eventlet.spawn(operate_on_row, 'B',
                           proceed=b_proceed)
        logging.debug('waiting for B to (attempt to) enter transaction')
        eventlet.sleep(1)  # should(?) advance B to blocking on transaction

        # While B is still blocked, A should be able to proceed
        a_proceed.send()

        # Will block forever(*) if DB library isn't reentrant.
        # (*) Until some form of timeout/deadlock detection kicks in.
        # This is the key test that async is working.  If this hangs
        # (or raises a timeout/deadlock exception), then you have failed
        # this test.
        self.assertTrue(a.wait())

        b_proceed.send()
        # If everything proceeded without blocking, B will throw a
        # "duplicate entry" exception when it tries to insert the same row
        self.assertRaises(db_exc.DBDuplicateEntry, b.wait)


# Note that sqlite fails the above concurrency tests, and is not
# mentioned below.
# ie: This file performs no tests by default.

class MySQLEventletTestCase(EventletTestMixin,
                            test_base._MySQLOpportunisticTestCase):
    pass


class PostgreSQLEventletTestCase(EventletTestMixin,
                                 test_base._PostgreSQLOpportunisticTestCase):
    pass
