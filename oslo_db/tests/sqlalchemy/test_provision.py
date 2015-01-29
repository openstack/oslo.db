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


from sqlalchemy import inspect
from sqlalchemy import schema
from sqlalchemy import types

from oslo_db.sqlalchemy import test_base


class DropAllObjectsTest(test_base.DbTestCase):

    def setUp(self):
        super(DropAllObjectsTest, self).setUp()

        self.metadata = metadata = schema.MetaData()
        schema.Table(
            'a', metadata,
            schema.Column('id', types.Integer, primary_key=True),
            mysql_engine='InnoDB'
        )
        schema.Table(
            'b', metadata,
            schema.Column('id', types.Integer, primary_key=True),
            schema.Column('a_id', types.Integer, schema.ForeignKey('a.id')),
            mysql_engine='InnoDB'
        )
        schema.Table(
            'c', metadata,
            schema.Column('id', types.Integer, primary_key=True),
            schema.Column('b_id', types.Integer, schema.ForeignKey('b.id')),
            schema.Column(
                'd_id', types.Integer,
                schema.ForeignKey('d.id', use_alter=True, name='c_d_fk')),
            mysql_engine='InnoDB'
        )
        schema.Table(
            'd', metadata,
            schema.Column('id', types.Integer, primary_key=True),
            schema.Column('c_id', types.Integer, schema.ForeignKey('c.id')),
            mysql_engine='InnoDB'
        )

        metadata.create_all(self.engine, checkfirst=False)
        # will drop nothing if the test worked
        self.addCleanup(metadata.drop_all, self.engine, checkfirst=True)

    def test_drop_all(self):
        insp = inspect(self.engine)
        self.assertEqual(
            set(['a', 'b', 'c', 'd']),
            set(insp.get_table_names())
        )

        self.provision.drop_all_objects()

        insp = inspect(self.engine)
        self.assertEqual(
            [],
            insp.get_table_names()
        )


class MySQLRetainSchemaTest(
        DropAllObjectsTest, test_base.MySQLOpportunisticTestCase):
    pass


class PostgresqlRetainSchemaTest(
        DropAllObjectsTest, test_base.PostgreSQLOpportunisticTestCase):
    pass
