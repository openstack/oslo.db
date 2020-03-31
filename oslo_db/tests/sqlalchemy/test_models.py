# Copyright 2012 Cloudscaling Group, Inc.
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

from collections import abc
import datetime
from unittest import mock

from oslotest import base as oslo_test
from sqlalchemy import Column
from sqlalchemy import Integer, String
from sqlalchemy import event
from sqlalchemy.ext.declarative import declarative_base

from oslo_db.sqlalchemy import models
from oslo_db.tests.sqlalchemy import base as test_base


BASE = declarative_base()


class ModelBaseTest(test_base._DbTestCase):
    def setUp(self):
        super(ModelBaseTest, self).setUp()
        self.mb = models.ModelBase()
        self.ekm = ExtraKeysModel()

    def test_modelbase_has_dict_methods(self):
        dict_methods = ('__getitem__',
                        '__setitem__',
                        '__contains__',
                        'get',
                        'update',
                        'save',
                        'items',
                        'iteritems',
                        'keys')
        for method in dict_methods:
            self.assertTrue(hasattr(models.ModelBase, method),
                            "Method %s() is not found" % method)

    def test_modelbase_is_iterable(self):
        self.assertTrue(issubclass(models.ModelBase, abc.Iterable))

    def test_modelbase_set(self):
        self.mb['world'] = 'hello'
        self.assertEqual('hello', self.mb['world'])

    def test_modelbase_update(self):
        h = {'a': '1', 'b': '2'}
        self.mb.update(h)
        for key in h.keys():
            self.assertEqual(h[key], self.mb[key])

    def test_modelbase_contains(self):
        mb = models.ModelBase()
        h = {'a': '1', 'b': '2'}
        mb.update(h)
        for key in h.keys():
            # Test 'in' syntax (instead of using .assertIn)
            self.assertIn(key, mb)

        self.assertNotIn('non-existent-key', mb)

    def test_modelbase_contains_exc(self):
        class ErrorModel(models.ModelBase):
            @property
            def bug(self):
                raise ValueError

        model = ErrorModel()
        model.update({'attr': 5})

        self.assertIn('attr', model)
        self.assertRaises(ValueError, lambda: 'bug' in model)

    def test_modelbase_items_iteritems(self):
        h = {'a': '1', 'b': '2'}
        expected = {
            'id': None,
            'smth': None,
            'name': 'NAME',
            'a': '1',
            'b': '2',
        }
        self.ekm.update(h)
        self.assertEqual(expected, dict(self.ekm.items()))
        self.assertEqual(expected, dict(self.ekm.iteritems()))

    def test_modelbase_dict(self):
        h = {'a': '1', 'b': '2'}
        expected = {
            'id': None,
            'smth': None,
            'name': 'NAME',
            'a': '1',
            'b': '2',
        }
        self.ekm.update(h)
        self.assertEqual(expected, dict(self.ekm))

    def test_modelbase_iter(self):
        expected = {
            'id': None,
            'smth': None,
            'name': 'NAME',
        }
        i = iter(self.ekm)
        found_items = 0
        while True:
            r = next(i, None)
            if r is None:
                break
            self.assertEqual(expected[r[0]], r[1])
            found_items += 1

        self.assertEqual(len(expected), found_items)

    def test_modelbase_keys(self):
        self.assertEqual(set(('id', 'smth', 'name')), set(self.ekm.keys()))

        self.ekm.update({'a': '1', 'b': '2'})
        self.assertEqual(set(('a', 'b', 'id', 'smth', 'name')),
                         set(self.ekm.keys()))

    def test_modelbase_several_iters(self):
        mb = ExtraKeysModel()
        it1 = iter(mb)
        it2 = iter(mb)

        self.assertFalse(it1 is it2)
        self.assertEqual(dict(mb), dict(it1))
        self.assertEqual(dict(mb), dict(it2))

    def test_extra_keys_empty(self):
        """Test verifies that by default extra_keys return empty list."""
        self.assertEqual([], self.mb._extra_keys)

    def test_extra_keys_defined(self):
        """Property _extra_keys will return list with attributes names."""
        self.assertEqual(['name'], self.ekm._extra_keys)

    def test_model_with_extra_keys(self):
        data = dict(self.ekm)
        self.assertEqual({'smth': None,
                          'id': None,
                          'name': 'NAME'},
                         data)


class ExtraKeysModel(BASE, models.ModelBase):
    __tablename__ = 'test_model'

    id = Column(Integer, primary_key=True)
    smth = Column(String(255))

    @property
    def name(self):
        return 'NAME'

    @property
    def _extra_keys(self):
        return ['name']


class TimestampMixinTest(oslo_test.BaseTestCase):

    def test_timestampmixin_attr(self):
        methods = ('created_at',
                   'updated_at')
        for method in methods:
            self.assertTrue(hasattr(models.TimestampMixin, method),
                            "Method %s() is not found" % method)


class SoftDeletedModel(BASE, models.ModelBase, models.SoftDeleteMixin):
    __tablename__ = 'test_model_soft_deletes'

    id = Column('id', Integer, primary_key=True)
    smth = Column('smth', String(255))


class SoftDeleteMixinTest(test_base._DbTestCase):
    def setUp(self):
        super(SoftDeleteMixinTest, self).setUp()

        t = BASE.metadata.tables['test_model_soft_deletes']
        t.create(self.engine)
        self.addCleanup(t.drop, self.engine)

        self.session = self.sessionmaker(autocommit=False)
        self.addCleanup(self.session.close)

    @mock.patch('oslo_utils.timeutils.utcnow')
    def test_soft_delete(self, mock_utcnow):
        dt = datetime.datetime.utcnow().replace(microsecond=0)
        mock_utcnow.return_value = dt

        m = SoftDeletedModel(id=123456, smth='test')
        self.session.add(m)
        self.session.commit()
        self.assertEqual(0, m.deleted)
        self.assertIsNone(m.deleted_at)

        m.soft_delete(self.session)
        self.assertEqual(123456, m.deleted)
        self.assertIs(dt, m.deleted_at)

    def test_soft_delete_coerce_deleted_to_integer(self):
        def listener(conn, cur, stmt, params, context, executemany):
            if 'insert' in stmt.lower():  # ignore SELECT 1 and BEGIN
                self.assertNotIn('False', str(params))

        event.listen(self.engine, 'before_cursor_execute', listener)
        self.addCleanup(event.remove,
                        self.engine, 'before_cursor_execute', listener)

        m = SoftDeletedModel(id=1, smth='test', deleted=False)
        self.session.add(m)
        self.session.commit()

    def test_deleted_set_to_null(self):
        m = SoftDeletedModel(id=123456, smth='test')
        self.session.add(m)
        self.session.commit()

        m.deleted = None
        self.session.commit()

        self.assertIsNone(m.deleted)
