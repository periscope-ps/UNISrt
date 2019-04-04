#!/usr/bin/env python3

# =============================================================================
#  UNIS-RT
#
#  Copyright (c) 2012-2016, Trustees of Indiana University,
#  All rights reserved.
#
#  This software may be modified and distributed under the terms of the BSD
#  license.  See the COPYING file for details.
#
#  This software was created at the Indiana University Center for Research in
#  Extreme Scale Technologies (CREST).
# =============================================================================

"""
UNIS model related tests
"""

import collections
import copy
import unittest
import unittest.mock as mock
from unittest.mock import MagicMock, Mock

from unis.settings import SCHEMAS, DEFAULT_CONFIG
from unis.models import Node, Exnode, Extent, schemaLoader
from unis.models.models import _CACHE, UnisObject, List, Local, _schemaFactory, Context
from unis.models.lists import UnisCollection

_emptyschema = { 'name': 'blank', 'id': 'blank_schema' }
EmptyObject = _schemaFactory(_emptyschema, 'EmptyObject', [type(UnisObject)])('EmptyObject', tuple([UnisObject]), {})

class UnisObjectTest(unittest.TestCase):
    def test_init(self):
        # Arrange
        test_data = {"key1": "value1", "key2": "value2", "$key3": "value3"}
        # Act
        obj1 = EmptyObject()
        obj2 = EmptyObject(test_data)
        expected_raw = {'$schema': 'blank_schema', 'selfRef': ''}
        schema = {'$schema': 'blank_schema', 'selfRef': { 'type': 'string', 'default': ''}}
        # Assert
        self.assertIsInstance(obj1, UnisObject)
        self.assertIsInstance(obj2, UnisObject)
        self.assertEqual(obj1.to_JSON(), expected_raw)
        self.assertEqual(obj2.to_JSON(), {**test_data, **expected_raw})
        for key, value in test_data.items():
            self.assertTrue(hasattr(obj2, key))
            self.assertEqual(getattr(obj2, key), value)
    
    def test_virtual(self):
        # Arrange
        obj1 = EmptyObject()
        
        # Act
        obj1.v = 10
        
        # Assert
        self.assertTrue(hasattr(obj1, "v"))
        self.assertEqual(getattr(obj1, "v"), 10)
        self.assertTrue("v" not in obj1.to_JSON())
        
    def test_extendSchema(self):
        # Arrange
        obj1 = EmptyObject()
        obj1.v = 10
        
        # Act
        obj1.extendSchema("v")
        
        # Assert
        self.assertTrue(hasattr(obj1, "v"))
        self.assertEqual(getattr(obj1, "v"), 10)
        self.assertTrue("v" in obj1.to_JSON())
        
    def test_extendSchema_value(self):
        # Arrange
        obj1 = EmptyObject()
        
        # Act
        obj1.extendSchema("v", 10)
        
        # Assert
        self.assertTrue(hasattr(obj1, "v"))
        self.assertEqual(getattr(obj1, "v"), 10)
        self.assertTrue("v" in obj1.to_JSON())
        self.assertEqual(obj1.to_JSON()['v'], 10)
        
    def test_inner_list(self):
        # Arrange
        obj1 = EmptyObject({"v": ["1", "2", "3"]})
        
        # Act
        vs = obj1.v

        # Assert
        self.assertIsInstance(vs, Context)
        self.assertIsInstance(vs.getObject(), List)
        self.assertEqual(vs.to_JSON(top=False), ["1", "2", "3"])
    
    def test_inner_dict(self):
        # Arrange
        obj1 = EmptyObject({"v": { "a": "1" } })
        
        # Act
        v = obj1.v
        
        # Assert
        self.assertIsInstance(v, Context)
        self.assertIsInstance(v.getObject(), Local)
        self.assertTrue(hasattr(v, "a"))
        self.assertEqual(v.a, "1")
        
class NetworkResourceTest(unittest.TestCase):

    VALID_NODE = {
        "id": "mynodeid",
        "ts": 1234,
        "name": "mynode",
        "ports": [],
        "rules": []
    }
    
    INVALID_NODE = {
        "id": 1234,
        "name": "mybadnode",
        "ports": [],
        "rules": []
    }
    
    def test_init(self):
        node1 = Node()
        node2 = Node(NetworkResourceTest.VALID_NODE)
        self.assertIsInstance(node1, Node)
        self.assertIsInstance(node2, Node)
        self.assertEqual(getattr(node1, '$schema'), SCHEMAS['Node'])
        self.assertEqual(getattr(node2, '$schema'), SCHEMAS['Node'])
        
    def test_validate(self):
        from jsonschema.exceptions import ValidationError
        
        good = Node(NetworkResourceTest.VALID_NODE)
        bad  = Node(NetworkResourceTest.INVALID_NODE)
        
        self.assertEquals(good.validate(), None)
        self.assertRaises(ValidationError, bad.validate)

    def test_validate_on_change(self):
        from jsonschema.exceptions import ValidationError
        def f(res):
            def val():
                setattr(res, "name", 10)
                res.validate()
            return val
        
        
        good = Node(NetworkResourceTest.VALID_NODE)
        bad = Node(NetworkResourceTest.VALID_NODE)
        
        good.name = "modified"
        self.assertEqual(good.name, "modified")
        self.assertRaises(ValidationError, f(bad))
        
class CollectionTest(unittest.TestCase):
    def setUp(self):
        UnisCollection.collections = {}
        
    def runtime(self):
        rt = MagicMock()
        rt.settings = { "namespace": "ut", **DEFAULT_CONFIG }
        return rt
    
    def test_init(self):
        # Act
        col = UnisCollection.get_collection("", Node, self.runtime())
        
        # Assert
        self.assertEqual(len(col), 0)
        self.assertEqual(col._cache, [])
        

    def test_append(self):
        # Arrange
        rt = self.runtime()
        col = UnisCollection.get_collection("", Node, rt)
        n1 = Node({"id": "1"})
        n2 = Node({"id": "2"})
        
        # Act
        col.append(n1.getObject())
        col.append(n2.getObject())
        
        # Assert
        self.assertEqual(len(col), 2)
        self.assertIn("id", col._indices)
        self.assertEqual(col._indices['id'].index('1'), 0)
        self.assertEqual(col._indices['id'].index('2'), 1)
        
    def test_append_duplicate(self):
        # Arrange
        rt = self.runtime()
        col = UnisCollection.get_collection("", Node, rt)
        n1 = Node({"id": "1", "v": 1})
        n2 = Node({"id": "1", "v": 2})
        
        # Act
        col.append(n1.getObject())
        col.append(n2.getObject())
        
        # Assert
        self.assertEqual(len(col), 1)
        self.assertEqual(col._indices['id'].index('1'), 0)
        self.assertEqual(col[0].v, 2)
        
    def test_append_bad(self):
        # Arrange
        rt = self.runtime()
        col = UnisCollection.get_collection("", Node, rt)
        e1 = Exnode()
        
        # Assert
        self.assertRaises(TypeError, col.append, e1.getObject())
        
    def test_setitem(self):
        # Arrange
        rt = self.runtime()
        col = UnisCollection.get_collection("", Node, rt)
        col.createIndex("v")
        n1 = Node({"id": "1", "v": 1})
        n2 = Node({"id": "1", "v": 2})
        
        # Act
        col.append(n1.getObject())
        col[0] = n2
        
        # Assert
        self.assertEqual(len(col), 1)
        self.assertEqual(col._indices['id'].index('1'), 0)
        self.assertEqual(col._indices['v'].index(2), {0})
        self.assertEqual(col[0].v, 2)
        
    def test_iter(self):
        # Arrange
        rt = self.runtime()
        rt._unis.get.return_value = []
        col = UnisCollection.get_collection("", Node, rt)
        
        # Act
        for i in range(100):
            col.append(Node({"id": str(i)}).getObject())
        
        # Assert
        for i, n in enumerate(col):
            self.assertEqual(int(n.id), i)

        self.assertEqual(len(col), 100)
    
    def tes_iter_remote(self):
        # Arrange
        rt = self.runtime()
        rt._unis.get.return_value = [{"id": str(i)} for i in range(100)]
        col = UnisCollection("ref", "nodes", Node, rt)
        
        # Assert
        i = 0
        for n in col:
            self.assertEqual(int(n.id), i)
            i += 1
        self.assertEqual(i, 100)
        for n in col:
            self.assertEqual((n.id, int(n.id)), self._indices["id"])
        rt._unis.get.assert_called_once_with("#/nodes", limit=None, kwargs={'skip': 0})
    
    def test_create_index_before_insert(self):
        # Arrange
        rt = self.runtime()
        col = UnisCollection.get_collection("", Node, rt)
        v = [1, 5, 2]
        
        # Arrange
        col.createIndex("v")
        for i in range(len(v)):
            col.append(Node({'id': str(i), 'v': v[i]}).getObject())
        
        # Assert
        self.assertIn("id", col._indices)
        self.assertIn("v", col._indices)
        for i in range(3):
            self.assertEqual(col._indices['id'].index(str(i)), i)
            self.assertEqual(col._indices['v'].index(v[i]), {i})
    
    def test_create_index_after_insert(self):
        # Arrange
        rt = self.runtime()
        col = UnisCollection.get_collection("", Node, rt)
        v = [1, 5, 2]
        
        # Arrange
        for i in range(len(v)):
            col.append(Node({'id': str(i), 'v': v[i]}).getObject())
        col.createIndex("v")
        
        # Assert
        self.assertIn("id", col._indices)
        self.assertIn("v", col._indices)
        for i in range(3):
            self.assertEqual(col._indices['id'].index(str(i)), i)
            self.assertEqual(col._indices['v'].index(v[i]), {i})
        
    def test_update_index(self):
        # Arrange
        rt = self.runtime()
        col = UnisCollection.get_collection("", Node, rt)
        col.createIndex("v")
        col.append(Node({"id": "1", "v": 1}).getObject())
        col.append(Node({"id": "2", "v": 5}).getObject())
        col.append(Node({"id": "3", "v": 2}).getObject())
        
        # Act
        n = col[0]
        n.v = 10
        col.updateIndex(n)
        
        # Assert
        self.assertIn("id", col._indices)
        self.assertIn("v", col._indices)
        self.assertEqual(col._indices['id'].index('1'), 0)
        self.assertEqual(col._indices['id'].index('2'), 1)
        self.assertEqual(col._indices['id'].index('3'), 2)
        self.assertEqual(col._indices['v'].index(2), {2})
        self.assertEqual(col._indices['v'].index(5), {1})
        self.assertEqual(col._indices['v'].index(10), {0})

    def test_where_single_pre_init(self):
        # Arrange
        rt = self.runtime()
        rt._unis.get.return_value = None
        col = UnisCollection.get_collection("", Node, rt)
        col.createIndex("v")
        col.append(Node({"id": "1", "v": 1}).getObject())
        col.append(Node({"id": "2", "v": 2}).getObject())
        col.append(Node({"id": "3", "v": 3}).getObject())
        
        # Act
        n = col.where({"v": 3})
        n = list(n)
        
        # Assert
        self.assertIsInstance(n, collections.Iterable)
        self.assertEqual(n[0].id, "3")
        self.assertEqual(n[0].v, 3)
        
    def test_where_lt_pre_init(self):
        # Arrange
        rt = self.runtime()
        rt._unis.get.return_value = None
        col = UnisCollection.get_collection("", Node, rt)
        col.createIndex("v")
        nodes = [Node({"id": "1", "v": 1}), Node({"id": "2", "v": 2}), Node({"id": "3", "v": 3})]
        for node in nodes:
            col.append(node.getObject())
        
        # Act
        n = col.where({"v": { "lt": 3 }})
        n = [v for v in n]
        
        # Assert
        self.assertIsInstance(n, collections.Iterable)
        self.assertEqual(len(n), 2)
        self.assertIn(nodes[0], n)
        self.assertIn(nodes[1], n)
        
    def test_where_le_pre_init(self):
        # Arrange
        rt = self.runtime()
        rt._unis.get.return_value = None
        col = UnisCollection.get_collection("", Node, rt)
        col.createIndex("v")
        nodes = [Node({"id": "1", "v": 1}), Node({"id": "2", "v": 2}), Node({"id": "3", "v": 3})]
        for node in nodes:
            col.append(node.getObject())
            
        # Act
        n = col.where({"v": { "le": 3 }})
        n = [v for v in n]
        
        # Assert
        self.assertIsInstance(n, collections.Iterable)
        self.assertEqual(len(n), 3)
        self.assertIn(nodes[0], n)
        self.assertIn(nodes[1], n)
        self.assertIn(nodes[2], n)

    def test_where_gt_pre_init(self):
        # Arrange
        rt = self.runtime()
        rt._unis.get.return_value = None
        col = UnisCollection.get_collection("", Node, rt) 
        col.createIndex("v")
        nodes = [Node({"id": "1", "v": 1}), Node({"id": "2", "v": 2}), Node({"id": "3", "v": 3})]
        for node in nodes:
            col.append(node.getObject())
        
        # Act
        n = col.where({"v": { "gt": 1 }})
        n = [v for v in n]
        
        # Assert
        self.assertIsInstance(n, collections.Iterable)
        self.assertEqual(len(n), 2)
        self.assertIn(nodes[2], n)
        self.assertIn(nodes[1], n)
        
    def test_where_ge_pre_init(self):
        # Arrange
        rt = self.runtime()
        rt._unis.get.return_value = None
        col = UnisCollection.get_collection("", Node, rt)
        col.createIndex("v")
        nodes = [Node({"id": "1", "v": 1}), Node({"id": "2", "v": 2}), Node({"id": "3", "v": 3})]
        for node in nodes:
            col.append(node.getObject())
        
        # Act
        n = list(col.where({"v": {"ge": 1 } }))
        
        # Assert
        self.assertIsInstance(n, collections.Iterable)
        self.assertEqual(len(n), 3)
        self.assertIn(nodes[0], n)
        self.assertIn(nodes[1], n)
        self.assertIn(nodes[2], n)

    def test_where_multi_pre_init(self):
        # Arrange
        rt = self.runtime()
        col = UnisCollection.get_collection("", Node, rt)
        nodes = [Node({"id": "1", "v": 1}), Node({"id": "2", "v": 2}), Node({"id": "3", "v": 3})]
        for node in nodes:
            col.append(node.getObject())
        
        # Act
        n = list(col.where({"id": "2", "v": { "lt": 3 }}))
        
        # Assert
        self.assertIsInstance(n, collections.Iterable)
        self.assertEqual(len(n), 1)
        self.assertIn(nodes[1], n)
        
    def test_where_func_pre_init(self):
        # Arrange
        rt = self.runtime()
        rt._unis.get.return_value = None
        col = UnisCollection.get_collection("", Node, rt)
        nodes = [Node({"id": "1", "v": 1}), Node({"id": "2", "v": 2}), Node({"id": "3", "v": 3})]
        for node in nodes:
            col.append(node.getObject())
        
        # Act
        n = col.where(lambda x: x.v == 2)
        n = [v for v in n]
        
        # Assert
        self.assertIsInstance(n, collections.Iterable)
        self.assertEqual(len(n), 1)
        self.assertIn(n[0], nodes)
        self.assertEqual(n[0].v, 2)
