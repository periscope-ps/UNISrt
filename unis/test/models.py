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

from unis.models.settings import SCHEMAS
from unis.models import Node, Exnode, Extent
from unis.models.models import CACHE, UnisObject, UnisList, schemaLoader
from unis.models.lists import UnisCollection

from unis.utils.pubsub import Events

class UnisObjectTest(unittest.TestCase):
    def test_init(self):
        # Arrange
        test_data = {"key1": "value1", "key2": "value2", "$key3": "value3"}
        # Act
        obj1 = UnisObject()
        obj2 = UnisObject(src=test_data)
        # Assert
        self.assertIsInstance(obj1, UnisObject)
        self.assertIsInstance(obj2, UnisObject)
        self.assertEqual(obj1.to_JSON(), {})
        self.assertEqual(obj2.to_JSON(), test_data)
        for key, value in test_data.items():
            self.assertTrue(hasattr(obj2, key))
            self.assertEqual(getattr(obj2, key), value)
    
    def test_virtual(self):
        # Arrange
        obj1 = UnisObject()
        
        # Act
        obj1.v = 10
        
        # Assert
        self.assertTrue(hasattr(obj1, "v"))
        self.assertEqual(getattr(obj1, "v"), 10)
        self.assertTrue("v" not in obj1.to_JSON())
        
    def test_commit(self):
        # Arrange
        obj1 = UnisObject()
        obj1.v = 10
        
        # Act
        obj1.commit("v")
        
        # Assert
        self.assertTrue(hasattr(obj1, "v"))
        self.assertEqual(getattr(obj1, "v"), 10)
        self.assertEqual(obj1.to_JSON(), {"v": 10})
        
    def test_remote(self):
        # Arrange
        obj1 = UnisObject()
        obj2 = UnisObject(local_only=False)
        
        # Assert
        self.assertFalse(obj1.remoteObject())
        self.assertTrue(obj2.remoteObject())
        
    def test_inner_list(self):
        # Arrange
        obj1 = UnisObject({"v": ["1", "2", "3"]})
        
        # Act
        vs = obj1.v
        
        # Assert
        self.assertIsInstance(vs, UnisList)
        self.assertEqual(vs.to_JSON(), ["1", "2", "3"])
        self.assertEqual(vs._parent, obj1)
    
    def test_inner_dict(self):
        # Arrange
        obj1 = UnisObject({"v": { "a": "1" } })
        
        # Act
        v = obj1.v
        
        # Assert
        self.assertIsInstance(v, UnisObject)
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
        node2 = Node(src=NetworkResourceTest.VALID_NODE)
        self.assertIsInstance(node1, Node)
        self.assertIsInstance(node2, Node)
        self.assertEqual(getattr(node1, '$schema'), SCHEMAS['Node'])
        self.assertEqual(getattr(node2, '$schema'), SCHEMAS['Node'])
        self.assertEqual(node1._schema, CACHE[SCHEMAS['Node']])
        
    def test_validate(self):
        from jsonschema.exceptions import ValidationError
        
        good = Node(src=NetworkResourceTest.VALID_NODE)
        bad  = Node(src=NetworkResourceTest.INVALID_NODE)
        
        self.assertEquals(good.validate(validate_id=True), None)
        self.assertRaises(ValidationError, bad.validate, validate_id=True)

    def test_validate_on_change(self):
        from jsonschema.exceptions import ValidationError
        
        good = Node(NetworkResourceTest.VALID_NODE)
        bad = Node(NetworkResourceTest.VALID_NODE)
        
        good.name = "modified"
        self.assertEqual(good.name, "modified")
        self.assertRaises(ValidationError, lambda: setattr(bad, "name", 10))
        
    def test_modify_runtime(self):
        # Arrange
        runtime = MagicMock()
        good = Node({"id": "1", "ts": 1, "v": {} }, runtime=runtime, local_only=False)
        inner = Node(runtime=runtime, local_only=False)
        
        # Act
        good.v = inner
        
        # Assert
        runtime.insert.assert_called_once_with(inner)
        runtime.update.assert_called_once_with(good)
        
    def test_dict_resolve_insert_runtime(self):
        # Arrange
        runtime = MagicMock()
        inner = { "$schema": "test", "val": 5, "id": "2" }
        good = Node({"id": "1", "v": inner}, runtime=runtime, local_only=False)
        
        # Act
        v = good.v
        
        # Assert
        runtime.insert.assert_called_once_with(inner)
        
    def test_dict_resolve_find_runtime(self):
        # Arrange
        runtime = MagicMock()
        inner = { "href": "test" }
        good = Node({"id": "1", "v": inner}, runtime=runtime, local_only=False)
        
        # Act
        v = good.v
        
        # Assert
        runtime.find.assert_called_once_with("test")
        
    def test_flush(self):
        # Arrange
        r1 = MagicMock()
        r2 = MagicMock()
        r3 = MagicMock()
        n1 = Node({"id": "1"}, runtime=r1, local_only=False)
        n2 = Node({"id": "2"}, runtime=r2, local_only=False)
        n3 = Node({"id": "3"}, runtime=r3, local_only=False)
        n1._dirty = True
        n2._pending = True
        
        # Act
        n1.flush()
        n2.flush()
        n3.flush()
        
        # Assert
        r1.update.assert_called_once_with(n1)
        self.assertEqual(0, r2.update.call_count)
        self.assertEqual(0, r3.update.call_count)
       
class CollectionTest(unittest.TestCase):
    def test_init(self):
        # Act
        col = UnisCollection("", "", Node, MagicMock())
        
        # Assert
        self.assertEqual(len(col), 0)
        self.assertEqual(col._cache, [])
        

    def test_append(self):
        # Arrange
        rt = MagicMock()
        col = UnisCollection("", "", Node, rt)
        n1 = Node({"id": "1"})
        n2 = Node({"id": "2"})
        
        # Act
        col.append(n1)
        col.append(n2)
        
        # Assert
        self.assertEqual(len(col), 2)
        self.assertEqual(n1._runtime, rt)
        self.assertEqual(n2._runtime, rt)
        self.assertIn("id", col._indices)
        self.assertEqual(col._indices["id"], [("1", 0), ("2", 1)])
        self.assertEqual(col._rangeset, set([0, 1]))
        rt._publish.assert_any_call(Events.new, n1)
        rt._publish.assert_any_call(Events.new, n2)
        self.assertEqual(rt._publish.call_count, 2)
        
    def test_append_duplicate(self):
        # Arrange
        rt = MagicMock()
        col = UnisCollection("", "", Node, rt)
        n1 = Node({"id": "1", "v": 1})
        n2 = Node({"id": "1", "v": 2})
        
        # Act
        col.append(n1)
        col.append(n2)
        
        # Assert
        self.assertEqual(len(col), 1)
        self.assertEqual(col._indices["id"], [("1", 0)])
        self.assertEqual(col._rangeset, set([0]))
        self.assertEqual(col[0].v, 2)
        rt._publish.assert_any_call(Events.new, n1)
        rt._publish.assert_any_call(Events.update, n1)
        self.assertEqual(rt._publish.call_count, 2)
        
    def test_append_bad(self):
        # Arrange
        rt = MagicMock()
        col = UnisCollection("", "", Node, rt)
        e1 = Exnode()
        
        # Assert
        self.assertRaises(TypeError, col.append(e1))
        
    def test_setitem(self):
        # Arrange
        rt = MagicMock()
        col = UnisCollection("", "", Node, rt)
        n1 = Node({"id": "1", "v": 1})
        n2 = Node({"id": "1", "v": 2})
        
        # Act
        col.append(n1)
        col[0] = n2
        
        # Assert
        self.assertEqual(len(col), 1)
        self.assertEqual(col._indices["id"], [("1", 0)])
        self.assertEqual(col._rangeset, set([0]))
        self.assertEqual(col[0].v, 2)
        rt._publish.assert_any_call(Events.new, n1)
        rt._publish.assert_any_call(Events.update, n1)
        self.assertEqual(rt._publish.call_count, 2)
        
    def test_setitem_bad_index(self):
        def set_col():
            col[0] = n2
        
        # Arrange
        rt = MagicMock()
        col = UnisCollection("", "", Node, rt)
        n1 = Node({"id": "1", "v": 1})
        n2 = Node({"id": "2", "v": 2})
        
        # Act
        col.append(n1)
        
        # Assert
        self.assertRaises(AttributeError, set_col)
        self.assertEqual(col._indices["id"], [("1", 0)])
        self.assertEqual(col._rangeset, set([0]))
        self.assertEqual(col[0].v, 1)
        rt._publish.assert_called_with(Events.new, n1)
        
    def test_iter(self):
        # Arrange
        rt = MagicMock()
        rt._unis.get.return_value = []
        col = UnisCollection("", "", Node, rt)
        
        # Act
        for i in range(100):
            col.append(Node({"id": str(i)}))
        
        # Assert
        i = 0
        for n in col:
            self.assertEqual(int(n.id), i)
            i += 1
        self.assertEqual(i, 100)

        self.assertEqual(rt._unis.subscribe.call_count, 1)
        self.assertTrue(col._subscribed)
    
    def tes_iter_remote(self):
        # Arrange
        rt = MagicMock()
        rt._unis.get.return_value = [{"id": str(i)} for i in range(100)]
        col = UnisCollection("", "", Node, rt)
        
        # Assert
        i = 0
        for n in col:
            self.assertEqual(int(n.id), i)
            i += 1
        self.assertEqual(i, 100)
        for n in col:
            self.assertEqual((n.id, int(n.id)), self._indices["id"])
    
    def test_create_index_before_insert(self):
        # Arrange
        rt = MagicMock()
        col = UnisCollection("", "", Node, rt)
        
        # Arrange
        col.createIndex("v")
        col.append(Node({"id": "1", "v": 1}))
        col.append(Node({"id": "2", "v": 5}))
        col.append(Node({"id": "3", "v": 2}))
        
        # Assert
        self.assertIn("id", col._indices)
        self.assertIn("v", col._indices)
        self.assertEqual(col._indices["id"], [("1", 0), ("2", 1), ("3", 2)])
        self.assertEqual(col._indices["v"], [(1, 0), (2, 2), (5, 1)])
        
    
    def test_create_index_after_insert(self):
        # Arrange
        rt = MagicMock()
        col = UnisCollection("", "", Node, rt)
        
        # Arrange
        col.append(Node({"id": "1", "v": 1}))
        col.append(Node({"id": "2", "v": 5}))
        col.append(Node({"id": "3", "v": 2}))
        col.createIndex("v")
        
        # Assert
        self.assertIn("id", col._indices)
        self.assertIn("v", col._indices)
        self.assertEqual(col._indices["id"], [("1", 0), ("2", 1), ("3", 2)])
        self.assertEqual(col._indices["v"], [(1, 0), (2, 2), (5, 1)])
        
    def test_update_index(self):
        # Arrange
        rt = MagicMock()
        col = UnisCollection("", "", Node, rt)
        col.createIndex("v")
        col.append(Node({"id": "1", "v": 1}))
        col.append(Node({"id": "2", "v": 5}))
        col.append(Node({"id": "3", "v": 2}))
        
        # Act
        n = col[0]
        n.v = 10
        col.updateIndex(n)
        
        # Assert
        self.assertIn("id", col._indices)
        self.assertIn("v", col._indices)
        self.assertEqual(col._indices["id"], [("1", 0), ("2", 1), ("3", 2)])
        self.assertEqual(col._indices["v"], [(2, 2), (5, 1), (10, 0)])

    def test_where_single_pre_init(self):
        # Arrange
        rt = Mock()
        rt._unis.get.return_value = None
        col = UnisCollection("", "", Node, rt)
        col.append(Node({"id": "1", "v": 1}))
        col.append(Node({"id": "2", "v": 2}))
        col.append(Node({"id": "3", "v": 3}))
        
        # Act
        n = col.where({"id": "3"})
        n = list(n)
        
        # Assert
        self.assertIsInstance(n, collections.Iterable)
        self.assertEqual(n[0].id, "3")
        self.assertEqual(n[0].v, 3)
        
    def test_where_lt_pre_init(self):
        # Arrange
        rt = MagicMock()
        rt._unis.get.return_value = None
        col = UnisCollection("", "", Node, rt)
        nodes = [Node({"id": "1", "v": 1}), Node({"id": "2", "v": 2}), Node({"id": "3", "v": 3})]
        for node in nodes:
            col.append(node)
        
        # Act
        n = col.where({"id": { "lt": "3" }})
        n = [v for v in n]
        
        # Assert
        self.assertIsInstance(n, collections.Iterable)
        self.assertEqual(len(n), 2)
        self.assertIn(nodes[0], n)
        self.assertIn(nodes[1], n)
        
    def test_where_le_pre_init(self):
        # Arrange
        rt = MagicMock()
        rt._unis.get.return_value = None
        col = UnisCollection("", "", Node, rt)
        nodes = [Node({"id": "1", "v": 1}), Node({"id": "2", "v": 2}), Node({"id": "3", "v": 3})]
        for node in nodes:
            col.append(node)
            
        # Act
        n = col.where({"id": { "le": "3" }})
        n = [v for v in n]
        
        # Assert
        self.assertIsInstance(n, collections.Iterable)
        self.assertEqual(len(n), 3)
        self.assertIn(nodes[0], n)
        self.assertIn(nodes[1], n)
        self.assertIn(nodes[2], n)

    def test_where_gt_pre_init(self):
        # Arrange
        rt = MagicMock()
        rt._unis.get.return_value = None
        col = UnisCollection("", "", Node, rt)
        nodes = [Node({"id": "1", "v": 1}), Node({"id": "2", "v": 2}), Node({"id": "3", "v": 3})]
        for node in nodes:
            col.append(node)
        
        # Act
        n = col.where({"id": { "gt": "1" }})
        n = [v for v in n]
        
        # Assert
        self.assertIsInstance(n, collections.Iterable)
        self.assertEqual(len(n), 2)
        self.assertIn(nodes[2], n)
        self.assertIn(nodes[1], n)
        
    def test_where_ge_pre_init(self):
        # Arrange
        rt = MagicMock()
        rt._unis.get.return_value = None
        col = UnisCollection("", "", Node, rt)
        nodes = [Node({"id": "1", "v": 1}), Node({"id": "2", "v": 2}), Node({"id": "3", "v": 3})]
        for node in nodes:
            col.append(node)
        
        # Act
        n = col.where({"id": { "ge": "1" }})
        n = [v for v in n]
        
        # Assert
        self.assertIsInstance(n, collections.Iterable)
        self.assertEqual(len(n), 3)
        self.assertIn(nodes[0], n)
        self.assertIn(nodes[1], n)
        self.assertIn(nodes[2], n)

    def test_where_multi_pre_init(self):
        # Arrange
        rt = MagicMock()
        rt._unis.get.return_value = None
        col = UnisCollection("", "", Node, rt)
        nodes = [Node({"id": "1", "v": 1}), Node({"id": "2", "v": 2}), Node({"id": "3", "v": 3})]
        for node in nodes:
            col.append(node)
        
        # Act
        n = col.where({"id": { "gt": "1" }, "v": { "lt": 3 }})
        n = [v for v in n]
        
        # Assert
        self.assertIsInstance(n, collections.Iterable)
        self.assertEqual(len(n), 1)
        self.assertIn(nodes[1], n)
        
    def test_where_eq_index_post_init(self):
        rt = MagicMock()
        rt._unis.get.return_value = [{"id": "1", "v": 1}, {"id": "2", "v": 2}, {"id": "3", "v": 3}]
        col = UnisCollection("", "", Node, rt)
        [n for n in col]
        
        # Act
        n = col.where({"id": "1"})
        n = [v for v in n]
        
        # Assert
        self.assertIsInstance(n, collections.Iterable)
        self.assertEqual(len(n), 1)
        self.assertEqual(n[0].v, 1)
        
    def test_where_eq_no_index_post_init(self):
        rt = MagicMock()
        rt._unis.get.return_value = [{"id": "1", "v": 1}, {"id": "2", "v": 2}, {"id": "3", "v": 3}]
        col = UnisCollection("", "", Node, rt)
        [n for n in col]
        
        # Act
        n = col.where({"v": 1})
        n = [v for v in n]
        
        # Assert
        self.assertIsInstance(n, collections.Iterable)
        self.assertEqual(len(n), 1)
        self.assertEqual(n[0].v, 1)
        
    def test_where_lt_index_post_init(self):
        rt = MagicMock()
        rt._unis.get.return_value = [{"id": "1", "v": 1}, {"id": "2", "v": 2}, {"id": "3", "v": 3}]
        col = UnisCollection("", "", Node, rt)
        [n for n in col]
        
        # Act
        n = col.where({"id": {"lt": "3"}})
        n = [v for v in n]
        
        # Assert
        self.assertIsInstance(n, collections.Iterable)
        self.assertEqual(len(n), 2)
        self.assertIn(n[0].v, [1, 2])
        self.assertIn(n[1].v, [1, 2])
        self.assertFalse(n[1].v == n[0].v)

    def test_where_le_index_post_init(self):
        rt = MagicMock()
        rt._unis.get.return_value = [{"id": "1", "v": 1}, {"id": "2", "v": 2}, {"id": "3", "v": 3}]
        col = UnisCollection("", "", Node, rt)
        [n for n in col]
        
        # Act
        n = col.where({"id": {"le": "3"}})
        n = [v for v in n]
        
        # Assert
        self.assertIsInstance(n, collections.Iterable)
        self.assertEqual(len(n), 3)
        self.assertIn(n[0].v, [1, 2, 3])
        self.assertIn(n[1].v, [1, 2, 3])
        self.assertIn(n[2].v, [1, 2, 3])
        self.assertTrue(n[1] != n[0] and n[1] != n[2] and n[0] != n[2])

    def test_where_gt_index_post_init(self):
        rt = MagicMock()
        rt._unis.get.return_value = [{"id": "1", "v": 1}, {"id": "2", "v": 2}, {"id": "3", "v": 3}]
        col = UnisCollection("", "", Node, rt)
        [n for n in col]
        
        # Act
        n = col.where({"id": {"gt": "1"}})
        n = [v for v in n]
        
        # Assert
        self.assertIsInstance(n, collections.Iterable)
        self.assertEqual(len(n), 2)
        self.assertIn(n[0].v, [1, 2, 3])
        self.assertIn(n[1].v, [1, 2, 3])
        self.assertTrue(n[1] != n[0])
        
    def test_where_ge_index_post_init(self):
        rt = MagicMock()
        rt._unis.get.return_value = [{"id": "1", "v": 1}, {"id": "2", "v": 2}, {"id": "3", "v": 3}]
        col = UnisCollection("", "", Node, rt)
        [n for n in col]
        
        # Act
        n = col.where({"id": {"ge": "2"}})
        n = [v for v in n]
        
        # Assert
        self.assertIsInstance(n, collections.Iterable)
        self.assertEqual(len(n), 2)
        self.assertIn(n[0].v, [1, 2, 3])
        self.assertIn(n[1].v, [1, 2, 3])
        self.assertTrue(n[1] != n[0])

    def test_where_multi_index_post_init(self):
        rt = MagicMock()
        rt._unis.get.return_value = [{"id": "1", "v": 1}, {"id": "2", "v": 2}, {"id": "3", "v": 3}]
        col = UnisCollection("", "", Node, rt)
        col.createIndex("v")
        [n for n in col]
        
        # Act
        n = col.where({"id": {"lt": "3"}, "v": {"gt": 1}})
        n = [v for v in n]
        
        # Assert
        self.assertIsInstance(n, collections.Iterable)
        self.assertEqual(len(n), 1)
        self.assertEqual(n[0].v, 2)
        
    def test_where_mixed_multi_index_post_init(self):
        rt = MagicMock()
        rt._unis.get.return_value = [{"id": "1", "v": 1}, {"id": "2", "v": 2}, {"id": "3", "v": 3}]
        col = UnisCollection("", "", Node, rt)
        [n for n in col]
        
        # Act
        n = col.where({"id": {"lt": "3"}, "v": {"gt": 1}})
        n = [v for v in n]
        
        # Assert
        self.assertIsInstance(n, collections.Iterable)
        self.assertEqual(len(n), 1)
        self.assertEqual(n[0].v, 2)
    
    def test_where_func_pre_init(self):
        # Arrange
        rt = MagicMock()
        rt._unis.get.return_value = None
        col = UnisCollection("", "", Node, rt)
        nodes = [Node({"id": "1", "v": 1}), Node({"id": "2", "v": 2}), Node({"id": "3", "v": 3})]
        for node in nodes:
            col.append(node)
        
        # Act
        n = col.where(lambda x: x.v == 2)
        n = [v for v in n]
        
        # Assert
        self.assertIsInstance(n, collections.Iterable)
        self.assertEqual(len(n), 1)
        self.assertIn(n[0], nodes)
        self.assertEqual(n[0].v, 2)
        
    def tests_where_func_post_init(self):
        rt = MagicMock()
        rt._unis.get.return_value = [{"id": "1", "v": 1}, {"id": "2", "v": 2}, {"id": "3", "v": 3}]
        col = UnisCollection("", "", Node, rt)
        [n for n in col]
        
        # Act
        n = col.where(lambda x: x.v == 2)
        n = [v for v in n]
        
        # Assert
        self.assertIsInstance(n, collections.Iterable)
        self.assertEqual(len(n), 1)
        self.assertEqual(n[0].v, 2)
        
