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

import copy
import json
import unittest
import unittest.mock as mock
from unittest.mock import MagicMock, patch
import unis
import unis.runtime
import unis.runtime.oal

from unis.models import Node
from unis.models.settings import SCHEMAS
from unis.runtime.oal import ObjectLayer


class OALTest(unittest.TestCase):
    @patch.object(unis.runtime.oal.UnisClient, 'getResources', return_value = [{ "href": "#/nodes", "targetschema": { "items": { "href": SCHEMAS["Node"] } } }])
    @patch.object(unis.runtime.oal.UnisClient, 'get', return_value = { "id": "2", "ts": 1, "v": 2})
    @patch.object(unis.runtime.oal.UnisCollection, 'where', return_value = [Node({ "id": "1", "ts": 1, "v": 0})])
    @patch.object(unis.runtime.oal.UnisCollection, 'append')
    def test_find_rel(self, a_mock, wh_mock, g_mock, gr_mock):
        oal = ObjectLayer("http://localhost:8888")
        
        v = oal.find("#/nodes/1")
        
        self.assertIsInstance(v, Node)
        self.assertEqual(v.id, "1")
        gr_mock.assert_called_once_with()
        wh_mock.assert_called_once_with({"id": "1"})
        
    @patch.object(unis.runtime.oal.UnisClient, 'getResources', return_value = [{ "href": "#/nodes", "targetschema": { "items": { "href": SCHEMAS["Node"] } } }])
    @patch.object(unis.runtime.oal.UnisCollection, 'where', return_value = [Node({ "id": "1", "ts": 1, "v": 0 })])
    @patch.object(unis.runtime.oal.UnisClient, 'get', return_value = { "id": "1", "ts": 1, "v": 0})
    @patch.object(unis.runtime.oal.UnisCollection, 'append')
    def test_find_abs(self, a_mock, g_mock, wh_mock, gr_mock):
        oal = ObjectLayer("http://localhost:8888")
        
        v = oal.find("http://localhost:8888/nodes/1")
        
        self.assertIsInstance(v, Node)
        self.assertEqual(v.id, "1")
        gr_mock.assert_called_once_with()
        wh_mock.assert_called_once_with({"id": "1"})
            
    @patch.object(unis.runtime.oal.UnisClient, 'getResources', return_value = [{ "href": "#/nodes", "targetschema": { "items": { "href": SCHEMAS["Node"] } } }])
    @patch.object(unis.runtime.oal.UnisCollection, 'where', return_value = [])
    @patch.object(unis.runtime.oal.UnisClient, 'get', return_value = { "id": "1", "ts": 1, "v": 0})
    @patch.object(unis.runtime.oal.UnisCollection, 'append')
    def test_find_page_miss(self, a_mock, g_mock, wh_mock, gr_mock):
        oal = ObjectLayer("http://localhost:8888")
        
        v = oal.find("http://localhost:8888/nodes/1")
        
        self.assertIsInstance(v, Node)
        self.assertEqual(v.id, "1")
        gr_mock.assert_called_once_with()
        wh_mock.assert_called_once_with({"id": "1"})
        g_mock.assert_called_once_with("http://localhost:8888/nodes/1")


    @patch.object(unis.runtime.oal.UnisClient, 'getResources', return_value = [{ "href": "#/nodes", "targetschema": { "items": { "href": SCHEMAS["Node"] } } }])
    @patch.object(unis.runtime.oal.UnisCollection, 'where', return_value = [Node({ "id": "1", "ts": 1, "v": 0 })])
    @patch.object(unis.runtime.oal.UnisClient, 'get', return_value = { "id": "1", "ts": 1, "v": 0})
    @patch.object(unis.runtime.oal.UnisCollection, 'append')
    def test_find_bad_url(self, a_mock, g_mock, wh_mock, gr_mock):
        oal = ObjectLayer("http://localhost:8888")
        
        self.assertRaises(ValueError, oal.find, "bad_reference/nodes/1")
        
    @patch.object(unis.runtime.oal.UnisClient, 'getResources', return_value = [{ "href": "#/nodes", "targetschema": { "items": { "href": SCHEMAS["Node"] } } }])
    def test_find_bad_collection(self, gr_mock):
        oal = ObjectLayer("http://localhost:8888")
        
        self.assertRaises(ValueError, oal.find, "#/bad_col/1")
        
    @patch.object(unis.runtime.oal.UnisClient, 'getResources', return_value = [{ "href": "#/nodes", "targetschema": { "items": { "href": SCHEMAS["Node"] } } }])
    @patch.object(unis.runtime.oal.UnisClient, 'post')
    @patch.object(unis.runtime.oal.UnisCollection, 'updateIndex')
    def test_update_ref(self, ui_mock, p_mock, gr_mock):
        oal = ObjectLayer("http://localhost:8888")
        n = Node({"id": "1", "v": 1})
        n._collection = "nodes"
        
        oal.update(n)
        p_mock.assert_called_with("#/nodes", json.dumps(n.to_JSON()))

    
    @patch.object(unis.runtime.oal.UnisClient, 'getResources', return_value = [{ "href": "#/nodes", "targetschema": { "items": { "href": SCHEMAS["Node"] } } }])
    @patch.object(unis.runtime.oal.UnisCollection, 'append')
    def test_insert_dict(self, a_mock, gr_mock):
        oal = ObjectLayer("http://localhost:8888")
        n = {"id": "1", "v": 1, "$schema": SCHEMAS['Node'] }
        
        oal.insert(n)
        
        self.assertEqual(a_mock.call_count, 1)
        
    @patch.object(unis.runtime.oal.UnisClient, 'getResources', return_value = [{ "href": "#/nodes", "targetschema": { "items": { "href": SCHEMAS["Node"] } } }])
    @patch.object(unis.runtime.oal.UnisCollection, 'append')
    def test_insert_obj(self, a_mock, gr_mock):
        oal = ObjectLayer("http://localhost:8888")
        n = Node({"id": "1"})
        
        oal.insert(n)
        
        gr_mock.assert_called_once_with()
        a_mock.assert_called_once_with(n)
        
