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
from unis.services import RuntimeService
from unis.runtime.oal import ObjectLayer
from unis.runtime import Runtime

class _RuntimeSettings(object):
    def __init__(self):
        self.settings = { "inline": False, "defer_update": False, "auto_sync": True, "subscribe": False }

rts = _RuntimeSettings()

class _TestService(RuntimeService):
    pass

class UnisServiceTest(unittest.TestCase):
    @patch.object(unis.runtime.oal.UnisClient, 'getResources', return_value = [{ "href": "#/nodes", "targetschema": { "items": { "href": SCHEMAS["Node"] } } }])
    @patch.object(unis.runtime.oal.UnisClient, 'post', return_value = {"selfRef": "test", "id": "1", "v": 1})
    @patch.object(unis.runtime.oal.UnisCollection, 'updateIndex')    
    def test_create_service(self, ui_mock, p_mock, gr_mock):
        rt = Runtime()
        service = _TestService()
        rt.addService(service)
        
        self.assertTrue(service in rt.nodes._services)

    @patch.object(unis.runtime.oal.UnisClient, 'getResources', return_value = [{ "href": "#/nodes", "targetschema": { "items": { "href": SCHEMAS["Node"] } } },
                                                                               { "href": "#/links", "targetschema": { "items": { "href": SCHEMAS["Link"] } } }])
    @patch.object(unis.runtime.oal.UnisClient, 'post', return_value = {"selfRef": "test", "id": "1", "v": 1})
    @patch.object(unis.runtime.oal.UnisCollection, 'updateIndex')    
    def test_selective_service(self, ui_mock, p_mock, gr_mock):
        rt = Runtime()
        service = _TestService([Node])
        rt.addService(service)
        
        self.assertTrue(service in rt.nodes._services)
        self.assertFalse(service in rt.links._services)
        
    @patch.object(unis.runtime.oal.UnisClient, 'getResources', return_value = [{ "href": "#/nodes", "targetschema": { "items": { "href": SCHEMAS["Node"] } } }])
    @patch.object(unis.runtime.oal.UnisClient, 'post', return_value = {"selfRef": "test", "id": "1", "v": 1})
    @patch.object(unis.runtime.oal.UnisCollection, 'updateIndex')    
    def test_new_service_call(self, ui_mock, p_mock, gr_mock):
        rt = Runtime(defer_update=True)
        service = _TestService()
        rt.addService(service)
        n = Node({"selfRef": "test", "id": "1", "v": 1})
        n._collection = "nodes"
        
        
class OALTest(unittest.TestCase):
    
    @patch.object(unis.runtime.oal.UnisClient, 'getResources', return_value = [{ "href": "#/nodes", "targetschema": { "items": { "href": SCHEMAS["Node"] } } }])
    @patch.object(unis.runtime.oal.UnisClient, 'get', return_value = { "id": "1", "ts": 1, "v": 2})
    @patch.object(unis.runtime.oal.UnisCollection, 'hasValue', return_value = True)
    @patch.object(unis.runtime.oal.UnisCollection, 'where', return_value = iter([Node({ "id": "1", "ts": 1, "v": 0})]))
    @patch.object(unis.runtime.oal.UnisCollection, 'append')
    def test_find_rel(self, a_mock, wh_mock, h_mock, g_mock, gr_mock):
        
        oal = ObjectLayer("http://localhost:8888", rts)
        
        v = oal.find("#/nodes/1")
        
        self.assertIsInstance(v, Node)
        self.assertEqual(v.id, "1")
        gr_mock.assert_called_once_with()
        h_mock.assert_called_once_with("id", "1")
        wh_mock.assert_called_once_with({"id": "1"})

        
    @patch.object(unis.runtime.oal.UnisClient, 'getResources', return_value = [{ "href": "#/nodes", "targetschema": { "items": { "href": SCHEMAS["Node"] } } }])
    @patch.object(unis.runtime.oal.UnisCollection, 'where', return_value = iter([Node({ "id": "1", "ts": 1, "v": 0 })]))
    @patch.object(unis.runtime.oal.UnisCollection, 'hasValue', return_value = True)
    @patch.object(unis.runtime.oal.UnisClient, 'get', return_value = { "id": "1", "ts": 1, "v": 0})
    @patch.object(unis.runtime.oal.UnisCollection, 'append')
    def test_find_abs(self, a_mock, g_mock, h_mock, wh_mock, gr_mock):
        
        oal = ObjectLayer("http://localhost:8888", rts)
        
        v = oal.find("http://localhost:8888/nodes/1")
        
        self.assertIsInstance(v, Node)
        self.assertEqual(v.id, "1")
        gr_mock.assert_called_once_with()
        h_mock.assert_called_once_with("id", "1")
        wh_mock.assert_called_once_with({"id": "1"})
    
    @patch.object(unis.runtime.oal.UnisClient, 'getResources', return_value = [{ "href": "#/nodes", "targetschema": { "items": { "href": SCHEMAS["Node"] } } }])
    @patch.object(unis.runtime.oal.UnisCollection, 'where', return_value = iter([]))
    @patch.object(unis.runtime.oal.UnisCollection, 'hasValue', return_value = False)
    @patch.object(unis.runtime.oal.UnisClient, 'get', return_value = { "$schema": SCHEMAS["Node"], "id": "1", "ts": 1, "v": 0})
    @patch.object(unis.runtime.oal.UnisCollection, 'append')
    def test_find_miss(self, a_mock, g_mock, h_mock, wh_mock, gr_mock):
        oal = ObjectLayer("http://localhost:8888", rts)
        
        v = oal.find("http://localhost:8888/nodes/1")
        
        self.assertIsInstance(v, Node)
        self.assertEqual(v.id, "1")
        gr_mock.assert_called_once_with()
        h_mock.assert_called_once_with("id", "1")
        g_mock.assert_called_once_with("http://localhost:8888/nodes/1")


    @patch.object(unis.runtime.oal.UnisClient, 'getResources', return_value = [{ "href": "#/nodes", "targetschema": { "items": { "href": SCHEMAS["Node"] } } }])
    @patch.object(unis.runtime.oal.UnisCollection, 'where', return_value = iter([Node({ "id": "1", "ts": 1, "v": 0 })]))
    @patch.object(unis.runtime.oal.UnisClient, 'get', return_value = { "id": "1", "ts": 1, "v": 0})
    @patch.object(unis.runtime.oal.UnisCollection, 'append')
    def test_find_bad_url(self, a_mock, g_mock, wh_mock, gr_mock):
        oal = ObjectLayer("http://localhost:8888", rts)
        
        self.assertRaises(ValueError, oal.find, "bad_reference/nodes/1")
        
    @patch.object(unis.runtime.oal.UnisClient, 'getResources', return_value = [{ "href": "#/nodes", "targetschema": { "items": { "href": SCHEMAS["Node"] } } }])
    def test_find_bad_collection(self, gr_mock):
        oal = ObjectLayer("http://localhost:8888", rts)
        
        self.assertRaises(ValueError, oal.find, "#/bad_col/1")
        
    @patch.object(unis.runtime.oal.UnisClient, 'getResources', return_value = [{ "href": "#/nodes", "targetschema": { "items": { "href": SCHEMAS["Node"] } } }])
    @patch.object(unis.runtime.oal.UnisClient, 'post', return_value = {"selfRef": "test", "id": "1", "v": 1})
    @patch.object(unis.runtime.oal.UnisCollection, 'updateIndex')
    def test_update_ref(self, ui_mock, p_mock, gr_mock):
        oal = ObjectLayer("http://localhost:8888", rts)
        n = Node({"selfRef": "test", "id": "1", "v": 1})
        n._collection = "nodes"
        
        oal.update(n)
        p_mock.assert_called_with("#/nodes", json.dumps([n.to_JSON()]))

    
    @patch.object(unis.runtime.oal.UnisClient, 'getResources', return_value = [{ "href": "#/nodes", "targetschema": { "items": { "href": SCHEMAS["Node"] } } }])
    @patch.object(unis.runtime.oal.UnisCollection, 'append')
    def test_insert_dict(self, a_mock, gr_mock):
        oal = ObjectLayer("http://localhost:8888", rts)
        n = {"id": "1", "v": 1, "$schema": SCHEMAS['Node'] }
        
        oal.insert(n)
        
        self.assertEqual(a_mock.call_count, 1)
        
    @patch.object(unis.runtime.oal.UnisClient, 'getResources', return_value = [{ "href": "#/nodes", "targetschema": { "items": { "href": SCHEMAS["Node"] } } }]) 
    @patch.object(unis.runtime.oal.UnisCollection, 'append')
    def test_insert_obj(self, a_mock, gr_mock):
        oal = ObjectLayer("http://localhost:8888", rts)
        n = Node({"id": "1"})
        
        oal.insert(n)
        
        gr_mock.assert_called_once_with()
        a_mock.assert_called_once_with(n)
                
    @patch.object(unis.runtime.oal.UnisClient, 'getResources', return_value = [{ "href": "#/nodes", "targetschema": { "items": { "href": SCHEMAS["Node"] } } }])
    @patch.object(unis.runtime.oal.UnisClient, 'post', return_value = { "selfRef": "test", "id": "1" })
    @patch.object(unis.runtime.oal.UnisCollection, 'updateIndex')
    @patch.object(unis.runtime.oal.UnisCollection, 'append')
    def test_modify_object(self, a_mock, ui_mock, p_mock, gr_mock):
        oal = ObjectLayer("http://localhost:8888", rts)
        n = Node({"selfRef": "test", "id": "1"})
        
        oal.insert(n)
        n._runtime = oal
        n._collection = "nodes"
        n.commit()
        n.name = "blah"
        
        self.assertEqual(n.name, "blah")
        a_mock.called_once_with(n)
        p_mock.assert_called_with("#/nodes", json.dumps([n.to_JSON()]))
        ui_mock.assert_called_with(n)