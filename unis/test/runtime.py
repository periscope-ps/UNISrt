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
from unis.settings import SCHEMAS
from unis.services import RuntimeService
from unis.runtime.oal import ObjectLayer
from unis.runtime import Runtime

class _RuntimeSettings(object):
    def __init__(self):
        self.settings = { "inline": False, "defer_update": False, "auto_sync": True, "subscribe": False, "unis": [ { "url": "http://localhost:8888", "ssl": False, "verify": None } ] }

rts = _RuntimeSettings()

class _TestService(RuntimeService):
    targets = [ Node ]

class UnisServiceTest(unittest.TestCase):
    @patch.object(unis.runtime.oal.UnisProxy, 'getResources', return_value = [{ "href": "#/nodes", "targetschema": { "items": { "href": SCHEMAS["Node"] } } },
                                                                               { "href": "#/links", "targetschema": { "items": { "href": SCHEMAS["Link"] } } }])
    @patch.object(unis.runtime.oal.UnisProxy, 'post', return_value = {"selfRef": "http://localhost:8888/nodes/test", "id": "1", "v": 1})
    @patch.object(unis.runtime.oal.UnisCollection, 'updateIndex')
    def test_attach_service(self, ui_mock, p_mock, gr_mock):
        rt = Runtime()
        service = _TestService([Node])
        rt.addService(service)
        
        self.assertTrue(service in rt.nodes._services)
        self.assertFalse(service in rt.links._services)
        
    @patch.object(unis.runtime.oal.UnisProxy, 'getResources', return_value = [{ "href": "#/nodes", "targetschema": { "items": { "href": SCHEMAS["Node"] } } }])
    @patch.object(unis.runtime.oal.UnisProxy, 'post', return_value = {"selfRef": "http://localhost:8888/nodes/test", "id": "1", "v": 1})
    @patch.object(unis.runtime.oal.UnisCollection, 'updateIndex')    
    def test_new_service_call(self, ui_mock, p_mock, gr_mock):
        rt = Runtime(defer_update=True)
        service = _TestService()
        rt.addService(service)
        n = Node({"selfRef": "http://localhost:8888/nodes/test", "id": "1", "v": 1})
        n.setCollection("nodes")
        
class RuntimeTest(unittest.TestCase):
    @patch.object(unis.runtime.oal.UnisProxy, 'getResources', return_value = [{ "href": "#/nodes", "targetschema": { "items": { "href": SCHEMAS["Node"] } } }])
    @patch.object(unis.runtime.oal.UnisProxy, 'post', return_value = { "selfRef": "http://localhost:8888/nodes/test", "id": "1" })
    @patch.object(unis.runtime.oal.UnisCollection, 'updateIndex')
    @patch.object(unis.runtime.oal.UnisCollection, 'append')
    def test_insert_publish(self, a_mock, ui_mock, p_mock, gr_mock):
        oal = Runtime(rts)
        n = Node({"selfRef": "http://localhost:8888/nodes/test", "id": "1"})
        
        oal.insert(n, publish_to="http://localhost:8888")
        n.name = "blah"
        
        self.assertEqual(n.name, "blah")
        a_mock.called_once_with(n)
        p_mock.assert_called_with(n)
        ui_mock.assert_called_with(n)

        
class OALTest(unittest.TestCase):
    @patch.object(unis.runtime.oal.UnisProxy, 'getResources', return_value = [{ "href": "#/nodes", "targetschema": { "items": { "href": SCHEMAS["Node"] } } }])
    @patch.object(unis.runtime.oal.UnisProxy, 'get', return_value = { "id": "1", "ts": 1, "v": 2})
    @patch.object(unis.runtime.oal.UnisCollection, 'hasValue', return_value = True)
    @patch.object(unis.runtime.oal.UnisCollection, 'where', return_value = iter([Node({ "id": "1", "ts": 1, "v": 0})]))
    @patch.object(unis.runtime.oal.UnisCollection, 'append')
    def test_find_rel(self, a_mock, wh_mock, h_mock, g_mock, gr_mock):
        
        oal = ObjectLayer(rts)
        
        v = oal.find("#/nodes/1")
        
        self.assertIsInstance(v, Node)
        self.assertEqual(v.id, "1")
        gr_mock.assert_called_once_with()
        h_mock.assert_called_once_with("id", "1")
        wh_mock.assert_called_once_with({"id": "1"})

        
    @patch.object(unis.runtime.oal.UnisProxy, 'getResources', return_value = [{ "href": "#/nodes", "targetschema": { "items": { "href": SCHEMAS["Node"] } } }])
    @patch.object(unis.runtime.oal.UnisCollection, 'where', return_value = iter([Node({ "id": "1", "ts": 1, "v": 0 })]))
    @patch.object(unis.runtime.oal.UnisCollection, 'hasValue', return_value = True)
    @patch.object(unis.runtime.oal.UnisProxy, 'get', return_value = { "id": "1", "ts": 1, "v": 0})
    @patch.object(unis.runtime.oal.UnisCollection, 'append')
    def test_find_abs(self, a_mock, g_mock, h_mock, wh_mock, gr_mock):
        oal = ObjectLayer(rts)
        
        v = oal.find("http://localhost:8888/nodes/1")
        
        self.assertIsInstance(v, Node)
        self.assertEqual(v.id, "1")
        gr_mock.assert_called_once_with()
        h_mock.assert_called_once_with("id", "1")
        wh_mock.assert_called_once_with({"id": "1"})
    
    @patch.object(unis.runtime.oal.UnisProxy, 'getResources', return_value = [{ "href": "#/nodes", "targetschema": { "items": { "href": SCHEMAS["Node"] } } }])
    @patch.object(unis.runtime.oal.UnisCollection, 'where', return_value = iter([]))
    @patch.object(unis.runtime.oal.UnisCollection, 'hasValue', return_value = False)
    @patch.object(unis.runtime.oal.UnisProxy, 'get', return_value = [{ "$schema": SCHEMAS["Node"], "id": "1", "ts": 1, "v": 0}])
    @patch.object(unis.runtime.oal.UnisCollection, 'append', side_effect=lambda x: x)
    def test_find_miss(self, a_mock, g_mock, h_mock, wh_mock, gr_mock):
        oal = ObjectLayer(rts)
        
        v = oal.find("http://localhost:8888/nodes/1")
        
        self.assertIsInstance(v, Node)
        self.assertEqual(v.id, "1")
        gr_mock.assert_called_once_with()
        h_mock.assert_called_once_with("id", "1")
        g_mock.assert_called_once_with("http://localhost:8888/nodes/1")


    @patch.object(unis.runtime.oal.UnisProxy, 'getResources', return_value = [{ "href": "#/nodes", "targetschema": { "items": { "href": SCHEMAS["Node"] } } }])
    @patch.object(unis.runtime.oal.UnisCollection, 'where', return_value = iter([Node({ "id": "1", "ts": 1, "v": 0 })]))
    @patch.object(unis.runtime.oal.UnisProxy, 'get', return_value = { "id": "1", "ts": 1, "v": 0})
    @patch.object(unis.runtime.oal.UnisCollection, 'append')
    def test_find_bad_url(self, a_mock, g_mock, wh_mock, gr_mock):
        oal = ObjectLayer(rts)
        
        self.assertRaises(ValueError, oal.find, "bad_reference/nodes/1")
        
    @patch.object(unis.runtime.oal.UnisProxy, 'getResources', return_value = [{ "href": "#/nodes", "targetschema": { "items": { "href": SCHEMAS["Node"] } } }])
    def test_find_bad_collection(self, gr_mock):
        oal = ObjectLayer(rts)
        
        self.assertRaises(ValueError, oal.find, "#/bad_col/1")
        
    @patch.object(unis.runtime.oal.UnisProxy, 'getResources', return_value = [{ "href": "#/nodes", "targetschema": { "items": { "href": SCHEMAS["Node"] } } }])
    @patch.object(unis.runtime.oal.UnisProxy, 'post', return_value = {"selfRef": "http://localhost:8888/nodes/test", "id": "1", "v": 1})
    @patch.object(unis.runtime.oal.UnisCollection, 'updateIndex')
    def test_update_ref(self, ui_mock, p_mock, gr_mock):
        oal = ObjectLayer(rts)
        n = Node({"selfRef": "http://localhost:8888/nodes/test", "id": "1", "v": 1})
        n.setCollection("nodes")
        
        oal.update(n)
        p_mock.assert_called_with([n])

    
    @patch.object(unis.runtime.oal.UnisProxy, 'getResources', return_value = [{ "href": "#/nodes", "targetschema": { "items": { "href": SCHEMAS["Node"] } } }])
    @patch.object(unis.runtime.oal.UnisCollection, 'append')
    def test_insert_dict(self, a_mock, gr_mock):
        oal = ObjectLayer(rts)
        n = {"id": "1", "v": 1, "$schema": SCHEMAS['Node'] }
        
        oal.insert(n)
        
        self.assertEqual(a_mock.call_count, 1)
        
    @patch.object(unis.runtime.oal.UnisProxy, 'getResources', return_value = [{ "href": "#/nodes", "targetschema": { "items": { "href": SCHEMAS["Node"] } } }]) 
    @patch.object(unis.runtime.oal.UnisCollection, 'append')
    def test_insert_obj(self, a_mock, gr_mock):
        oal = ObjectLayer(rts)
        n = Node({"id": "1"})
        
        oal.insert(n)
        
        gr_mock.assert_called_once_with()
        a_mock.assert_called_once_with(n)
        
    @patch.object(unis.runtime.oal.UnisProxy, 'getResources', return_value = [{ "href": "#/nodes", "targetschema": { "items": { "href": SCHEMAS["Node"] } } }])
    @patch.object(unis.runtime.oal.UnisProxy, 'post', return_value = { "selfRef": "http://localhost:8888/nodes/test", "id": "1" })
    @patch.object(unis.runtime.oal.UnisCollection, 'updateIndex')
    @patch.object(unis.runtime.oal.UnisCollection, 'append')
    def test_modify_object(self, a_mock, ui_mock, p_mock, gr_mock):
        oal = ObjectLayer(rts)
        n = Node({"selfRef": "http://localhost:8888/test", "id": "1"})
        
        oal.insert(n)
        n._runtime = oal
        n.setCollection("nodes")
        n.commit("http://localhost:8888")
        n.name = "blah"
        
        self.assertEqual(n.name, "blah")
        a_mock.called_once_with(n)
        p_mock.assert_called_with([n])
        ui_mock.assert_called_with(n)
