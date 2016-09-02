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
import unittest
import unittest.mock as mock
from unittest.mock import MagicMock

# XXX: why do we have to load Runtime first?
from unis.runtime import Runtime
from unis.runtime.settings import SCHEMAS
from unis.models.models import UnisObject, schemaLoader
from unis.models import Node, Exnode, Extent

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

class NetworkResourceTest(unittest.TestCase):

    VALID_NODE = {
        "id": "mynodeid",
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

    def test_validate(self):
        from jsonschema.exceptions import ValidationError
        
        good = Node(src=NetworkResourceTest.VALID_NODE)
        bad  = Node(src=NetworkResourceTest.INVALID_NODE)
        
        self.assertEquals(good.validate(validate_id=True), None)
        self.assertRaises(ValidationError, bad.validate, validate_id=True)
        
class ExnodeTest(unittest.TestCase):
    pass
