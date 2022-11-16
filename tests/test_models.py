from mundus import models
from mundus.models import cache
from mundus.models.models import containers
from mundus.models.relationship import RelationshipList
from mundus.settings import ID_FIELD, TS_FIELD
from mundus.exceptions import SchemaError as MundusSchemaError
from mundus import settings

import warnings, pytest, mundus, importlib, sys, os
from pytest import fixture
from unittest import mock
from jsonschema.exceptions import ValidationError, SchemaError

simple = """{
        "$schema": "http://json-schema.org/draft-07/hyper-schema#",
        "$id": "http://unis.open.sice.indiana.edu/schema/tester/goodschema",
        "description": "Base entity type for network resources.  Entities assicated with a network.",
        "title": "GoodSchema",
        "type": "object",
        "required": [
            "foo", "bar"
        ],
        "properties": {
            "foo": {
                "description": "State of the network entity as of last update.",
                "type": "string",
                "default": "UNKNOWN"
            },
            "bar": {
                "description": "URN for the entity if applicable",
                "type": "string"
            }
        }
    }"""

class MockContainer(object):
    def __init__(self):
        self._add_rel_called = []

    def commit(self, v): pass
    def add(self, v):
        if v.container() is None:
            v._set_container(self)
    def _add_relationship(self, rel):
        self._add_rel_called.append(rel)

    def find_relationship(self, href): return [5, 2, 8]

@fixture
def mock_container(monkeypatch):
    container = MockContainer()
    def mock_container(x):
        return container
    monkeypatch.setattr(containers, "get_container", mock_container)
    return container
    
@fixture
def schema_flat():
    return "http://unis.open.sice.indiana.edu/schema/2.0.0/entities/topology/networkresource"
@fixture
def schema_complex():
    return "http://unis.open.sice.indiana.edu/schema/2.0.0/entities/topology/computenode"
@fixture
def good_schema():
    return simple

@fixture
def partial_schema():
    return """{
        "$schema": "http://json-schema.org/draft-07/hyper-schema#",
        "$id": "http://unis.open.sice.indiana.edu/schema/tester/partial",
        "description": "Base entity type for network resources.  Entities assicated with a network.",
        "title": "PartialSchema",
        "type": "object",
        "required": [
            "foo", "bar"
        ],
        "properties": {
            "foo": {
                "description": "State of the network entity as of last update.",
                "type": "string"
            },
            "bar": {
                "description": "URN for the entity if applicable",
                "type": "string"
            },
            "baz": {
                "type": "object",
                "additionalProperties": "string",
                "properties": {
                  "test": { "type": "integer" }
                }
            }
        }
    }"""

def test_class_factory(schema_flat, mock_container):
    cls = models.get_class(schema_flat)

def test_class_factory_complex(schema_complex, mock_container):
    cls = models.get_class(schema_complex)

def test_class_factory_file(good_schema, mock_container):
    with mock.patch("builtins.open", mock.mock_open(read_data=good_schema)) as mock_file:
        cls = models.get_class("testfile", is_file=True)
        assert list(cls.__slots__) == ['foo', 'bar']

def test_class_partial_file(partial_schema, mock_container):
    with mock.patch("builtins.open", mock.mock_open(read_data=partial_schema)) as mock_file:
        cls = models.get_class("partialfile", is_file=True)
        cls = models.get_class(cls._schema["$id"] + "/baz", class_name="partialtest")
        assert hasattr(cls, "__dict__")

def test_class_bad_frag(schema_flat, mock_container):
    cls = models.get_class(schema_flat)
    try: cls = cache._cache(schema_flat + "#/badtest")
    except MundusSchemaError:
        assert True
        return
    assert False

def test_factory_cache(schema_flat, mock_container):
    cls = models.get_class(schema_flat)
    cls2 = models.get_class(schema_flat)

    assert cls == cls2

def test_constructor(schema_flat, mock_container):
    c = models.get_class(schema_flat)()

def test_constructor_values(schema_flat, mock_container):
    c = models.get_class(schema_flat)({"urn": "value:foo:bar"})

def test_constructor_restriction(schema_flat, mock_container):
    try:
        models.get_class(schema_flat)({"urn": "value:foo:bar", "foo": "baz"})
    except AttributeError:
        assert True
        return
    assert False

def test_dict_converion(schema_complex, mock_container):
    c = models.get_class(schema_complex)()
    d = dict(c)
    d.pop(ID_FIELD)
    assert d == {'name': '', 'description': '', 'status': 'UNKNOWN', 'urn': '', 'expires': 0, 'persistentStorage': [], 'memory': {'type': '', 'size': 0, 'options': {}}, 'cpus': [], 'cache': [], ':ts': 0, ':type': 'http://unis.open.sice.indiana.edu/schema/2.0.0/entities/topology/computenode#'}

def test_abstract_dict_conversion(schema_complex, mock_container):
    c = models.get_class(schema_complex)()
    c.memory.options.foo = 5
    d = dict(c)
    d.pop(ID_FIELD)
    assert d == {'name': '', 'description': '', 'status': 'UNKNOWN', 'urn': '', 'expires': 0, 'persistentStorage': [], 'memory': {'type': '', 'size': 0, 'options': {"foo": 5}}, 'cpus': [], 'cache': [], ':ts': 0, ':type': 'http://unis.open.sice.indiana.edu/schema/2.0.0/entities/topology/computenode#'}

def test_id(schema_flat, mock_container):
    c = models.get_class(schema_flat)()
    c2 = models.get_class(schema_flat)({ID_FIELD: "hello"})
    assert c.id
    assert c2.id == "hello"

def test_ts(schema_flat, mock_container):
    c = models.get_class(schema_flat)()
    c2 = models.get_class(schema_flat)({TS_FIELD: 10})
    assert c.ts == 0
    assert c2.ts == 10

def test_selfref(schema_complex, mock_container):
    c = models.get_class(schema_complex)()
    assert c.selfRef == f"nodes/{c.id}"

def test_collection(schema_complex, mock_container):
    c = models.get_class(schema_complex)()
    assert c.colRef == f"nodes"
    
def test_clone(schema_complex, mock_container):
    c = models.get_class(schema_complex)()
    c2 = c.clone()
    d1, d2 = dict(c), dict(c2)
    d1.pop(ID_FIELD)
    d2.pop(ID_FIELD)
    assert c is not c2
    assert d1 == d2

def test_value_init(schema_complex, mock_container):
    c = models.get_class(schema_complex)({"urn": "test:urn"})
    assert c.urn == "test:urn"

def test_value_set(schema_complex, mock_container):
    c = models.get_class(schema_complex)()
    c.urn = "test:urn"
    assert c.urn == "test:urn"

def test_deep_set(schema_complex, mock_container):
    c = models.get_class(schema_complex)()
    c.memory.type = "test_type"
    assert c.memory.type == "test_type"

def test_array_get(schema_complex, mock_container):
    c = models.get_class(schema_complex)({"cache": [{"level": 3}, {}]})

    assert dict(c.cache[0]) == {"level": 3, "size": 0, "serves": []}
    assert dict(c.cache[1]) == {"level": 0, "size": 0, "serves": []}

def test_array_slice(schema_complex, mock_container):
    c = models.get_class(schema_complex)({"cache": [{"level": 3}, {}]})
    result = [{"level": 3, "size":0, "serves": []},
              {"level": 0, "size":0, "serves": []}]
    assert [dict(v) for v in c.cache[0:]] == result
    assert [dict(v) for v in c.cache[-2:]] == result
    assert [dict(v) for v in c.cache[:]] == result
    assert [dict(v) for v in c.cache[:1]] == [{"level": 3, "size": 0, "serves": []}]
    
def test_array_append_empty(schema_complex, mock_container):
    c = models.get_class(schema_complex)()
    c.cache.append({})
    assert dict(c.cache[0]) == {"level": 0, "size": 0, "serves": []}

def test_array_append_partial(schema_complex, mock_container):
    c = models.get_class(schema_complex)()
    c.cache.append({"level": 5, "size": 10})
    assert dict(c.cache[0]) == {"level": 5, "size": 10, "serves": []}

def test_array_remove(schema_complex, mock_container):
    c = models.get_class(schema_complex)()
    c.cache.append({})
    c.cache.append({"level": 5, "size": 10})
    c.cache.pop(0)
    assert len(c.cache) == 1
    assert c.cache[0].level == 5
    assert c.cache[0].size == 10

def test_array_setitem(schema_complex, mock_container):
    c = models.get_class(schema_complex)()
    c.cache.append({})
    c.cache.append({})
    c.cache[0] = {"level": 10}
    assert c.cache[1].level == 0
    assert c.cache[0].level == 10

def test_array_insert(schema_complex, mock_container):
    c = models.get_class(schema_complex)()
    c.cache.append({})
    c.cache.append({})
    c.cache.insert(1, {"level": 10})
    assert len(c.cache) == 3
    assert c.cache[0].level == 0
    assert c.cache[1].level == 10
    assert c.cache[2].level == 0

def test_array_iter(schema_complex, mock_container):
    count = 0
    c = models.get_class(schema_complex)()
    c.cache.append({"level": 0})
    c.cache.append({"level": 1})
    c.cache.append({"level": 2})
    for i,v in enumerate(c.cache):
        assert i == v.level

def test_model_locks(schema_complex, mock_container):
    c = models.get_class(schema_complex)()
    c.name = "hello"
    c.cache.append({})
    c.memory.type = "test"
    assert "name" in c._locks
    assert 0 in c.cache._locks
    assert "type" in c.memory._locks

def test_links_get(schema_complex, mock_container):
    c = models.get_class(schema_complex)()
    links = c.measuredBy()
    assert isinstance(links, RelationshipList)

def test_links_push_target(schema_complex, mock_container):
    c = models.get_class(schema_complex)({ID_FIELD: "a"})
    c2 = models.get_class(schema_complex)({ID_FIELD: "b"})
    links = c.measuredBy().append(c2)
    assert len(mock_container._add_rel_called) == 1
    assert dict(mock_container._add_rel_called[0]) == {"subject": "nodes/b", "target": "nodes/a"}

def test_links_append_index(schema_complex, mock_container):
    c = models.get_class(schema_complex)({ID_FIELD: "a"})
    c2 = models.get_class(schema_complex)({ID_FIELD: "b"})
    links = c.measuredBy().append(c2, idx=0)
    assert len(mock_container._add_rel_called) == 1
    assert dict(mock_container._add_rel_called[0]) == {"subject": "nodes/b", "target": "nodes/a", "index": 0}
    
def test_links_push_subject(schema_complex, mock_container):
    c = models.get_class(schema_complex)({ID_FIELD: "a"})
    c2 = models.get_class(schema_complex)({ID_FIELD: "b"})
    links = c.measures().append(c2)
    assert len(mock_container._add_rel_called) == 1
    assert dict(mock_container._add_rel_called[0]) == {"subject": "nodes/a", "target": "nodes/b"}

def test_link_query_len(schema_complex, mock_container):
    c = models.get_class(schema_complex)({ID_FIELD: "a"})
    links = c.measures()
    assert len(links) == 3
    
def test_link_query(schema_complex, mock_container):
    c = models.get_class(schema_complex)({ID_FIELD: "a"})
    links = c.measures()
    for i,v in enumerate(links):
        assert v == [5, 2, 8][i]

def test_model_unlock(schema_complex, mock_container):
    c = models.get_class(schema_complex)()
    c.name = "hello"
    c.cache.append({})
    c.memory.type = "test"
    c.unlock()
    assert "name" not in c._locks
    assert 0 not in c.cache._locks
    assert "type" not in c.memory._locks

def test_bad_validate(schema_complex, mock_container):
    c = models.get_class(schema_complex)({"name": "foo"})
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            c.validate()
    except ValidationError:
        assert True
        return
    assert False

def test_good_validate(schema_complex, mock_container):
    c = models.get_class(schema_complex)({"name": "bar"})
    c.cpus.append({})
    c.cache.append({})
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        c.validate()

def test_callback(schema_complex, mock_container):
    ls = []
    c = models.get_class(schema_complex)({"name": "test"})
    c.addCallback(lambda x,ch: ls.append((x.name, ch)))
    c._event_callback("channel")
    assert ("test", "channel") in ls

def test_callback_cascade(schema_complex, mock_container):
    ls = []
    c = models.get_class(schema_complex)({"name": "test", "urn": "foo"})
    c.addCallback(lambda x,ch: ls.append((x.name, ch)))
    c.addCallback(lambda x,ch: ls.append((x.urn, ch)))
    c._event_callback("channel")
    assert ("test", "channel") in ls
    assert ("foo", "channel") in ls

def test_merge_error(schema_complex, mock_container):
    c = models.get_class(schema_complex)({TS_FIELD: 5})
    c2 = models.get_class(schema_complex)({TS_FIELD: 10, "name": "test", "urn": "foobar"})
    try:
        c._merge(c2)
    except ValueError:
        assert True
        return
    assert False

def test_simple_merge(schema_complex, mock_container):
    c = models.get_class(schema_complex)({ID_FIELD: "5", TS_FIELD: 5})
    c2 = models.get_class(schema_complex)({ID_FIELD: "5", TS_FIELD: 10, "name": "test", "urn": "foobar"})

    c._merge(c2)
    assert c.name == "test"
    assert c.urn == "foobar"

def test_skip_merge(schema_complex, mock_container):
    c = models.get_class(schema_complex)({ID_FIELD: "5", TS_FIELD: 5})
    c2 = models.get_class(schema_complex)({ID_FIELD: "5", TS_FIELD: 10, "name": "test", "urn": "foobar"})

    c2._merge(c)
    assert c.name == ""
    assert c.urn == ""

def test_deep_merge(schema_complex, mock_container):
    c = models.get_class(schema_complex)({ID_FIELD: "5", TS_FIELD: 5})
    c2 = models.get_class(schema_complex)({ID_FIELD: "5", TS_FIELD: 10,  "memory": {"options": { "test": "hello" }}})

    c._merge(c2)
    assert c.memory.options.test == "hello"

def test_lock_merge(schema_complex, mock_container):
    c = models.get_class(schema_complex)({ID_FIELD: "5", TS_FIELD: 5})
    c2 = models.get_class(schema_complex)({ID_FIELD: "5", TS_FIELD: 10, "name": "new", "urn": "foobar"})

    c.name = "old"
    
    c._merge(c2)
    assert c.name == "old"
    assert c.urn == "foobar"

def test_array_merge(schema_complex, mock_container):
    c = models.get_class(schema_complex)({ID_FIELD: "5", TS_FIELD: 5, "cpus": [{"speed": 10, "model": "a"}]})
    c2 = models.get_class(schema_complex)({ID_FIELD: "5", TS_FIELD: 10, "cpus": [{"speed": 14}, {}]})
    
    c._merge(c2)
    assert len(c.cpus) == 2
    assert c.cpus[0].speed == 14
    assert c.cpus[0].model == ""
    assert c.cpus[1].speed == 0

def test_array_lock(schema_complex, mock_container):
    c = models.get_class(schema_complex)({ID_FIELD: "5", TS_FIELD: 5, "cpus": [{"speed": 10, "model": "a"}]})
    c2 = models.get_class(schema_complex)({ID_FIELD: "5", TS_FIELD: 10, "cpus": [{"speed": 14}, {}]})

    c.cpus[0].speed = 12
    assert c.cpus[0].speed == 12
    assert "speed" in c.cpus[0]._locks

    c._merge(c2)
    assert len(c.cpus) == 2
    assert c.cpus[0].speed == 12
    assert c.cpus[0].model == ""
    assert c.cpus[1].speed == 0


def mock_open(*args, **kwargs):
    if args[0] == "archive":
        return mock.mock_open(read_data="""
cache:
  - schema-07
models:
  - simple
""")(*args, **kwargs)
    if args[0] == "archive_bad":
        return mock.mock_open(read_data="""
cache:
  - weird:///schema-07
models:
  - simple
""")(*args, **kwargs)
    elif args[0].startswith("schema-07"):
        return mock.mock_open(read_data="""
{
    "$schema": "http://json-schema.org/draft-07/schema#",
    "$id": "http://json-schema.org/draft-07/schema#",
    "title": "Core schema meta-schema",
    "definitions": {
        "schemaArray": {
            "type": "array",
            "minItems": 1,
            "items": { "$ref": "#" }
        },
        "nonNegativeInteger": {
            "type": "integer",
            "minimum": 0
        },
        "nonNegativeIntegerDefault0": {
            "allOf": [
                { "$ref": "#/definitions/nonNegativeInteger" },
                { "default": 0 }
            ]
        },
        "simpleTypes": {
            "enum": [
                "array",
                "boolean",
                "integer",
                "null",
                "number",
                "object",
                "string"
            ]
        },
        "stringArray": {
            "type": "array",
            "items": { "type": "string" },
            "uniqueItems": true,
            "default": []
        }
    },
    "type": ["object", "boolean"],
    "properties": {
        "$id": {
            "type": "string",
            "format": "uri-reference"
        },
        "$schema": {
            "type": "string",
            "format": "uri"
        },
        "$ref": {
            "type": "string",
            "format": "uri-reference"
        },
        "$comment": {
            "type": "string"
        },
        "title": {
            "type": "string"
        },
        "description": {
            "type": "string"
        },
        "default": true,
        "readOnly": {
            "type": "boolean",
            "default": false
        },
        "writeOnly": {
            "type": "boolean",
            "default": false
        },
        "examples": {
            "type": "array",
            "items": true
        },
        "multipleOf": {
            "type": "number",
            "exclusiveMinimum": 0
        },
        "maximum": {
            "type": "number"
        },
        "exclusiveMaximum": {
            "type": "number"
        },
        "minimum": {
            "type": "number"
        },
        "exclusiveMinimum": {
            "type": "number"
        },
        "maxLength": { "$ref": "#/definitions/nonNegativeInteger" },
        "minLength": { "$ref": "#/definitions/nonNegativeIntegerDefault0" },
        "pattern": {
            "type": "string",
            "format": "regex"
        },
        "additionalItems": { "$ref": "#" },
        "items": {
            "anyOf": [
                { "$ref": "#" },
                { "$ref": "#/definitions/schemaArray" }
            ],
            "default": true
        },
        "maxItems": { "$ref": "#/definitions/nonNegativeInteger" },
        "minItems": { "$ref": "#/definitions/nonNegativeIntegerDefault0" },
        "uniqueItems": {
            "type": "boolean",
            "default": false
        },
        "contains": { "$ref": "#" },
        "maxProperties": { "$ref": "#/definitions/nonNegativeInteger" },
        "minProperties": { "$ref": "#/definitions/nonNegativeIntegerDefault0" },
        "required": { "$ref": "#/definitions/stringArray" },
        "additionalProperties": { "$ref": "#" },
        "definitions": {
            "type": "object",
            "additionalProperties": { "$ref": "#" },
            "default": {}
        },
        "properties": {
            "type": "object",
            "additionalProperties": { "$ref": "#" },
            "default": {}
        },
        "patternProperties": {
            "type": "object",
            "additionalProperties": { "$ref": "#" },
            "propertyNames": { "format": "regex" },
            "default": {}
        },
        "dependencies": {
            "type": "object",
            "additionalProperties": {
                "anyOf": [
                    { "$ref": "#" },
                    { "$ref": "#/definitions/stringArray" }
                ]
            }
        },
        "propertyNames": { "$ref": "#" },
        "const": true,
        "enum": {
            "type": "array",
            "items": true,
            "minItems": 1,
            "uniqueItems": true
        },
        "type": {
            "anyOf": [
                { "$ref": "#/definitions/simpleTypes" },
                {
                    "type": "array",
                    "items": { "$ref": "#/definitions/simpleTypes" },
                    "minItems": 1,
                    "uniqueItems": true
                }
            ]
        },
        "format": { "type": "string" },
        "contentMediaType": { "type": "string" },
        "contentEncoding": { "type": "string" },
        "if": { "$ref": "#" },
        "then": { "$ref": "#" },
        "else": { "$ref": "#" },
        "allOf": { "$ref": "#/definitions/schemaArray" },
        "anyOf": { "$ref": "#/definitions/schemaArray" },
        "oneOf": { "$ref": "#/definitions/schemaArray" },
        "not": { "$ref": "#" }
    },
    "default": true
}
""")(*args, **kwargs)
    elif args[0].startswith("simple"):
        return mock.mock_open(read_data=simple)(*args, **kwargs)

def test_archive():
    os.environ["MUNDUS_SCHEMA_ARCHIVE"] = "archive"
    with mock.patch("builtins.open", mock_open) as mock_file:
        importlib.reload(settings)
        importlib.reload(cache)
        importlib.reload(models)
        assert len(cache._CACHE.keys()) == 4
        assert len(models.cache._CLASSES.keys()) == 1

def test_bad_archive():
    os.environ["MUNDUS_SCHEMA_ARCHIVE"] = "archive_bad"
    with mock.patch("builtins.open", mock_open) as mock_file:
        importlib.reload(settings)
        importlib.reload(cache)
        try:
            importlib.reload(models)
        except ValueError:
            assert True
            return
        assert False
