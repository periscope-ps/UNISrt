import json, jsonschema, requests, logging
from urllib import parse

from mundus.exceptions import SchemaError

log = logging.getLogger("mundus.models.cache")
_CLASSES, _SLOTLESS, _CACHE = {}, {}, {}

def _cache(schema_url, is_file=False):
    netloc, frag = parse.urldefrag(schema_url)
    if netloc[-1] != "#": netloc += "#"

    if netloc in _CACHE:
        schema = _CACHE[netloc]
    else:
        if is_file:
            log.debug(f"Loading schema file - '{schema_url}'")
            with open(schema_url) as f:
                schema = json.load(f)
        else:
            log.debug(f"GET remote schema - '{netloc}'")
            r = requests.get(netloc)
            r.raise_for_status()
            schema = r.json()
        if schema["$id"][-1] != "#": schema["$id"] += "#"
        if not frag: _CACHE[schema["$id"]] = schema

    if frag:
        try:
            for f in frag.split('/'):
                if f:
                    schema = schema["properties"][f]
                    if schema.get("type", "") == "array":
                        schema = schema.get("items", {})
            schema = {**schema}
            schema["$id"] = f"{netloc}{frag}"
        except KeyError as e:
            raise SchemaError(f"Invalid schema at '{netloc}{frag}'") from e
    return schema

class add_general_meta(type):
    def __new__(mcls, name, parents, d, schema):
        log.debug(f"--Generating general type '{schema['$id']}'")
        return super().__new__(mcls, name, parents, d)

    def __init__(cls, name, parents, d, schema):
        ty_map = {"null": None, "string": "", "boolean": False,
                  "number": 0, "integer": 0, "object": {}, "array": []}
        cls._schema = schema
        cls.__name__ = cls.__qualname__ = name
        setattr(cls, ":type", schema["$id"])
        setattr(cls, "__doc__", schema.get("description", None))
        cls._defaults = {}

        kwargs = {}
        for k,v in schema.get("properties", {}).items():
            if not k.startswith(':'):
                kwargs[k] = v
        for k,v in kwargs.items():
            cls._defaults[k] = v.get("default", None) or ty_map[v["type"]]

class add_defaults_meta(type):
    def __new__(mcls, name, parents, d, schema):
        log.debug(f"--Loading properties '{schema['$id']}'")
        d["__slots__"] = tuple(_SLOTLESS[schema["$id"]][0].keys())
        return super().__new__(mcls, name, parents, d)

    def __init__(cls, name, parents, d, schema):
        ty_map = {"null": None, "string": "", "boolean": False,
                  "number": 0, "integer": 0, "object": {}, "array": []}
        cls._schema, cls._resolver = schema, jsonschema.RefResolver(schema["$id"], schema, _CACHE)
        cls._links = _SLOTLESS[schema["$id"]][1]
        cls.colRef = cls._links.get("collection", None)
        cls.__name__ = cls.__qualname__ = name
        cls.__mmro__ = tuple()
        setattr(cls, ":type", schema["$id"])
        setattr(cls, "__doc__", schema.get("description", None))
        cls._defaults = {}

        for k,v in _SLOTLESS[schema["$id"]][0].items():
            cls._defaults[k] = v.get("default", None) or ty_map[v["type"]]
            
    def __instancecheck__(self, other):
        return self.__mmro__[0] in getattr(other, "__mmro__", [])
    
def class_factory(schema_url, root_class, class_name=None, is_file=False):
    def normalize(x):
        x = parse.urldefrag(x)
        return f"{x[0]}#{x[1]}"
    def _build():
        parents, kwargs, links = [root_class], {}, {}
        log.debug(f"Constructing model for '{schema_url}'")
        schema = _cache(schema_url, is_file)
        if schema.get("allOf", []):
            parents = []
            for p in schema.get("allOf", []):
                parents.append(class_factory(p["$ref"], root_class, None))
        
        for p in parents:
            kwargs.update(**_SLOTLESS.get(getattr(p, ":type", None), [{}])[0])
            for k,v in getattr(p, "_links", {}).items():
                links[k] = v
        if "properties" in schema and not schema.get("additionalProperties", False):
            for k,v in schema["properties"].items():
                if not k.startswith(':'):
                    kwargs[k] = v
        else:
            cls = add_general_meta(class_name or schema["title"], (root_class,), {}, schema)
            _CLASSES[normalize(schema["$id"])] = cls
            return cls

        for d in schema.get("links", []):
            links[d["rel"]] = d["href"]

        _SLOTLESS[schema["$id"]] = (kwargs, links)
        cls = add_defaults_meta(class_name or schema["title"], (root_class,), {}, schema)
        _CLASSES[normalize(schema["$id"])] = cls
        mmro = set()
        for p in parents:
            for m in p.__mro__:
                mmro.add(m)
        cls.__mmro__ = tuple([cls] + list(mmro))
        return cls
    return _CLASSES.get(normalize(schema_url), None) or _build()
