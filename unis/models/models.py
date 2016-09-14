import copy
import datetime
import httplib2
import json
import jsonschema
import time
import re
import weakref

from unis.models.settings import SCHEMAS, JSON_SCHEMA_SCHEMA, JSON_SCHEMA_HYPER, \
    JSON_SCHEMA_LINKS, JSON_SCHEMAS_ROOT, SCHEMAS_LOCAL, SCHEMA_CACHE_DIR
from unis.utils.pubsub import Events

# Define the default JSON Schemas that are defined in the JSON schema RFC
JSON_SCHEMA        = json.loads(open(JSON_SCHEMAS_ROOT + "/schema").read())
HYPER_SCHEMA       = json.loads(open(JSON_SCHEMAS_ROOT + "/hyper-schema").read())
HYPER_LINKS_SCHEMA = json.loads(open(JSON_SCHEMAS_ROOT + "/links").read())

CACHE = {
    JSON_SCHEMA_SCHEMA: JSON_SCHEMA,
    JSON_SCHEMA_HYPER: HYPER_SCHEMA,
    JSON_SCHEMA_LINKS: HYPER_LINKS_SCHEMA,
}

# For internal use only
class JSONObjectMeta(type):
    class AttrDict(object):
        def __init__(self, **kwargs):
            self.__dict__ = kwargs
        def __set__(self, obj, value):
            self.__dict__[obj] = value
        def __get__(self, obj, ty=None):
            if not obj:
                return self
            return self.__dict__[obj]
        def __delete__(self, obj):
            del self.__dict__[obj]
    def __init__(cls, name, bases, classDict):
        super(JSONObjectMeta, cls).__init__(name, bases, classDict)
        def get_virt(self, n):
            v = cls.__meta__.__get__(self)
            if n in v:
                return v[n]
            else:
                raise AttributeError("No local or remote attribute '{n}'".format(n = n))
        def set_virt(self, n, v):
            cls.__meta__.__get__(self)[n] = v
            return v
        
        cls.__meta__ = JSONObjectMeta.AttrDict()
        cls._models = {}
        cls.get_virtual = get_virt
        cls.set_virtual = set_virt
        cls.remoteObject = lambda obj: isinstance(obj, UnisObject) and not obj._local
    
    def __call__(cls, *args, **kwargs):
        instance = super(JSONObjectMeta, cls).__call__()
        cls.__meta__.__set__(instance, {})
        instance.initialize(*args, **kwargs)
        return instance


class UnisList(metaclass = JSONObjectMeta):
    class iter(object):
        def __init__(self, ls):
            self.ls = ls
            self.index = 0
        def __next__(self):
            if self.index > len(self.ls.items):
                raise StopIteration()
            item = self.index
            self.index += 1
            return self.ls.items[item]
    
    def initialize(self, model, parent, *args):
        for arg in args:
            assert isinstance(arg, model)
        self.model = model
        self._parent = parent
        self.items = []
        for item in args:
            self.items.append(item)
    
    def append(self, item):
        if item.get_virtual("_nocol"):
            item._parent = self
        assert isinstance(item, self.model), "{t1} is not of type {t2}".format(t1 = type(item), t2 = self.model)
        self.items.append(item)
        if not item._local or item.get_virtual("_nocol"):
            if item.get_virtual("_nocol"):
                item._parent = self
            self._parent._dirty = True
            self._parent.update()
    
    def update(self):
        self._parent._dirty = True
        self._parent.update()
        
    def to_JSON(self):
        tmpResult = []
        for item in self.items:
            if isinstance(item, UnisList):
                tmpResult.append(item.to_JSON())
            elif isinstance(item, UnisObject):
                if not item._local:
                    tmpResult.append({ "rel": "full", "href": item.selfRef })
                elif item.get_virtual("_nocol"):
                    tmpResult.append(item.to_JSON())
            else:
                tmpResult.append(item)
        return tmpResult
    
    def __getitem__(self, key):
        return self.items[key]
    def __setitem__(self, key, v):
        assert isinstance(v, self.model), "{t1} is not of type {t2}".format(t1 = type(v), t2 = self.model)
        self.items[key] = v
        if not v._local or v.get_virtual("_nocol"):
            if v.get_virtual("_nocol"):
                v._parent = self
            self._parent._dirty = True
            self._parent.update()
    def __delitem__(self, key):
        self.items.__delitem__(key)
    def __iter__(self):
        return iter(self.items)
    def __repr__(self):
        return self.items.__repr__()
    def __str__(self):
        return self.items.__str__()

class UnisObject(metaclass = JSONObjectMeta):
    def initialize(self, src={}, runtime=None, set_attr=True, defer=False, local_only=True):
        assert isinstance(src, dict), "{t} src must be of type dict".format(t = type(self))
        
        self._runtime = runtime
        self._collection = None
        self._defer = defer
        self._dirty = False
        self._pending = False
        self._local = local_only
        self._nocol = False
        self._override_virtual = False
        
        for k, v in src.items():
            if set_attr:
                self.__dict__[k] = v
            else:
                self._override_virtual = True
                self.set_virtual(k, v)
    
    def __getattribute__(self, n):
        if n in ["get_virtual", "__dict__"]:
            return super(UnisObject, self).__getattribute__(n)
        else:
            v = super(UnisObject, self).__getattribute__(n)
            if n in self.__dict__:
                if isinstance(v, list):
                    v = self._resolve_list(v, n)
                elif isinstance(v, dict):
                    v = self._resolve_dict(v)
                self.__dict__[n] = v
            return v
    
    def __getattr__(self, n):
        return self.get_virtual(n)

    
    def __setattr__(self, n, v):
        self.set_virtual("_lasttouched", datetime.datetime.utcnow())
        
        if n in self.__dict__:
            if isinstance(v, list):
                v = UnisList(self._models.get(n, UnisObject), self, *v)
            if v == self.__dict__[n]:
                return
            super(UnisObject, self).__setattr__(n, v)
            self.set_virtual("_dirty", True)
            if not self._local:
                if isinstance(v, UnisObject):
                    model = self._models.get(n, type(v))
                    if model != type(v):
                        raise ValueError("{t1}.{n} expects {t2} - got {t3}".format(t1=type(self), n=n, t2=model, t3=type(v)))
                        
                    if not v._local:
                        self._runtime.insert(v)
                        self.update()
                else:
                    self.update()
            else:
                self.validate()
        else:
            self.set_virtual(n, v)
    
    def isPending(self):
        return self._pending
    def modified(self):
        return self._lasttouched
    def isDeferred(self):
        return self._defer
    def setDeferred(self, n):
        self._defer = n
    
    
    def _resolve_list(self, ls, n):
        tmpResult = UnisList(self._models.get(n, UnisObject), self)
        for i in ls:
            if isinstance(i, dict):
                tmpResult.items.append(self._resolve_dict(i))
            elif isinstance(i, list):
                tmpResult.items.append(self._resolve_list(i, n))
            else:
                tmpResult.items.append(i)
        return tmpResult
        
    def _resolve_dict(self, o):
        if "$schema" in o:
            if self._local:
                model = self._schemaLoader.get_class(o["$schema"])
                return model(o, self._runtime)
            else:
                return self._runtime.insert(o)
        elif "href" in o:
            if self._runtime:
                return self._runtime.find(o["href"])
            else:
                return o
        else:
            # Convert object and cache
            o = UnisObject(o, self._runtime)
            o.set_virtual("_nocol", True)
            o.set_virtual("_parent", self)
            return o
    
    def to_JSON(self):
        tmpResult = {}
        for k, v in self.__dict__.items():
            if isinstance(v, UnisObject):
                if not v._local:
                    if v._schema:
                        tmpResult[k] = { "rel": "full", "href": v.selfRef }
                    else:
                        tmpResult[k] = v.to_JSON()
                if v.get_virtual("_nocol"):
                    tmpResult[k] = v.to_JSON()
            elif isinstance(v, UnisList):
                tmpResult[k] = v.to_JSON()
            else:
                tmpResult[k] = v
        return tmpResult
    
    def commit(self, n=None):
        if not n:
            if self._local:
                if self._runtime:
                    self.__dict__["ts"] = int(time.time()) * 1000000
                    self._local = False
                    self._dirty = True
                    self.update()
                else:
                    raise AttributeError("Object does not have a registered runtime")
        else:
            if n not in self.__dict__:
                self.__dict__[n] = self.get_virtual(n)
                if not self._local:
                    self._dirty = True
                    self.update()
                elif self.get_virtual("_nocol"):
                    p = self.get_virtual("_parent")
                    p._dirty = True
                    p.update(True)
    
    def update(self, force = False):
        if self.get_virtual("_nocol"):
            self._parent._dirty = True
            self._parent.update(force)
        else:
            update = False
            if force:
                self.validate()
                update = True
            elif (self._dirty and not self._pending):
                self.validate()
                if not self._defer:
                    update = True
            if update and not self._local:
                self._pending = True
                self.ts = int(time.time()) * 1000000
                self._runtime.update(self)
    def flush(self):
        if self._dirty and not self._pending:
            self.__dict__["ts"] = int(time.time()) * 1000000
            self._pending = True
            self._runtime.update(self)
    
    def validate(self, validate_id=False):
        if self._schema and self._resolver:
            uid = self.id
            if not validate_id:
                self.__dict__["id"] = "tmpid"
            jsonschema.validate(self.to_JSON(), self._schema, resolver = self._resolver)
            self.__dict__["id"] = uid
        else:
            raise AttributeError("No schema found for object")


def schemaMetaFactory(name, schema, parents = [JSONObjectMeta], loader=None):
    assert isinstance(schema, dict), "schema is not of type dict"
    class SchemaMetaClass(*parents):
        def __init__(cls, name, bases, classDict):
            super(SchemaMetaClass, cls).__init__(name=name, bases=bases, classDict=classDict)
            cls._schema = schema
            cls._schemaLoader = loader
            cls._models = {}
            cls._resolver = jsonschema.RefResolver(schema["id"], schema, loader._CACHE)
            cls.__doc__ = schema.get("description", None)
            
                # This is a good idea, but it requires a ton more work to function correctly with HyperSchema
                #if v.get("type", None) == "array" and "items" in v and "$ref" in v["items"]:
                #    cls._models[k] = loader.get_class(v["items"]["$ref"])
        
        def __call__(meta, *args, **kwargs):
            # TODO:
            #      Revisit how to move this to class __init__ instead of __call__
            instance = super(SchemaMetaClass, meta).__call__(*args, **kwargs)
            instance.__dict__["$schema"] = meta._schema["id"]
            if schema.get("type", "object") == "object":
                for k, v in schema.get("properties", {}).items():
                    if k not in instance.__dict__:
                        if instance._override_virtual and k in instance.__meta__:
                            instance.__dict__[k] = instance.__meta__[k]
                        else:
                            defaults = { "string": "", "boolean": False, "integer": 0, "number": 0, "object": {}, "array": [] }
                            instance.__dict__[k] = v.get("default", defaults.get(v.get("type", "object"), None))
            return instance
            
    return SchemaMetaClass


class SchemasLoader(object):
    """JSON Schema Loader"""
    _CACHE = {}
    _CLASSES_CACHE = {}
    _LOCATIONS = {}
    
    def __init__(self, locations=None, cache=None, class_cache=None):
        assert isinstance(locations, (dict, type(None))), \
            "locations is not of type dict or None."
        assert isinstance(cache, (dict, type(None))), \
            "cache is not of type dict or None."
        assert isinstance(class_cache, (dict, type(None))), \
            "class_cache is not of type dict or None."
        self._LOCATIONS = locations or {}
        self._CACHE = cache or {}
        self._CLASSES_CACHE = class_cache or {}
    
    def get(self, uri):
        if uri in self._CACHE:
            return self._CACHE[uri]
        location = self._LOCATIONS.get(uri, uri)
        return self._load_schema(location)
        
    def get_class(self, schema_uri, class_name = None):
        if schema_uri in self._CLASSES_CACHE:
            return self._CLASSES_CACHE[schema_uri]
        
        schema = self.get(schema_uri)
        class_name = class_name or str(schema.get("name", None))
        
        ### TODO ###
        # Make parent resolution work for all types of allof fields
        ############
        parent_uri = schema.get("allOf", [])
        parent_metas = []
        parents = []
        if parent_uri:
            for parent in parent_uri:
                if "$ref" in parent:
                    re_str = "http[s]?://(?:[^:/]+)(?::[0-9]{1-4})?/(?:[^/]+/)*(?P<sname>[^/]+)#$"
                    matches = re.compile(re_str).match(parent["$ref"])
                    assert matches, "$ref in allof must be a full url"
                    cls = self.get_class(parent["$ref"])
                    parents.append(cls)
                    parent_metas.append(type(cls))
                else:
                    raise AttributeError("allof must be remote references")
        else:
            parents = [UnisObject]
            parent_metas = [JSONObjectMeta]
            
        if not class_name:
            raise AttributeError("class_name is not defined by the provided schema or the client")
            
        meta = schemaMetaFactory("{n}Meta".format(n = class_name), schema, parent_metas, self)
        self._CLASSES_CACHE[schema_uri] = meta(class_name, tuple(parents), {})
        return self._CLASSES_CACHE[schema_uri]
    
    def _load_schema(self, name):
         raise NotImplementedError("Schemas._load_schema is not implemented")

class SchemasHTTPLib2(SchemasLoader):
    """Relies on HTTPLib2 HTTP client to load schemas"""
    def __init__(self, http, locations=None, cache=None, class_cache=None):
        super(SchemasHTTPLib2, self).__init__(locations, cache, class_cache)
        self._http = http
    
    def _load_schema(self, uri):
        resp, content = self._http.request(uri, "GET")
        self._CACHE[uri] = json.loads(content.decode())
        return self._CACHE[uri]

"""Load a locally stored copy of the schemas if requested."""
if SCHEMAS_LOCAL:
    for s in SCHEMAS.keys():
        try:
            CACHE[SCHEMAS[s]] = json.loads(open(JSON_SCHEMAS_ROOT + "/" + s).read())
        except Exception as e:
            print("Error loading cached schema for {0}: {1}".format(e, s))

http_client = httplib2.Http(SCHEMA_CACHE_DIR)
schemaLoader = SchemasHTTPLib2(http_client, cache=CACHE)

JSONSchema = schemaLoader.get_class(JSON_SCHEMA_SCHEMA, "JSONSchema")
HyperSchema = schemaLoader.get_class(JSON_SCHEMA_HYPER, "HyperSchema")
HyperLink = schemaLoader.get_class(JSON_SCHEMA_LINKS, "HyperLink")
