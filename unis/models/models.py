import copy
import datetime
import httplib2
import json
import jsonschema
import time
import re
import weakref
import tempfile

from unis.models.settings import SCHEMAS, JSON_SCHEMA_SCHEMA, JSON_SCHEMA_HYPER, \
    JSON_SCHEMA_LINKS, JSON_SCHEMAS_ROOT, SCHEMAS_LOCAL
from unis.utils.pubsub import Events
from unis import logging

# Define the default JSON Schemas that are defined in the JSON schema RFC
#JSON_SCHEMA        = json.loads(open(JSON_SCHEMAS_ROOT + "/schema").read())
#HYPER_SCHEMA       = json.loads(open(JSON_SCHEMAS_ROOT + "/hyper-schema").read())
#HYPER_LINKS_SCHEMA = json.loads(open(JSON_SCHEMAS_ROOT + "/links").read())

CACHE = {
    #JSON_SCHEMA_SCHEMA: JSON_SCHEMA,
    #JSON_SCHEMA_HYPER: HYPER_SCHEMA,
    #JSON_SCHEMA_LINKS: HYPER_LINKS_SCHEMA,
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
        def has_virt(self, n):
            return n in cls.__meta__.__get__(self)
        
        cls.__meta__ = JSONObjectMeta.AttrDict()
        cls.__reserved__ = JSONObjectMeta.AttrDict()
        cls._models = {}
        cls.names = []
        cls.get_virtual = get_virt
        cls.set_virtual = set_virt
        cls.has_virtual = has_virt
        cls.remoteObject = lambda obj: isinstance(obj, UnisObject) and not obj._local
    
    def __call__(cls, *args, **kwargs):
        instance = super(JSONObjectMeta, cls).__call__()
        cls.__meta__.__set__(instance, {})
        cls.__reserved__.__set__(instance, {})
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
    
    @logging.info("UnisList")
    def initialize(self, model, parent, *args):
        self.model = model
        self._parent = parent
        self.items = []
        for item in args:
            if isinstance(item, dict) and item.get("$schema", None):
                if self._parent._runtime:
                    item = self._parent._runtime.insert(item)
                    item._defer = self._parent._defer
                    item._local = self._parent._local
            
            if isinstance(item, UnisObject):
                self._parent._waiting_on.add(item)
            self.items.append(item)
    
    @logging.info("UnisList")
    def append(self, item):
        assert (isinstance(item, self.model) or isinstance(item, LocalObject)), "{t1} is not of type {t2}".format(t1 = type(item), t2 = self.model)
        self.items.append(item)
        if isinstance(item, UnisObject):
            self._parent._waiting_on.add(item)
        if isinstance(item, LocalObject) or not getattr(item, "_local", True):
            if isinstance(item, LocalObject):
                item._parent = self._parent
            self.update()
    
    @logging.info("UnisList")
    def update(self):
        self._parent._dirty = True
        self._parent.update()
        
    @logging.info("UnisList")
    def to_JSON(self, include_virtuals=False):
        tmpResult = []
        for item in self.items:
            if isinstance(item, UnisList) or isinstance(item, LocalObject):
                tmpResult.append(item.to_JSON(include_virtuals))
            elif isinstance(item, UnisObject):
                if not item._local and hasattr(item, "selfRef"):
                    tmpResult.append({ "rel": "full", "href": item.selfRef })
            else:
                tmpResult.append(item)
        return tmpResult
    
    def __getitem__(self, key):
        v = self.items[key]
        if isinstance(v, dict):
            v = self._resolve_dict(v)
            self.items[key] = v
        elif isinstance(v, list):
            v = self._resolve_list(v, n)
            self.items[key] = v
        return v
    def __setitem__(self, key, v):
        assert (isinstance(v, self.model) or isinstance(v, LocalObject)), "{t1} is not of type {t2}".format(t1 = type(v), t2 = self.model)
        oldVal = self.items[key]
        if isinstance(oldVal, UnisObject):
            self._parent._waiting_on = self._parent._waiting_on - set(oldVal)
        self.items[key] = v
        if isinstance(v, LocalObject) or not getattr(v, "_local", True):
            if isinstance(v, LocalObject):
                v._parent = self._parent
            self.update()
        if isinstance(v, UnisObject):
            self._parent._waiting_on.add(v)
    def __delitem__(self, key):
        self.items.__delitem__(key)
    def __len__(self):
        return self.items.__len__()
    def __iter__(self):
        for i in range(len(self.items)):
            yield self[i]
    def __repr__(self):
        return getattr(self, 'items', []).__repr__()
    def __str__(self):
        return self.items.__str__()
        
    @logging.debug("UnisList")
    def _resolve_list(self, ls, n):
        return UnisList(self.model, self._parent, *ls)
    
    @logging.debug("UnisList")
    def _resolve_dict(self, o):
        if "$schema" in o:
            if self._parent._runtime:
                o = self._parent._runtime.insert(o)
                o._defer = self._parent._defer
                self._parent._waiting_on.add(o)
                if not self._parent._local:
                    o.commit()
        elif "href" in o:
            if self._parent._runtime:
                o = self._parent._runtime.find(o["href"])
            else:
                return o
        else:
            o = LocalObject(o, self._parent)
            
        assert isinstance(o, self.model), "expected model {t}, got {t2}".format(t = type(self.model), t2 = type(o))
        return o


class LocalObject(metaclass=JSONObjectMeta):
    @logging.info("LocalObject")
    def initialize(self, src, parent):
        self.set_virtual("_parent", parent)
        for k, v in src.items():
            self.__dict__[k] = v
    
    
    def __getattribute__(self, n):
        v = super(LocalObject, self).__getattribute__(n)
        if n != "__dict__" and n in self.__dict__:
            if isinstance(v, list):
                v = self._resolve_list(v, n)
            elif isinstance(v, dict):
                v = self._resolve_dict(v)
            self.__dict__[n] = v
        return v
    
    def __getattr__(self, n):
        return self.get_virtual(n)
    
    def __setattr__(self, n, v):
        oldVal = self.__dict__.get(n, None)
        if isinstance(oldVal, UnisObject):
            self._parent._waiting_on = self._parent._waiting_on - set(oldVal)
        if isinstance(oldVal, UnisList):
            self._parent._waiting_on = self._parent._waiting_on - set(list(oldVal))
        self.__dict__[n] = v
        if isinstance(v, LocalObject):
            v._parent = self._parent
        elif isinstance(v, UnisObject):
            self._parent._waiting_on.add(v)
        
        if not getattr(v, "_local", False):
            self.update()
    
    def __str__(self):
        return json.dumps(self.to_JSON(include_virtuals=True))
    
    @logging.debug("LocalObject")
    def _resolve_list(self, ls, n):
        return UnisList(self._models.get(n, UnisObject), self, *ls)
    
    @logging.debug("LocalObject")
    def _resolve_dict(self, o):
        if "$schema" in o:
            if self._parent._runtime:
                o = self._parent._runtime.insert(o)
                o._defer = self._parent._defer
                self._parent._waiting_on.add(o)
                if not self._parent._local:
                    o.commit()
        elif "href" in o:
            if self._parent._runtime:
                return self._parent._runtime.find(o["href"])
            else:
                return o
        else:
            o = LocalObject(o, self)
            return o
    
    @logging.info("LocalObject")
    def update(self):
        self._parent._dirty = True
        self._parent.update()


    @logging.info("LocalObject")
    def to_JSON(self, include_virtuals=False):
        tmpResult = {}
        for k, v in self.__dict__.items():
            if isinstance(v, UnisObject):
                if not v._local or include_virtuals:
                    if v._schema and hasattr(v, "selfRef"):
                        tmpResult[k] = { "rel": "full", "href": v.selfRef }
                    else:
                        tmpResult[k] = v.to_JSON(include_virtuals)
            elif isinstance(v, UnisList) or isinstance(v, LocalObject):
                tmpResult[k] = v.to_JSON(include_virtuals)
            elif isinstance(v, list):
                self.__dict__[k] = self._resolve_list(v, k)
                tmpResult[k] = self.__dict__[k].to_JSON(include_virtuals)
            else:
                tmpResult[k] = v
        return tmpResult



class UnisObject(metaclass = JSONObjectMeta):
    @logging.info("UnisObject")
    def initialize(self, src={}, runtime=None, set_attr=True, defer=False, local_only=True):
        assert isinstance(src, dict), "{t} src must be of type dict, got {t2}".format(t = type(self), t2 = type(src))
        
        for k, v in src.items():
            if set_attr:
                self.__dict__[k] = v
            else:
                self.set_virtual(k, v)
        
        self.__reserved__["_autocommit"] = False
        self.__reserved__["_runtime"] = runtime
        self.__reserved__["_collection"] = None
        self.__reserved__["_defer"] = defer
        self.__reserved__["_dirty"] = False
        self.__reserved__["_local"] = local_only
        self.__reserved__["_waiting_on"] = set()
        
    def __getattribute__(self, n):
        if n in ["get_virtual", "__dict__", "__reserved__"]:
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
        if n in self.__reserved__:
            return self.__reserved__[n]
        return self.get_virtual(n)
    
    def __setattr__(self, n, v):
        if n == "ts":
            raise AttributeError("Cannot set attribute ts, ts is a restricted property")
        if n in self.__dict__:
            if isinstance(v, list):
                v = UnisList(self._models.get(n, UnisObject), self, *v)
            if v == self.__dict__[n]:
                return
                
            oldVal = self.__dict__.get(n, None)
            if isinstance(oldVal, UnisObject):
                self._waiting_on = self._waiting_on - set(oldVal)
            if isinstance(oldVal, UnisList):
                self._waiting_on = self._waiting_on - set(list(oldVal))
            super(UnisObject, self).__setattr__(n, v)
            self._dirty = True
            
            if isinstance(v, UnisObject):
                self._waiting_on.add(v)
                
            if not self._local:
                if isinstance(v, UnisObject):
                    model = self._models.get(n, type(v))
                    if model != type(v):
                        raise ValueError("{t1}.{n} expects {t2} - got {t3}".format(t1=type(self), n=n, t2=model, t3=type(v)))
                        
                    if not v._local:
                        if v not in self._runtime:
                            self._runtime.insert(v)
                        self.update()
                else:
                    self.update()
        elif n in self.__reserved__:
            self.__reserved__[n] = v
        else:
            self.set_virtual(n, v)
            if self._autocommit:
                self.commit(n)
    
    @logging.info("UnisObject")
    def poke(self):
        if not self._local:
            payload = json.dumps({"ts": int(time.time() *  1000000)})
            self._runtime._unis.put(self.selfRef, payload)
    @logging.info("UnisObject")
    def setAutoCommit(self, n):
        self._autocommit = n
    @logging.info("UnisObject")
    def isAutoCommit(self):
        return self._autocommit
    @logging.info("UnisObject")
    def isDeferred(self):
        return self._defer
    @logging.info("UnisObject")
    def setDeferred(self, n):
        self._defer = n
    @logging.info("UnisObject")
    def setWithoutUpdate(self, n, v):
        if n in self.__dict__:
            self.__dict__[n] = v
        else:
            raise AttributeError("'{c}' object has no attribute '{n}'".format(c=type(self), n=n))
    
    
    @logging.debug("UnisObject")
    def _resolve_list(self, ls, n):
        return UnisList(self._models.get(n, UnisObject), self, *ls)
    
    @logging.debug("UnisObject")
    def _resolve_dict(self, o):
        if "$schema" in o:
            if self._runtime:
                o = self._runtime.insert(o)
                o._defer = self._defer
                self._waiting_on.add(o)
                if not self._local:
                    o.commit()
        elif "href" in o:
            if self._runtime:
                return self._runtime.find(o["href"])
            else:
                return o
        else:
            o = LocalObject(o, self)
            return o
    
    @logging.info("UnisObject")
    def to_JSON(self, include_virtuals=False):
        tmpResult = {}
        for k, v in self.__dict__.items():
            if isinstance(v, UnisObject):
                if not v._local or include_virtuals:
                    if v._schema and hasattr(v, "selfRef"):
                        tmpResult[k] = { "rel": "full", "href": v.selfRef }
                    else:
                        tmpResult[k] = v.to_JSON(include_virtuals)
            elif isinstance(v, UnisList) or isinstance(v, LocalObject):
                tmpResult[k] = v.to_JSON(include_virtuals)
            elif isinstance(v, list):
                self.__dict__[k] = self._resolve_list(v, k)
                tmpResult[k] = self.__dict__[k].to_JSON(include_virtuals)
            else:
                tmpResult[k] = v
        return tmpResult
    
    @logging.info("UnisObject")
    def commit(self, n=None):
        if not n:
            if self._local:
                if self._runtime:
                    self.__dict__["ts"] = int(time.time() * 1000000)
                    self._local = False
                    self._dirty = True
                    try:
                        self.update()
                    except:
                        self._local = True
                        raise
                else:
                    raise AttributeError("Object does not have a registered runtime")
        else:
            if n not in self.__dict__:
                if not self.has_virtual(n):
                    self.set_virtual(n, None)
                self.__dict__[n] = self.get_virtual(n)
                if not self._local:
                    self._dirty = True
                    self.update()
    
    @logging.info("UnisObject")
    def update(self, force = False):
        if (force or self._dirty) and not self._local:
            self._runtime.update(self)
    
    @logging.info("UnisObject")
    def validate(self, validate_id=False):
        if self._schema and self._resolver:
            uid = getattr(self, "id", None)
            if not validate_id:
                self.__dict__["id"] = "tmpid"
            try:
                jsonschema.validate(self.to_JSON(), self._schema, resolver = self._resolver)
            except:
                self.__dict__["id"] = uid
                raise
            self.__dict__["id"] = uid
        else:
            raise AttributeError("No schema found for object")
            
    def __str__(self):
        return json.dumps(self.to_JSON(include_virtuals=True))
    def __repr__(self):
        return "<UnisObject.{}>".format(type(self).__name__)
    
    #def __eq__(self, other):
    #    return hasattr(self, "id") and hasattr(other, "id") and self.id == other.id
        

def schemaMetaFactory(name, schema, parents = [JSONObjectMeta], loader=None):
    assert isinstance(schema, dict), "schema is not of type dict"
    class SchemaMetaClass(*parents):
        def __init__(cls, name, bases, classDict):
            super(SchemaMetaClass, cls).__init__(name=name, bases=bases, classDict=classDict)
            cls.names = [name]
            for base in bases:
                cls.names.extend(base.names)
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
                        if k in instance.__meta__:
                            instance.__dict__[k] = instance.__meta__[k]
                        else:
                            ty = "null"
                            defaults = { "null": None, "string": "", "boolean": False, "integer": 0, "number": 0, "object": {}, "array": [] }
                            if "anyOf" in v:
                                for t in v["anyOf"]:
                                    if "type" in t:
                                        ty = t["type"]
                                        break
                                        
                            instance.__dict__[k] = v.get("default", defaults.get(v.get("type", ty), None))
            return instance
            
    return SchemaMetaClass


class SchemasLoader(object):
    """JSON Schema Loader"""
    _CACHE = {}
    _CLASSES_CACHE = {}
    _LOCATIONS = {}
    
    def __init__(self, locations=None, cache={}, class_cache=None):
        assert isinstance(locations, (dict, type(None))), \
            "locations is not of type dict or None."
        assert isinstance(cache, (dict, type(None))), \
            "cache is not of type dict or None."
        assert isinstance(class_cache, (dict, type(None))), \
            "class_cache is not of type dict or None."
        self._LOCATIONS = locations or {}
        self._CACHE = cache
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

http_client = httplib2.Http()
schemaLoader = SchemasHTTPLib2(http_client, cache=CACHE)

JSONSchema = schemaLoader.get_class(JSON_SCHEMA_SCHEMA, "JSONSchema")
HyperSchema = schemaLoader.get_class(JSON_SCHEMA_HYPER, "HyperSchema")
HyperLink = schemaLoader.get_class(JSON_SCHEMA_LINKS, "HyperLink")
