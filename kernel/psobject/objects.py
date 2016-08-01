import datetime
import jsonschema
import time
import re

from kernel.psobject import schemas
        
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
            return cls.__meta__.__get__(self).get(n, None)
        def set_virt(self, n, v):
            cls.__meta__.__get__(self)[n] = v
            return v
        
        cls.__meta__ = JSONObjectMeta.AttrDict()
        cls.get_virtual = get_virt
        cls.set_virtual = set_virt
    
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
    
    def __init__(self, model, *args):
        for arg in args:
            assert isinstance(arg, model)
        self.model = model
        self.items = args
    
    def append(self, item):
        assert isinstance(item, self.model)
        self.items.append(item)

    def to_JSON(self):
        tmpResult = []
        for item in self.items:
            if isinstance(item, UnisList):
                tmpResult.append(item.to_JSON())
            elif isinstance(item, UnisObject):
                if not item._virtual:
                    tmpResult.append(item.to_JSON())
            else:
                tmpResult.append(item)
    
    def __getitem__(self, key):
        return self.items[key]
    def __setitem__(self, key, v):
        assert isinstance(v, self.model)
    def __delitem__(self, key):
        self.items.__delitem(key)
    def __iter__(self):
        return iter(self)
        

class UnisObject(metaclass = JSONObjectMeta):
    def initialize(self, src = {}, runtime = None, defer = False, local_only = True):
        self._runtime = runtime
        self._defer = defer
        self._dirty = self._pending = False
        self._local = local_only
        self._schema = None
        self._models = {}
        
        assert isinstance(src, (dict, type(None))), "Data must be of type dict or NoneType"
        for k, v in src.items():
            self.__dict__[k] = v
        
    def __getattribute__(self, n):
        if n in ["get_virtual"]:
            return super(UnisObject, self).__getattribute__(n)
        
        v = self.get_virtual(n)
        if isinstance(v, type(None)):
            v = super(UnisObject, self).__getattribute__(n)
            if n not in ["__dict__", "__meta__"]:
                if isinstance(v, (list, dict)):
                    if isinstance(v, list):
                        v = self._resolve_list(v, n)
                    if isinstance(v, dict):
                        v = self._resolve_dict(v)
                    super(UnisObject, self).__setattr__(n, v)
        return v
    
    def __setattr__(self, n, v):
        # If the attribute is a UnisObject - i.e. it refers to a descrete resource in UNIS
        # create a dictionary that conforms to the json 'link' schema.
        self.set_virtual("_lasttouched", datetime.datetime.utcnow())
        
        ### TODO ###
        # Ensure that only references are placed in link locations
        ############
        if n in self.__dict__:
            if isinstance(v, list):
                v = UnisList(self.get_virtual("_models").get(n, UnisObject), *v)
            super(UnisObject, self).__setattr__(n, v)
            self.set_virtual("_dirty", True)
            if not self.get_virtual("_local") and (not isinstance(v, UnisObject) or not v._virtual):
                self.update()
        else:
            self.set_virtual(n, v)
        
    def _resolve_list(self, ls, n):
        tmpResult = UnisList(self._models.get(n, UnisObject))
        for i in ls:
            if isinstance(i, dict):
                tmpResult.append(self._resolve_dict(i))
            elif isinstance(i, list):
                tmpResult.append(self._resolve_list(i, n))
            else:
                tmpResult.append(i)
        return tmpResult
        
    def _resolve_dict(self, o):
        if "href" in o:
            return self._runtime.find(o["href"])
        else:
            # Convert object and cache
            return UnisObject(o, self._runtime)
    
    def to_JSON(self):
        tmpResult = {}
        for k, v in self.__dict__.items():
            if isinstance(v, UnisObject):
                if not v._virtual:
                    if v._schema:
                        tmpResult[k] = { "rel": "full", "href": v.selfRef }
                    else:
                        tmpResult[k] = v.to_JSON()
            elif isinstance(v, UnisList):
                tmpResult[k] = v.to_JSON()
            else:
                tmpResult[k] = v
        return tmpResult
    
    def commit(self, n=None):
        if not n:
            self._local = False
        elif n not in self.__dict__:
            self.__dict__[n] = self.get_virtual(n)
            if not self._local:
                self._dirty = True
                self.update()
    
    def update(self):
        if self._dirty and not self._pending and not self._defer:
            self._pending = True
            self.ts = int(time.time()) * 1000000
            self._runtime.update(self)
    def flush(self):
        if self._dirty and not self._pending:
            self.ts = int(time.time()) * 1000000
            self._pending = True
            self._runtime.update(self)
    
    def validate(self):
        if self._schema:
            jsonschema.validate(self.to_JSON(), self._schema, resolver = self._resolver)
        else:
            raise AttributeError("No schema found for object")



def schemaMetaFactory(name, schema, parents = [JSONObjectMeta]):
    assert isinstance(schema, dict), "schema is not of type dict"
    class SchemaMetaClass(*parents):
        def __init__(cls, name, bases, classDict):
            super(SchemaMetaClass, cls).__init__(name=name, bases=bases, classDict=classDict)
            cls._schema = schema
            cls._resolver = jsonschema.RefResolver(schema["id"], schema, schemas.CACHE)
            cls.__doc__ = schema.get("description", None)
        
        def __call__(meta, *args, **kwargs):
            instance = super(SchemaMetaClass, meta).__call__(*args, **kwargs)
            if schema.get("type", "object") == "object":
                for k, v in schema.get("properties", {}).items():
                    instance.__dict__[k] = v.get("default", None)
        
            return instance
            
    return SchemaMetaClass

__classes__ = {}
def get_class(schema_uri, class_name = None):
    if schema_uri in __classes__:
        return __classes__[schema_uri]
    schema = schemas.get(schema_uri)
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
                cls = get_class(parent["$ref"])
                parents.append(cls)
                parent_metas.append(type(cls))
            else:
                raise AttributeError("allof must be remote references")
    else:
        parents = [UnisObject]
        parent_metas = [JSONObjectMeta]
    
    if not class_name:
        raise AttributeError("class_name is not defined by the provided schema or the client")
    
    meta = schemaMetaFactory("{n}Meta".format(n = class_name), schema, parent_metas)
    __classes__[schema_uri] = meta(class_name, tuple(parents), {})
    return __classes__[schema_uri]
