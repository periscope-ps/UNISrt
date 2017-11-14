import asyncio
import itertools
import json
import jsonschema
import os
import re
import requests
import time

from lace.logging import trace

#from unis.settings import SCHEMA_CACHE_DIR
#TESTING
SCHEMA_CACHE_DIR = "/home/jemusser/smith/UNISrt/.cache"

class _attr(object):
    def __init__(self, default=None):
        self._default = default
        self.__values__ = {}
    def __get__(self, obj, cls):
        return self.__values__.get(obj, self._default)
    def __set__(self, obj, v):
        self.__values__[obj] = v
    
class _unistype(object):
    _rt_parent, _rt_runtime = _attr(), _attr()
    _rt_source, _rt_raw, _rt_reference = _attr(), _attr(), _attr()
    _rt_restricted = []
    @trace.debug("unistype")
    def __init__(self, v, rt, ref):
        self._rt_runtime, self._rt_reference, self._rt_raw = rt, ref, self
    
    @trace.debug("unistype")
    def __getattribute__(self, n):
        v = super(_unistype, self).__getattribute__(n)
        if n != '__dict__' and n in self.__dict__:
            self.__dict__[n] = self._lift(v, self._get_reference(n))
            return self.__dict__[n]._rt_raw
        return v
    
    @trace.debug("unistype")
    def __setattr__(self, n, v):
        if n in self._rt_restricted:
            raise AttributeError("Cannot change restricted attribute {}".format(n))
        if hasattr(type(self), n):
            object.__setattr__(self, n, v)
        else:
            eq = lambda a,b: (isinstance(a, _unistype) and a._rt_raw == b._rt_raw) or a == b._rt_raw
            newvalue = self._lift(v, self._get_reference(n))
            if not eq(self.__dict__[n], newvalue):
                super(_unistype, self).__setattr__(n, self._lift(v, self._get_reference(n)))
                self._update(self._get_reference(n))
    
    @trace.debug("unistype")
    def _lift(self, v, ref):
        if isinstance(v, _unistype):
            return v
        elif isinstance(v, dict):
            if '$schema' in v or 'href' in v:
                v = self._rt_runtime.insert(v) if "$schema" in v else self._rt_runtime.find(v['href'])
            else:
                v =  Local(v, self._rt_runtime, ref)
        elif isinstance(v, list):
            v = List(v, self._rt_runtime, ref)
        else:
            v = Primitive(v, self._rt_runtime, ref)
        v._rt_parent = self._rt_parent
        return v
    
    @trace.debug("unistype")
    def _update(self, ref):
        if self._rt_parent:
            self._rt_parent._update(ref)
    @trace.debug("unistype")
    def _get_reference(self, n):
        raise NotImplemented()
    @trace.info("unistype")
    def to_JSON(self):
        raise NotImplemented()
    @trace.info("unistype")
    def setRuntime(self, rt):
        self._rt_runtime = rt
        map(lambda x: x.setRuntime(rt), [v for v in self.__dict__ if isinstance(v, _unistype)])
    
class Primitive(_unistype):
    @trace.debug("Primitive")
    def __init__(self, v, rt, ref):
        super(Primitive, self).__init__(v, rt, ref)
        self._rt_raw = v
    @trace.debug("Primitive")
    def _get_reference(self, n):
        return self._rt_reference
    @trace.info("Primitive")
    def to_JSON(self):
        return self._rt_raw
    @trace.none
    def __repr__(self):
        return "<unis.Primitive {}>".format(self._rt_raw)
    
class List(_unistype):
    _rt_ls = _attr()
    @trace.debug("List")
    def __init__(self, v, rt, ref):
        super(List, self).__init__(v, rt, ref)
        self._rt_ls = list(map(lambda x: self._lift(x, self._rt_reference), v))
    @trace.debug("List")
    def __getitem__(self, i):
        return self._rt_ls[i]._rt_raw
    @trace.debug("list")
    def __setitem__(self, i, v):
        self._rt_ls[i] = self._lift(v, self._rt_reference)
        self._update(self._rt_reference)
    @trace.info("List")
    def append(self, v):
        self._rt_ls.append(self._lift(v, self._rt_reference))
        self._update(self._rt_reference)
    @trace.info("List")
    def remove(self, v):
        return self._rt_ls.remove(v)
    @trace.info("List")
    def where(self, f):
        if isinstance(pred, types.FunctionType):
            return (v for v in filter(pred, self._rt_ls))
        else:
            ops = {
                "gt": lambda b: lambda a: type(a) is type(b) and a > b,
                "ge": lambda b: lambda a: type(a) is type(b) and a >= b,
                "lt": lambda b: lambda a: type(a) is type(b) and a < b,
                "le": lambda b: lambda a: type(a) is type(b) and a <= b,
                "eq": lambda b: lambda a: type(a) is type(b) and a == b
            }
            f = [(k,ops[k](v)) for k,v in f.items()]
            def _check(x, k, op):
                return isinstance(x, _unistype) and hasattr(x, k) and op(getattr(x, k))
            return (x for x in self if all(map(lambda v: _check(x,*v), f)))
    
    @trace.debug("List")
    def _get_reference(self, n):
        return self._rt_reference
    @trace.info("List")
    def to_JSON(self):
        self._rt_ls = list(map(lambda x: self._lift(x, self._rt_reference), self._rt_ls))
        return list(map(lambda x: x.to_JSON(), self._rt_ls))
    @trace.debug("List")
    def __iter__(self):
        self._rt_ls = list(map(lambda x: self._lift(x, self._rt_reference), self._rt_ls))
        return map(lambda x: x._rt_raw, self._rt_ls)
    @trace.debug("List")
    def __len__(self):
        return len(self._rt_ls)
    @trace.debug("List")
    def __contains__(self, v):
        return v in self._rt_ls
    @trace.none
    def __repr__(self):
        return "<unis.List {}>".format(self._rt_ls.__repr__())

class Local(_unistype):
    @trace.debug("Local")
    def __init__(self, v, rt, ref):
        super(Local, self).__init__(v, rt, ref)
        self.__dict__ = v
    @trace.debug("Local")
    def _get_reference(self, n):
        return self._rt_reference
    @trace.info("Local")
    def to_JSON(self):
        return {k:self._lift(v, self._rt_reference).to_JSON() for k,v in self.__dict__.items()}
    @trace.none
    def __repr__(self):
        return "<unis.Local {}>".format(self.__dict__.__repr__())

class UnisObject(_unistype):
    _rt_remote, _rt_source, _rt_collection = _attr(), _attr(), _attr()
    _rt_restricted = ["ts", "selfRef"]
    @trace.debug("UnisObject")
    def __init__(self, v={}, rt=None, ref=None):
        super(UnisObject, self).__init__(v, rt, ref)
        self._rt_parent, self._rt_remote = self, set(v.keys()) | set(self._rt_defaults.keys())
        self.__dict__.update({**self._rt_defaults, **v, **{'$schema': self._rt_schema['id']}})
        
    @trace.debug("UnisObject")
    def __setattr__(self, n, v):
        self.__dict__['ts'] = int(time.time() * 1000000)
        super(UnisObject, self).__setattr__(n, v)
    @trace.debug("UnisObject")
    def _update(self, ref):
        if ref in self._rt_remote and self._rt_runtime:
            self._rt_runtime.update(self)
    @trace.debug("UnisObject")
    def _get_reference(self, n):
        return n
    @trace.info("UnisObject")
    def poke(self):
        if self.selfRef:
            self.__dict__['ts'] = int(time.time() * 1000000)
            payload = json.dumps({'ts': self.ts})
            asyncio.run_until_complete(self._rt_runtime._unis.put(self.selfRef, payload))
    @trace.info("UnisObject")
    def getSource(self):
        return self._rt_source
    @trace.info("UnisObject")
    def setCollection(self, v):
        self._rt_collection = v
    @trace.info("UnisObject")
    def getCollection(self):
        return self._rt_collection
    @trace.info("UnisObject")
    def commit(self, publish_to=None):
        if not self._rt_runtime:
            raise AttributeError("Resource must be attached to a Runtime instance to commit")
        if not self.selfRef and self._rt_collection:
            url = publish_to or self._rt_runtime._unis.default_source
            assert(re.compile("http[s]?://([^:/]+)(?::[0-9]{1,5})$").match(url))
            self._rt_source = url
            self.__dict__['ts'] = int(time.time() * 1000000)
            self.__dict__['selfRef'] = "{}/{}/{}".format(url, self._rt_collection, self.id)
            self._update('id')
    @trace.info("UnisObject")
    def extendSchema(self, n, v=None):
        if v:
            self.__dict__[n] = self._lift(v, n)
        if n not in self._rt_remote:
            self._rt_remote.add(n)
            self._update(n)
    @trace.info("UnisObject")
    def validate(self):
        jsonschema.validate(self.to_JSON(), self._rt_schema, resolver=self._rt_resolver)
    @trace.info("UnisObject")
    def to_JSON(self):
        result = {}
        for k,v in self.__dict__.items():
            if isinstance(v, _unistype):
                if not isinstance(v, UnisObject):
                    result[k] = v.to_JSON()
                elif v.selfRef:
                    result[k] = { "rel": "full", "href": v.selfRef }
            else:
                result[k] = v
        return result
    @trace.none
    def __repr__(self):
        return "<{}.{} {}>".format(self.__class__.__module__, self.__class__.__name__, self.__dict__.__repr__())

_CACHE = {}
if SCHEMA_CACHE_DIR:
    try:
        os.makedirs(SCHEMA_CACHE_DIR)
    except OSError as exp:
        pass
    for n in os.listdir(SCHEMA_CACHE_DIR):
        with open(SCHEMA_CACHE_DIR + "/" + n) as f:
            schema = json.load(f)
            _CACHE[schema['id']] = schema

def _schemaFactory(schema, n, tys):
    class _jsonMeta(*tys):
        def __init__(cls, name, bases, attrs):
            def _value(v):
                tys = {'null': None, 'string': "", 'boolean': False, 'number': 0, 'integer': 0, 'object': {}, 'array': []}
                return v.get('default', tys[v.get('type', 'null')])
            _props = lambda s: {k:_value(v) for k,v in s.get('properties', {}).items()}
            cls.names, cls._rt_defaults = set(), {}
            super(_jsonMeta, cls).__init__(name, bases, attrs)
            cls.names.add(n)
            cls._rt_defaults.update({k:v for k,v in _props(schema).items()})
            cls._rt_schema, cls._rt_resolver = schema, jsonschema.RefResolver(schema['id'], schema, _CACHE)
            cls.__doc__ = schema.get('description', None)
            
    return _jsonMeta

class _SchemaCache(object):
    _CLASSES = {}
    def get_class(self, schema_uri, class_name=None):
        def _make_class():
            schema = _CACHE.get(schema_uri, None) or requests.get(schema_uri).json()
            if SCHEMA_CACHE_DIR and schema_uri not in _CACHE:
                with open(SCHEMA_CACHE_DIR + "/" + schema['id'].replace('/', ''), 'w') as f:
                    json.dump(schema, f)
            parents = [self.get_class(p['$ref']) for p in schema.get('allOf', [])] or [UnisObject]
            pmeta = [type(p) for p in parents]
            meta = _schemaFactory(schema,class_name or schema['name'], pmeta)
            self._CLASSES[schema_uri] = meta(class_name or schema['name'], tuple(parents), {})
            return self._CLASSES[schema_uri]
        return self._CLASSES.get(schema_uri, None) or _make_class()
