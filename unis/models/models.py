import asyncio
import itertools
import json
import jsonschema
import os
import re
import requests
import time

from lace.logging import trace
from urllib.parse import urlparse

from unis.exceptions import UnisReferenceError
from unis.rest import UnisClient
from unis.settings import SCHEMA_CACHE_DIR
from unis.utils import async

class SkipResource(Exception):
    pass

class _attr(object):
    def __init__(self, default=None):
        self._default, self.__values__ = default, {}
    def __get__(self, obj, cls):
        return self.__values__.get(obj, self._default)
    def __set__(self, obj, v):
        self.__values__[obj] = v
    
class Context(object):
    def __init__(self, obj, runtime):
        self._obj, self._rt = obj, runtime
    def __getattribute__(self, n):
        if n not in ['_obj', '_rt'] and not hasattr(type(self), n):
            v = self._obj._getattribute(n, self._rt)
            if callable(v):
                def f(*args, **kwargs):
                    kwargs['ctx'] = self._rt
                    return v(*args, **kwargs)
                return f
            return Context(v, self._rt) if isinstance(v, _unistype) else v
        return super(Context, self).__getattribute__(n)
    def __setattr__(self, n, v):
        if n in ['_obj', '_rt']:
            return super(Context, self).__setattr__(n, v)
        return self._obj._setattr(n, v, self._rt)
    def __getitem__(self, i):
        v = self._obj._getitem(i, self._rt)
        return Context(v, self._rt) if isinstance(v, _unistype) else v
    def __setitem__(self, i, v):
        return self._obj._setitem(i, v, self._rt)
    def __iter__(self):
        for v in self._obj._iter(self._rt):
            yield Context(v, self._rt) if isinstance(v, _unistype) else v
    def __dir__(self):
        return dir(self._obj)
    def __len__(self):
        if hasattr(type(self._obj), '__len__'):
            return self._obj.__len__()
        else:
            return 1
    def __repr__(self):
        return repr(self._obj)
    def __eq__(self, other):
        if isinstance(other, Context):
            return self._obj == other._obj
        return self._obj == other
    def __hash__(self):
        return hash(self._obj)
    
    def getRuntime(self):
        return self._rt
    def setRuntime(self, runtime):
        self._rt = runtime
    def getObject(self):
        return self._obj

class _nodefault(object): pass
class _unistype(object):
    _rt_parent = _attr()
    _rt_source, _rt_raw, _rt_reference = _attr(), _attr(), _attr()
    _rt_restricted = []
    @trace.debug("unistype")
    def __init__(self, v, ref):
        self._rt_reference, self._rt_raw, = ref, self
    
    def __getattribute__(self, n):
        if not hasattr(type(self), n):
            if n in self._rt_restricted:
                return self._getattribute(n, None)
            raise NotImplementedError # This is for debugging purposes, this line should never be reached
        return super(_unistype, self).__getattribute__(n)
    @trace.debug("unistype")
    def _getattribute(self, n, ctx, default=_nodefault()):
        try:
            v = super(_unistype, self).__getattribute__(n)
        except AttributeError:
            v = default
            if isinstance(default, _nodefault):
                raise
        if n != '__dict__' and n in self.__dict__:
            self.__dict__[n] = self._lift(v, self._get_reference(n), ctx)
            return self.__dict__[n]._rt_raw
        return v
    
    @trace.debug("unistype")
    def __setattr__(self, n, v):
        if not hasattr(type(self), n):
            raise NotImplementedError # This is for debugging purposes, this line should never be reached
        return super(_unistype, self).__setattr__(n, v)
    @trace.debug("unistype")
    def _setattr(self, n, v, ctx):
        if n in self._rt_restricted:
            raise AttributeError("Cannot change restricted attribute {}".format(n))
        if hasattr(type(self), n):
            object.__setattr__(self, n, v)
        else:
            eq = lambda a,b: (isinstance(a, _unistype) and a._rt_raw == b._rt_raw) or a == b._rt_raw
            newvalue = self._lift(v, self._get_reference(n), ctx)
            if n not in self.__dict__ or not eq(self.__dict__[n], newvalue):
                super(_unistype, self).__setattr__(n, self._lift(v, self._get_reference(n), ctx))
                self._update(self._get_reference(n), ctx)
    
    @trace.debug("unistype")
    def _lift(self, v, ref, ctx):
        v = v.getObject() if isinstance(v, Context) else v
        if isinstance(v, _unistype):
            return v
        elif isinstance(v, dict):
            if '$schema' in v or 'href' in v:
                return ctx.insert(v) if "$schema" in v else ctx.find(v['href'])[0]
            v =  Local(v, ref)
        elif isinstance(v, list):
            v = List(v, ref)
        else:
            v = Primitive(v, ref)
        v._rt_parent = self._rt_parent
        return v
    
    @trace.debug("unistype")
    def _update(self, ref, ctx):
        if self._rt_parent:
            self._rt_parent._update(ref, ctx)
    @trace.debug("unistype")
    def _get_reference(self, n):
        raise NotImplemented()
    @trace.info("unistype")
    def to_JSON(self, ctx, top):
        raise NotImplemented()
    @trace.none
    def __repr__(self):
        return super().__repr__()
    
class Primitive(_unistype):
    @trace.debug("Primitive")
    def __init__(self, v, ref):
        super(Primitive, self).__init__(v, ref)
        self._rt_raw = v
    @trace.debug("Primitive")
    def _get_reference(self, n):
        return self._rt_reference
    @trace.info("Primitive")
    def to_JSON(self, ctx, top):
        return self._rt_raw
    @trace.none
    def __repr__(self):
        return "<unis.Primitive>"#.format(self._rt_raw)
    
class List(_unistype):
    _rt_ls = _attr()
    @trace.debug("List")
    def __init__(self, v, ref):
        super(List, self).__init__(v, ref)
        v = v if isinstance(v, list) else [v]
        self._rt_ls = [x.getObject() if isinstance(x, Context) else x for x in v]
    @trace.debug("List")
    def _getitem(self, i, ctx):
        return self._lift(self._rt_ls[i], self._rt_reference, ctx)._rt_raw
    @trace.debug("list")
    def _setitem(self, i, v, ctx):
        self._rt_ls[i] = self._lift(v, self._rt_reference, ctx)
        self._update(self._rt_reference, ctx)
    @trace.info("List")
    def append(self, v, ctx):
        self._rt_ls.append(self._lift(v, self._rt_reference, ctx))
        self._update(self._rt_reference, ctx)
    @trace.info("List")
    def remove(self, v, ctx):
        return self._rt_ls.remove(v)
    @trace.info("List")
    def where(self, f, ctx):
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
            return (x for x in self if all([_check(x,*v) for v in f]))
    
    @trace.debug("List")
    def _get_reference(self, n):
        return self._rt_reference
    @trace.info("List")
    def to_JSON(self, ctx, top):
        self._rt_ls = [self._lift(x, self._rt_reference, ctx) for x in self._rt_ls]
        return [r.to_JSON(ctx,top) for r in [x for x in self._rt_ls if x._getattribute('selfRef', ctx, True)]]
    @trace.debug("List")
    def _iter(self, ctx):
        self._rt_ls = [self._lift(x, self._rt_reference, ctx) for x in self._rt_ls]
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
    def __init__(self, v, ref):
        super(Local, self).__init__(v, ref)
        for k,v in v.items():
            self.__dict__[k] = v
    @trace.debug("Local")
    def _get_reference(self, n):
        return self._rt_reference
    @trace.info("Local")
    def to_JSON(self, ctx, top):
        return {k:self._lift(v, self._rt_reference, ctx).to_JSON(ctx, top) for k,v in self.__dict__.items()}
    @trace.none
    def __repr__(self):
        return "<unis.Local {}>".format(self.__dict__.__repr__())

class _metacontextcheck(type):
    def __instancecheck__(self, other):
        other = other.getObject() if hasattr(other, 'getObject') else other
        return super(_metacontextcheck, self).__instancecheck__(other)
class UnisObject(_unistype, metaclass=_metacontextcheck):
    _rt_remote, _rt_collection = _attr(), _attr()
    _rt_restricted, _rt_live = ["ts", "selfRef"], False
    _rt_callback = lambda s,x,e: x
    @trace.info("UnisObject")
    def __init__(self, v=None, ref=None):
        v = v or {}
        super(UnisObject, self).__init__(v, ref)
        self._rt_parent, self._rt_remote, self._rt_live = self, set(v.keys()) | set(self._rt_defaults.keys()), True
        self.__dict__.update({**self._rt_defaults, **v})
        if self.__dict__.get('selfRef'):
            self._rt_source = UnisClient.resolve(self._getattribute('selfRef', None))
    
    @trace.debug("UnisObject")
    def _setattr(self, n, v, ctx):
        if n in self.__dict__:
            self.__dict__['ts'] = int(time.time() * 1000000)
        super(UnisObject, self)._setattr(n, v, ctx)
    @trace.debug("UnisObject")
    def _update(self, ref, ctx):
        if ref in self._rt_remote and ctx and self._rt_live:
            self.__dict__['ts'] = int(time.time() * 1000000)
            self._rt_collection.update(self)
            ctx.update(Context(self, ctx))
    @trace.debug("UnisObject")
    def _get_reference(self, n):
        return n
    @trace.info("UnisObject")
    def touch(self, ctx):
        if self._getattribute('selfRef', ctx):
            self.__dict__['ts'] = int(time.time() * 1000000)
            cid, rid = self.getSource(), self._getattribute('id', ctx)
            async.make_async(self._rt_collection._unis.put, cid, rid, {'ts': self.ts, 'id': rid})
    @trace.info("UnisObject")
    def getSource(self, ctx=None):
        if not self._rt_source:
            raise UnisReferenceError("Attempting to resolve unregistered resource.", [])
        return self._rt_source
    @trace.info("UnisObject")
    def setCollection(self, v, ctx=None):
        self._rt_collection = v
    @trace.info("UnisObject")
    def getCollection(self, ctx=None):
        return self._rt_collection
    @trace.info("UnisObject")
    def commit(self, publish_to=None, ctx=None):
        if not ctx:
            raise AttributeError("Failed to aquire runtime context")
        if not self.selfRef and self._rt_collection:
            url = publish_to or ctx.settings['default_source']
            try:
                self._rt_source = UnisClient.resolve(url)
            except UnisReferenceError:
                ctx.addSources([{'url': url, 'default': False, 'enabled': True}])
                self._rt_source = UnisClient.resolve(url)
            self.__dict__['ts'] = int(time.time() * 1000000)
            self.__dict__['selfRef'] = "{}/{}/{}".format(url, self._rt_collection.name, self._getattribute('id', ctx))
            self._update('id', ctx)
    @trace.info("UnisObject")
    def extendSchema(self, n, v=None, ctx=None):
        if v:
            self.__dict__[n] = self._lift(v, n, ctx)
        if n not in self._rt_remote:
            self._rt_remote.add(n)
            self._update(n, ctx)
    @trace.info("UnisObject")
    def addCallback(self, fn, ctx=None):
        self._rt_callback = lambda s,x,e: fn(Context(x, ctx), e)
    @trace.debug("UnisObject")
    def _callback(self, event, ctx=None):
        self._rt_callback(self, event)
    @trace.info("UnisObject")
    def validate(self, ctx):
        jsonschema.validate(self.to_JSON(ctx), self._rt_schema, resolver=self._rt_resolver)
    @trace.info("UnisObject")
    def to_JSON(self, ctx=None, top=True):
        result = {}
        if top:
            for k,v in filter(lambda x: x[0] in self._rt_remote, self.__dict__.items()):
                try:
                    result[k] = v.to_JSON(ctx, False) if isinstance(v, _unistype) else v
                except SkipResource:
                    pass
            result['$schema'] = self._rt_schema['id']
        else:
            if self.selfRef:
                result = { "rel": "full", "href": self.selfRef }
            else:
                raise SkipResource()
        return result
    @trace.none
    def __repr__(self):
        return "<{}.{} {}>".format(self.__class__.__module__, self.__class__.__name__, self.__dict__.keys())

_CACHE = {}
if SCHEMA_CACHE_DIR:
    try:
        os.makedirs(SCHEMA_CACHE_DIR)
    except FileExistsError:
        pass
    except OSError as exp:
        raise exp
    for n in os.listdir(SCHEMA_CACHE_DIR):
        with open(SCHEMA_CACHE_DIR + "/" + n) as f:
            schema = json.load(f)
            _CACHE[schema['id']] = schema

def _schemaFactory(schema, n, tys, raw=False):
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
            setattr(cls, '$schema', schema['id'])
            cls._rt_schema, cls._rt_resolver = schema, jsonschema.RefResolver(schema['id'], schema, _CACHE)
            cls.__doc__ = schema.get('description', None)
        
        def __call__(cls, *args, **kwargs):
            instance = super(_jsonMeta, cls).__call__(*args, **kwargs)
            return Context(instance, None) if not raw else instance
        
        def __instancecheck__(self, other):
            return hasattr(other, 'names') and not self.names - other.names
    return _jsonMeta

class _SchemaCache(object):
    _CLASSES = {}
    def get_class(self, schema_uri, class_name=None, raw=False):
        key = (schema_uri, raw)
        def _make_class():
            schema = _CACHE.get(schema_uri, None) or requests.get(schema_uri).json()
            if SCHEMA_CACHE_DIR and schema_uri not in _CACHE:
                with open(SCHEMA_CACHE_DIR + "/" + schema['id'].replace('/', ''), 'w') as f:
                    json.dump(schema, f)
            parents = [self.get_class(p['$ref'], None, True) for p in schema.get('allOf', [])] or [UnisObject]
            pmeta = [type(p) for p in parents]
            meta = _schemaFactory(schema, class_name or schema['name'], pmeta, raw)
            _CACHE[schema['id']] = schema
            self._CLASSES[key] = meta(class_name or schema['name'], tuple(parents), {})
            return self._CLASSES[key]
        return self._CLASSES.get(key, None) or _make_class()
