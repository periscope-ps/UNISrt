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
    """
    Special exception used by :class:`UnisObject <unis.models.models.UnisObject>`
    to identify invalid or incomplete resources and remove them from iteration.
    """
    pass

class _attr(object):
    def __init__(self, default=None):
        self._default, self.__values__ = default, {}
    def __get__(self, obj, cls):
        return self.__values__.get(obj, self._default)
    def __set__(self, obj, v):
        self.__values__[obj] = v
    
class Context(object):
    """
    :param obj: :class:`UnisObject <unis.models.models.UnisObject>` referenced by the context.
    :param runtime: :class:`Runtime <unis.runtime.runtime.Runtime>` associated with the context.
    :type obj: :class:`UnisObject <unis.models.models.UnisObject>`
    :type runtime: :class:`Runtime <unis.runtime.runtime.Runtime>`
    
    Wrapper class linking :class:`UnisObjects <unis.models.models.UnisObject>` to
    :class:`Runtimes <unis.runtime.runtime.Runtime>`
    """
    
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
        """
        :return: :class:`Runtime <unis.runtime.runtime.Runtime>` associated with the :class:`Context <unis.models.models.Context>`.
        
        Get the :class:`Runtime <unis.runtime.runtime.Runtime>` associated with the 
        :class:`Context <unis.models.models.Context>`.
        """
        return self._rt
    def setRuntime(self, runtime):
        """
        :param runtime: Instance associated with the :class:`Context <unis.models.models.Context>`.
        :type runtime: :class:`Runtime <unis.runtime.runtime.Runtime>`
        
        Set the :class:`Runtime <unis.runtime.runtime.Runtime>` associated with the 
        :class:`Context <unis.models.models.Context>`.
        """
        self._rt = runtime
    def getObject(self):
        """
        :return: :class:`UnisObject <unis.models.models.UnisObject>` associated with the :class:`Context <unis.models.models.Context>`.
        
        Get the raw :class:`UnisObject <unis.models.models.UnisObject>` associated with the 
        :class:`Context <unis.models.models.Context>`.
        """
        return self._obj
    def setObject(self, res):
        """
        :param res: Resource instance maintained by the :class:`Context <unis.models.models.Context>`.
        :type res: :class:`UnisObject <unis.models.models.UnisObject>`
    
        set the resource associated with the :class:`Context <unis.models.models.Context>`.
        """
        self._obj = res

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
        def _eq(a, b):
            if isinstance(b, _unistype):
                return (isinstance(a, _unistype) and a._rt_raw == b._rt_raw) or a == b._rt_raw
            return (isinstance(a, _unistype) and a._rt_raw == b) or a == b
        if isinstance(v, Context):
            v = v.getObject()
        if n in self._rt_restricted:
            raise AttributeError("Cannot change restricted attribute {}".format(n))
        if hasattr(type(self), n):
            object.__setattr__(self, n, v)
        else:
            if n not in self.__dict__ or not _eq(self.__dict__[n], v):
                super(_unistype, self).__setattr__(n, v)
                self._update(self._get_reference(n), ctx)
    
    @trace.debug("unistype")
    def _lift(self, v, ref, ctx, remote=True):
        v = v.getObject() if isinstance(v, Context) else v
        if isinstance(v, _unistype):
            return v
        elif isinstance(v, dict):
            if '$schema' in v or 'href' in v:
                if remote and ctx:
                    return ctx.insert(v) if "$schema" in v else ctx.find(v['href'])[0]
                else:
                    return v
            else:
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
    @trace.info("unistype")
    def merge(self, other, ctx):
        raise NotImplemented()
    @trace.none
    def __repr__(self):
        return super().__repr__()
    
class Primitive(_unistype):
    """
    :param list v: The value of the object.
    :param ref: The owner of the object.
    :type ref: :class:`UnisObject <unis.models.models.UnisObject>`
    
    A runtime type representation of a python ``number``, ``string``, and ``boolean``.
    """
    @trace.debug("Primitive")
    def __init__(self, v, ref):
        super(Primitive, self).__init__(v, ref)
        self._rt_raw = v
    @trace.debug("Primitive")
    def _get_reference(self, n):
        return self._rt_reference
    @trace.info("Primitive")
    def to_JSON(self, ctx, top):
        """
        :param ctx: Context of the current operation.
        :param bool top: Indicates if this is the first ``to_JSON`` call in the chain.
        :returns: string or number value of the :class:`UnisObject <unis.models.models.Primitive>`
        
        Returns the raw value stored in the object.
        """
        return self._rt_raw
    def merge(self, other, ctx):
        """
        :param other: Instance to merge with the :class:`Primitive <unis.models.models.Primitive>`.
        :type other: :class:`Primitive <unis.models.models.Primitive>`
        
        Merges two :class:`Primitives <unis.models.models.Primitive>`, the passed in instance overwrites
        the calling instance where conflicts occur.
        """
        self._rt_raw = other.rt_raw
    @trace.none
    def __repr__(self):
        return "<unis.Primitive>"#.format(self._rt_raw)
    
class List(_unistype):
    """
    :param list v: The list of values in the object.
    :param ref: The owner of the object.
    :type ref: :class:`UnisObject <unis.models.models.UnisObject>`
    
    A runtime type representation of a python ``list``.
    """
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
        if isinstance(v, Context):
            v = v.getObject()
        self._rt_ls[i] = v
        self._update(self._rt_reference, ctx)
    @trace.info("List")
    def append(self, v, ctx):
        """
        :param any v: Value to be appended to the :class:`List <unis.models.models.List>`.
        :param ctx: Context of the current operation.
        
        Append a value to the :class:`List <unis.models.models.List>`.  This value may be of any type.
        """
        if isinstance(v, Context):
            v = v.getObject()
        self._rt_ls.append(v)
        self._update(self._rt_reference, ctx)
    @trace.info("List")
    def remove(self, v, ctx):
        """
        :param any v: Value to be removed from the :class:`List <unis.models.models.List>`.
        :param ctx: Context of the current operation.
        :returns: The value removed.
        
        Remove a value from the :class:`List <unis.models.models.List>`.  This must be a member of the 
        :class:`List <unis.models.models.List>`.
        """
        if isinstance(v, Context):
            v = v.getObject()
        result = self._rt_ls.remove(v)
        self._update(self._rt_reference, ctx)
        return result
    @trace.info("List")
    def where(self, f, ctx):
        """
        :param f: Predicate to filter the list.
        :param ctx: Context of the current operation.
        :returns: Generator returning filtered values.
        
        Return a subset of the :class:`List <unis.models.models.List>` including only members
        where ``f`` holds True.  This function takes the same style predicate as
        :meth:`UnisCollection.where <unis.models.lists.UnisCollection.where>`.
        """
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
            return (self._lift(x, self._rt_reference, ctx) for x in self if all([_check(x,*v) for v in f]))
    
    @trace.debug("List")
    def _get_reference(self, n):
        return self._rt_reference
    @trace.info("List")
    def to_JSON(self, ctx, top):
        """
        :param ctx: Context of the current operation.
        :param bool top: Indicates if this is the first ``to_JSON`` call in the chain.
        :returns: ``list`` containing the values in the object.
        
        Returns a plain ``list`` formated version of the object, recursively calling 
        ``to_JSON`` on all members of the :class:`List <unis.models.models.List>` where
        applicable.
        """
        res = []
        for i, item in enumerate(self._rt_ls):
            if isinstance(item, (list, dict)):
                self._rt_ls[i] = self._lift(item, self._get_reference(i), ctx, False)
        for item in self._rt_ls:
            try:
                res.append(item.to_JSON(ctx, top) if isinstance(item, _unistype) else item)
            except SkipResource:
                pass
        return res
    @trace.info("List")
    def merge(self, other, ctx):
        """
        :param other: Instance to merge with the :class:`List <unis.models.models.List>`.
        :type other: :class:`List <unis.models.models.List>`
        
        Merges two :class:`Lists <unis.models.models.List>`, the passed in instance overwrites
        the calling instance where conflicts occur.
        """
        for v in other:
            self.append(v)
    
    @trace.debug("List")
    def _iter(self, ctx):
        self._rt_ls = [self._lift(x, self._rt_reference, ctx) for x in self._rt_ls]
        return iter([x._rt_raw for x in self._rt_ls])
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
    """
    :param dict v: The attribute names and values to be included in the object.
    :param ref: The owner of the object.
    :type ref: :class:`UnisObject <unis.models.models.UnisObject>`
    
    A runtime type representation of a python ``dict``.
    """
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
        """
        :param ctx: Context of the current operation.
        :param bool top: Indicates if this is the first ``to_JSON`` call in the chain.
        :returns: ``dict`` containing the raw respresentation of all child members.
        
        Returns a plain ``dict`` formated version of the object, recursively calling 
        ``to_JSON`` on all members of the :class:`Local <unis.models.models.Local>` where
        applicable.
        """
        res = {}
        for k,v in self.__dict__.items():
            if isinstance(v, (list, dict)):
                self.__dict__[k] = v = self._lift(v, self._get_reference(k), ctx, False)
            try:
                res[k] = v.to_JSON(ctx, top) if isinstance(v, _unistype) else v
            except SkipResource:
                pass
        return res
    @trace.info("Local")
    def merge(self, other, ctx):
        """
        :param other: Instance to merge with the :class:`Local <unis.models.models.Local>`.
        :type other: :class:`Local <unis.models.models.Local>`
        
        Merges two :class:`Locals <unis.models.models.Local>`, the passed in instance overwrites
        the calling instance where conflicts occur.
        """
        for k,v in other.__dict__.items():
            self.__dict__[k] = v
    @trace.none
    def __repr__(self):
        return "<unis.Local {}>".format(self.__dict__.__repr__())

class _metacontextcheck(type):
    def __instancecheck__(self, other):
        other = other.getObject() if hasattr(other, 'getObject') else other
        return super(_metacontextcheck, self).__instancecheck__(other)
class UnisObject(_unistype, metaclass=_metacontextcheck):
    """
    :param dict v: (optional) A ``dict`` containing all attribute names and values in the resource.
    :param ref: (optional) A reference to the owner of the object.
    :type ref: :class:`UnisObject <unis.models.models.UnisObject>`
    
    The base class for all runtime resources.  This is used to maintain a record
    of the construction and state of the internal attributes.
    
    All attributes listed in ``v`` are considered to be *remote* attributes and are included in
    the data store on update.
    """
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
        super(UnisObject, self)._setattr(n, v, ctx)
    @trace.debug("UnisObject")
    def _update(self, ref, ctx):
        if ref in self._rt_remote and ctx and self._rt_live:
            self._rt_collection.update(self)
            ctx._update(Context(self, ctx))
    @trace.debug("UnisObject")
    def _get_reference(self, n):
        return n

    @trace.info("UnisObject")
    def _delete(self, ctx):
        self.__dict__['selfRef'] = ''
        self._rt_source = None
    @trace.info("UnisObject")
    def touch(self, ctx):
        """
        :param ctx: Context of the current operation.
        
        Force the :class:`UnisObject <unis.models.models.UnisObject>` to modify its timestamp in the data store.
        This affects a "keep alive" signal to the data store.
        """
        if self._getattribute('selfRef', ctx):
            cid, rid = self.getSource(), self._getattribute('id', ctx)
            async.make_async(self._rt_collection._unis.put, cid, rid, {'id': rid})
    @trace.info("UnisObject")
    def getSource(self, ctx=None):
        """
        :param ctx: Context of the current operation.
        :returns: :class:`CID <unis.rest.unis_client.CID>` for this object.
        
        Returns the ID of the data store in which the resource is stored.
        """
        if not self._rt_source:
            raise UnisReferenceError("Attempting to resolve unregistered resource.", [])
        return self._rt_source
    @trace.info("UnisObject")
    def setCollection(self, v, ctx=None):
        """
        :param v: Collection to set.
        :param ctx: Context of the current operation.
        
        Sets the :class:`UnisCollection <unis.models.lists.UnisCollection>` that the object
        belongs to.
        """
        self._rt_collection = v
    @trace.info("UnisObject")
    def getCollection(self, ctx=None):
        """
        :param ctx: Context of the current operation.
        :returns: :class:`UnisCollection <unis.models.lists.UnisCollection>` that the object belongs to.
        
        Returns the collection that the object belongs to.
        """
        return self._rt_collection
    @trace.info("UnisObject")
    def commit(self, publish_to=None, ctx=None):
        """
        :param str publish_to: Data store in which to insert the resource.
        :param ctx: Context of the current operation.
        
        ``commit`` stages the object to be inserted into a remote data store.
        If the object is already a member of an instance, this function does
        nothing.  Like any modification to the remote data stores, this function's
        behavior is dependent of the state of the :class:`Runtime's <unis.runtime.runtime.Runtime>`
        ``defer_update`` setting.
        
        If the calling :class:`Runtime <unis.runtime.runtime.Runtime>` is in ``deferred_mode``, the
        :class:`UnisObject <unis.models.models.UnisObject>` will only be staged and sent to the remote
        instance only after a call the :meth:`ObjectLayer.flush <unis.runtime.oal.ObjectLayer>`.  In
        ``immediate_mode`` the resource will be dispatched immediately.
        """
        if not ctx:
            raise AttributeError("Failed to aquire runtime context")
        if not self.selfRef and self._rt_collection:
            url = publish_to or ctx.settings['default_source']
            try:
                self._rt_source = UnisClient.resolve(url)
            except UnisReferenceError:
                ctx.addSources([{'url': url, 'default': False, 'enabled': True}])
                self._rt_source = UnisClient.resolve(url)
            self.__dict__['selfRef'] = "{}/{}/{}".format(url, self._rt_collection.name, self._getattribute('id', ctx))
            self._update('id', ctx)
    @trace.info("UnisObject")
    def extendSchema(self, n, v=None, ctx=None):
        """
        :param str n: Name of the attribute to add.
        :param any v: (optional) Value to set to the new attribute.
        :param ctx: Context of the current operation.
        
        Add a new attribute to the internal schema.  Adding an attribute to the schema will cause the corresponding
        :class:`UnisObject <unis.models.models.UnisObject>` to be marked as pending an update and will include the
        new attribute in the remote data store.
        """
        if v:
            self.__dict__[n] = v
        if n not in self._rt_remote:
            self._rt_remote.add(n)
            self._update(n, ctx)
    @trace.info("UnisObject")
    def addCallback(self, fn, ctx=None):
        """
        :param callable fn: Callback function to attach to the :class:`UnisObject <unis.models.models.UnisObject>`.
        :param ctx: Context of the current operation.
        
        Add a callback to the individual :class:`UnisObject <unis.models.models.UnisObject>`.  This callback
        functions as described in :meth:`UnisCollection.addCallback <unis.models.lists.UnisCollection.addCallback>`.
        """
        self._rt_callback = lambda s,x,e: fn(Context(x, ctx), e)
    @trace.debug("UnisObject")
    def _callback(self, event, ctx=None):
        self._rt_callback(self, event)
    @trace.info("UnisObject")
    def validate(self, ctx):
        """
        :param ctx: Context of the current operation.
        :raises ValidationError: If :class:`UnisObject <unis.models.models.UnisObject>` fails to validate.
        
        Validate the :class:`UnisObject <unis.models.models.UnisObject>` against the JSON Schema used
        to construct its type.
        """
        jsonschema.validate(self.to_JSON(ctx), self._rt_schema, resolver=self._rt_resolver)
    @trace.info("UnisObject")
    def to_JSON(self, ctx=None, top=True):
        """
        :param ctx: Context of the current operation.
        :param bool top: Indicates if this is the first ``to_JSON`` call in the chain.
        :returns: ``dict`` containing the raw respresentation of all child members.
        
        Returns a plain ``dict`` formated version of the object, recursively calling 
        **to_JSON** on all members of the :class:`Local <unis.models.models.Local>` where
        applicable.
        
        If ``top`` is ``False``, **to_JSON** instead returns a dict containing the reference to
        the resource's remote data store entry.
        """
        result = {}
        if top:
            for k,v in filter(lambda x: x[0] in self._rt_remote, self.__dict__.items()):
                if isinstance(v, (list, dict)):
                    self.__dict__[k] = v = self._lift(v, self._get_reference(k), ctx, False)
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

    @trace.info("UnisObject")
    def merge(self, other, ctx):
        """
        :param other: Instance to merge with the :class:`UnisObject <unis.models.models.UnisObject>`.
        :type other: :class:`UnisObject <unis.models.models.UnisObject>`
        
        Merges two :class:`UnisObject <unis.models.models.UnisObject>`, the instance with the highest
        timestamp takes priority.
        """
        a, b = self, other if self.ts < other.ts else other, self
        for k,v in b.__dict__.items():
            if k in a.__dict__:
                if isinstance(b.__dict__[k], (list, dict)):
                    a.__dict__[k] = a._lift(v, a._get_reference(k), ctx, False)
                if isinstance(a.__dict__[k], _unistype):
                    a.__dict__[k].merge(v, ctx)
                else:
                    a.__dict__[k] = v
            else:
                a.__dict__[k] = v
        for n in b._rt_remote:
            a._rt_remote.add(n)
    
    @trace.info("UnisObject")
    def clone(self, ctx):
        """
        :param ctx: Context of the current operation.
        
        Create an exact clone of the object, clearing the ``selfRef`` and ``id``
        attributes.
        
        .. warning:: Any references made in the object will retain their old value.  This function is insufficient to make a complete clone of a heirarchy of resources.
        """
        d = self.to_JSON(ctx)
        d.update(**{'selfRef': '', 'id': ''})
        model = type(self)
        return Context(model(d), ctx)
    
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
