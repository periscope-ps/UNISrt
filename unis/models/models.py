import os, types as ptypes, uuid
import json, jsonschema, requests, logging, unis

from lace.logging import trace, getLogger
from collections import defaultdict

import unis

from unis import events
from unis.settings import ID_FIELD, TS_FIELD
from unis.utils.lists import dict_filter

class queryable(type):
    @property
    def Q(cls): return unis.Q().istype(cls)

@trace("unis.models")
class _unisclass(object, metaclass=queryable):
    _rt_restricted = [ID_FIELD, 'selfRef', TS_FIELD]
    __slots__ = ('_rt_parent', '_rt_raw', '_rt_ref', '_rt_locks')
    def __init__(self, v, r, p):
        setr = object.__setattr__
        setr(self, '_rt_raw', v)
        setr(self, '_rt_locks', [])
        setr(self, '_rt_ref', r)
        setr(self, '_rt_parent', p)
    def __getattribute__(self, n):
        if hasattr(type(self), n): return super().__getattribute__(n)
        v = super().__getattribute__(n)
        return v._rt_raw if hasattr(type(v), '_rt_raw') else v

    def __setattr__(self, n, v):
        super().__setattr__(n, v)
        if not hasattr(type(self), n):
            self._rt_locks.append(n)
            self._update(self._get_ref(n))

    def _lift(self, v, ref, remote=True):
        if isinstance(v, _unisclass): return v
        elif isinstance(v, dict):
            if 'href' in v:
                return unis.runtime.find(v['href'], remote)[0]
            return Local(v, ref, self._rt_parent)
        elif isinstance(v, list):
            return List(v, ref, self._rt_parent)
        else:
            return Primitive(v, ref, self._rt_parent)

    def _update(self, ref):
        try: self._rt_parent._update(ref)
        except AttributeError: return
    def _get_ref(self, n): return self._rt_ref
    def is_locked(self):
        return self._rt_locks
    def unlock(self):
        self._rt_locks = []
    def load_all_reference(self): raise NotImplemented()
    def _merge(self, other): raise NotImplemented()
    def _clone(self): return self._rt_raw

@trace("unis.models")
class Primitive(_unisclass):
    """
    :param any v: The value of the object.
    :param str r: The property referencing the object.
    :param p: The parent containing the object
    :type p: :class:`UnisObject <unis.models.models.UnisObject>`
    
    A runtime type representation of python ``number``, ``string`, or ``boolean`` objects.
    """
    __slots__ = tuple()
    def _merge(self, b):
        """
        :param b: Instance to merge with the :class:`Primitive <unis.models.models.Primitive>`
        :type b: :class:`Primitive <unis.models.models.Primitive>`
        
        Merges two :class:`Primitives <unis.models.models.Primitive>`, the passed in instance
        overwrites the calling instance where conflicts occur.
        """
        self._rt_raw = b._rt_raw if isinstance(b, _unisclass) else b
    def load_all_references(self): pass
    def __repr__(self): return f"<unis.Primitive {hex(id(self))}>"
    def __str__(self): return self._rt_raw.__str__()
    def __eq__(a, b): return (isinstance(b, _unisclass) and a._rt_raw) or a._rt_raw == b

@trace("unis.models")
class List(_unisclass):
    """
    :param list v: The list of values in the object.
    :param str r: The property referencing the list.
    :param p: The parent containing the list.
    :type p: :class:`UnisObject <unis.models.models.UnisObject>`
    
    A runtime type representation of python ``list`` objects.
    """
    __slots__ = ('_rt_ls',)
    def __init__(self, v, r, p):
        self._rt_ls = v if isinstance(v, list) else [v]
        self._rt_raw, self._rt_locks, self._rt_ref, self._rt_parent = self, [], r, p
    def __setitem__(self, i, v):
        self._rt_ls[i] = v
        self._rt_locks.append(i)
        self._update(self._rt_ref)
    def __getitem__(self, i):
        return self._lift(self._rt_ls[i], self._rt_ref)._rt_raw

    def append(self, v):
        """
        :param any v: Value to be appended to the :class:`List <unis.models.models.List>`.
        
        Append a value to the :class:`List <unis.models.models.List>`.
        """
        self._rt_locks.append(len(self._rt_ls))
        self._rt_ls.append(v)
        self._update(self._rt_ref)

    def remove(self, v):
        """
        :param any v: Value to be removed from the :class:`List <unis.models.models.List>`.
        
        Remove a value from the :class:`List <unis.models.models.List>`.  This value must be a
        member of the list.
        """
        ls = self._rt_ls
        self._rt_locks = list(range(len(ls)))
        ls.remove(v)
        self._update(self._rt_ref)

    def where(self, f):
        """
        :param f: Predicate to filter the list.
        :returns: List containing entries of the list matching the predicate.

        Return a subset of the :class:`List <unis.models.models.List>` including only members
        where ``f`` holds True.  This function takes the same style predicate as
        :meth:`UnisCollection.where <unis.containers.UnisCollection.where>`.
        """
        if not isinstance(f, (ptypes.FunctionType, dict)):
            msg = f"Invalid type `{type(f)}` passed to .where"
            logging.getLogger("unis.models").error(msg)
            raise ValueError(msg)
        f = f if isinstance(f, ptypes.FunctionType) else dict_filter(f)
        return list(filter(f, [self._lift(x, self._get_ref(i))._rt_raw for i,x in enumerate(self._rt_ls)]))

    def _merge(self, b):
        """
        :param b: Instance to merge with the :class:`List <unis.models.models.List>`.
        :type b: :class:`List <unis.models.models.List>`
        
        Merges two :class:`Lists <unis.models.models.List>`, values in the merging list
        in the merged list are appended.
        """
        if not self.is_locked():
            self._rt_ls = b

    def _clone(self): return [(v._clone() if isinstance(v, _unisclass) else v) for v in self._rt_ls]
    def is_locked(self): return bool(self._rt_locks) or any([x.is_locked() for x in self if hasattr(x, 'is_locked')])
    def load_all_references(self):
        """
        Forces the object and all sub-objects to reify themselves into unis types.
        """
        for i,v in enumerate(self._rt_ls):
            self._rt_ls[i] = v = self._lift(v, self._get_ref(i))
            v.load_all_references()
    def unlock(self):
        """
        Unlocks fields that have been modified by the client for remote modification.
        This function is called automatically by the runtime when a resource is 
        stored to the data store.
        """
        self._rt_locks = []
        [x.unlock() for x in self._rt_ls if hasattr(x, 'unlock')]
    def __iter__(self):
        return iter([self._lift(x, self._rt_ref)._rt_raw for x in self._rt_ls])
    def __len__(self): return len(self._rt_ls)
    def __contains__(self, v): return v in self._rt_ls
    def __repr__(self): return f"<unis.List {hex(id(self))}>"
    def __str__(self): return self._rt_ls.__str__()
    def __eq__(a, b): return hasattr(b, '__len__') and len(b) == len(a._rt_ls) and \
        all([a._rt_ls[i] == b[i] for i in range(len(b))])

@trace("unis.models")
class Local(_unisclass):
    """
    :param any v: The Value of the object.
    :param str r: The property referencing the object.
    :param p: The parent containing the object
    :type p: :class:`UnisObject <unis.models.models.UnisObject>`
    
    A runtime type representation of python ``dict`` object.
    """

    _rt_defaults = {}
    __slots__ = ('__dict__',)
    def __init__(self, v, r, p):
        super().__init__(self, r, p)
        object.__getattribute__(self, '__dict__').update(**(v or {}))

    def __getattribute__(self, n):
        if hasattr(type(self), n): return object.__getattribute__(self, n)
        try: v = super().__getattribute__(n)
        except AttributeError as e:
            if n in self._rt_defaults: v = self._rt_defaults[n]
            else: raise e
        if n != '__dict__' and (n in self.__dict__ or n in self._rt_defaults):
            self.__dict__[n] = v = self._lift(v, self._get_ref(n))
        return v._rt_raw if hasattr(type(v), '_rt_raw') else v

    def __setattr__(self, n, v):
        if n in _unisclass._rt_restricted:
            raise AttributeError(f"Cannot change restricted attribute {n}")
        d = object.__getattribute__(self, '__dict__')
        if n not in d or v != d[n]:
            super().__setattr__(n, v)

    def items(self, onlyremote=False):
        """
        :returns: key,value tuple of object properties.

        Returns the key and value pairs for each property in the resource.
        """
        return [(k, self._lift(v, self._get_ref(k))) for k,v in self.__dict__.items() if v is not None]

    def _merge(self, b):
        """
        :param other: Instance to merge with the :class:`Local <unis.models.models.Local>`.
        :type other: :class:`Local <unis.models.models.Local>'

        Merges two :class:`Locals <unis.models.models.Local>`, the passed in instance overwrites
        the calling instance where conflicts occur.
        """
        for k,v in b.__dict__.items():
            if k not in self._rt_locks:
                if k not in self.__dict__ or not hasattr(self.__dict__[k], '_merge'):
                    self.__dict__[k] = v
                else:
                    self.__dict__[k]._merge(self._lift(v, self._get_ref(k)))
    def unlock(self):
        self._rt_locks = []
        [x.unlock() for x in self.__dict__.values() if hasattr(x, 'unlock')]
    def is_locked(self):
        return self._rt_locks or any([hasattr(x, 'is_locked') and x.is_locked() for x in self.__dict__.values()])
    def load_all_references(self):
        for k,v in self.__dict__.items():
            self.__dict__[k] = v = self._lift(v, self._get_ref(k))
            v.load_all_references()
    def _clone(self): return {k:(v._clone() if isinstance(v, _unisclass) else v) for k,v in self.__dict__.items()}
    def __repr__(self):
        return f"<unis.Local {hex(id(self))}>"
    def __str__(self):
        return str(dict(self.items()))
    def __eq__(a, b): return hasattr(b, 'items') and \
        set([x for x,_ in a.items()]) == set([x for x,_ in b.items()]) and \
        all([getattr(a, k) == v for k,v in b.items()])
    def __iter__(self):
        for k,v in self.items(onlyremote=True): yield k, _flatten(v)
@trace("unis.models")
class UnisObject(Local):
    """
    :param dict v: (optional) A ``dict`` containing all attribute names and values in the resource.

    The base class for all runtime resources.  This is used to maintain a record
    of the construction and state of the internal attributes.

    All attributes listed in ``v`` are considered to be *remote* attributes and are included in
    the data store on update.
    """
    __slots__ = ('_rt_remote', '_rt_callbacks', '_rt_container')
    def __init__(self, v=None):
        super().__init__(v, None, self)
        setr = object.__setattr__
        setr(self, '_rt_remote', list({**(v or {}), **type(self)._rt_defaults}))
        setr(self, '_rt_callbacks', {})
        if not getattr(self, ID_FIELD):
            setr(self, ID_FIELD, str(uuid.uuid4()))
        unis.containers.nullcontainer.insert(self)
        events.manager.publish(self, events.types.CREATE)

    def _update(self, ref):
        c = getattr(self, '_rt_container', None)
        if ref in self._rt_remote and c:
            c.update(self)

    def _get_ref(self, n): return n
    def _delete(self):
        self.__dict__['selfRef'] = ''

    def extend_schema(self, n, v=None):
        """
        :param str n: Name of the attribuet to add.
        :param any v: (optional) Value to set to the new attribute.
        
        Add a new attribute to the internal schema.  Adding an attribute to the schema will cause the corresponding
        :class:`UnisObject <unis.models.models.UnisObject>` to be marked as pending an update and will include the
        new attribute in the remote data store.
        """
        if v: setattr(self, n, v)
        if n not in self._rt_remote:
            self._rt_remote.append(n)
            self._update(n)

    def add_callback(self, fn):
        """
        :param callable fn: Callback function to attach to the :class:`UnisObject <unis.models.models.UnisObject>`.
        
        Add a callback to the :class:`UnisObject <unis.model.model.UnisObject>` object.  This callback functions
        as described in :meth:`UnisCollection.add_callback <unis.containers.UnisCollection.add_callback>`.
        """
        self._rt_callbacks.append(fn)

    def _callback(self, event):
        [cb(self, event) for cb in self._rt_callbacks]

    def items(self, onlyremote=False):
        """
        :returns: list of key,value tuples
        
        Returns the key,value pairs for each property stored in the resource.
        """
        result = {**self._rt_defaults, **self.__dict__}
        for k,v in result.items():
            if not onlyremote or k in self._rt_remote:
                yield (k, self._lift(v, self._get_ref(k), False))

    def set_container(self, v, c):
        """
        :param v: Container holding this resource.
        :param c: Name of the collection holding this resource.
        :type v: `Container <unis.containers.Container>`
        :type c: str
        
        Sets the container currently storing this resource.
        """
        object.__setattr__(self, '_rt_container', (v, c))

    def get_container(self):
        """
        :returns: `Container <unis.containers.Container>`

        Returns the container currently holding this resource.
        """
        return self._rt_container[0] if self._rt_container else None

    def get_collection(self):
        """
        :returns: str
        
        Returns the collection name holding this resource.
        """
        return self._rt_container[1] if self._rt_container else None

    def clone(self):
        """
        Create an exact clone of the object, clearing the ``selfRef`` and ``id`` attributes.
        
        .. warning:: Any references made in the object will retain their old value.  This function is
        insufficient to make a complete clone of a heirarchy of resources.
        """
        d = {k:(v._clone() if isinstance(v, _unisclass) else v) for k,v in self.__dict__.items()}
        return type(self)({**d, **{'selfRef': '', ID_FIELD: ''}})
    def is_locked(self):
        """
        Indicates the locked status of an object.  Locked objects take priority on merge.  Top level
        resources are always unlocked, but may contain individual properties that are locked.
        """
        return False
    def merge(self, b):
        """
        :param other: Instance to merge with the :class:`UnisObject <unis.models.models.UnisObject>`.
        :type other: :class:`UnisObject <unis.models.models.UnisObject>'

        Merges two :class:`UnisObjects <unis.models.models.UnisObject>`, the passed in instance overwrites
        the calling instance where conflicts occur unless the field has been written to but not pushed.
        """
        self.__class__ = b.__class__
        return super()._merge(b)
    def get_measurement(self, ty):
        """
        :param ty: `eventType` name associated with the measurement
        :type ty: str
        :returns: :class:`DataCollection <unis.measurements.data.DataCollection>`

        Returns a :class:`DataCollection <unis.measurements.data.DataCollection>` associated with a
        measurement type `ty` for the resource.
        """
        return self._rt_measurements[ty]
    def add_measurement(self, ty, md=None):
        """
        :param ty: `eventType` name associated with the measurement
        :param md: :class:`Metadata <unis.models.models.UnisObject>` describing the measurement
        :type ty: str
        :type md: :class:`Metadata <unis.models.models.UnisObject>`
        :returns: :class:`DataCollection <unis.measurement.data.DataCollection>`

        Adds a :class:`DataCollection <unis.measurement.data.DataCollection>` to the resource
        with the provided `eventType`.  If no measurement of that type exists in the remote,
        one will be created.
        """
        if not md:
            Metadata = _SchemaCache().get_class(settings.SCHEMAS['Metadata'])
            md = Metadata({'subject': self, 'eventType': ty})
            self._rt_container.insert(md)
            dc = DataCollection(md, registered=False)
        else:
            dc = DataCollection(md)
        self._rt_measurements[ty] = dc
        return dc
    def touch_remote(self):
        """
        Updates the timestamp of any remote without modifying other fields.
        
        .. note:: This operation is storage efficient compared to property writes but will be
        pushed immediately on call, regardless of the state of the `defer_update` setting.
        """
        uid = self._dict__[ID_FIELD]
        self._rt_container._client.put(uid, {ID_FIELD: uid})
    def validate(self):
        """
        :param ctx: Context of the current operation.
        :raises ValidationError: If :class:`UnisObject <unis.models.models.UnisObject>` fails to validate.
        
        Validate the :class:`UnisObject <unis.models.models.UnisObject>` against the JSON Schema used
        to construct its type.
        """
        pass
        #resolver = jsonschema.RefResolver(self._rt_schema[ID_FIELD], self._rt_schema, _CACHE)
        #jsonschema.validate(, self._rt_schema, resolver=resolver)
    def _clone(self):
        return self
    def __repr__(self):
        return f"<{type(self).__module__}.{type(self).__name__} {hex(id(self))}>"

_CACHE = {}
d = {'null': None, 'string': "", 'boolean': False,
     'number': 0, 'integer': 0, 'object': {}, 'array': []}

_log = getLogger('unis.models')
class _add_defaults(object):
    def __init__(self, s, n, p): self.schema, self.n, self.parents = s, n, p
    def __call__(self, cls):
        props, tys = self.get_props()
        props.update(**{k:v for p in self.parents for k,v in getattr(p, '_rt_defaults', {}).items()})
        tys.update(**{k:v for p in self.parents for k,v in getattr(p, '_rt_types', {}).items()})
        cls._rt_defaults, cls._rt_types, cls._rt_schema = props, tys, self.schema
        cls._rt_resolver = jsonschema.RefResolver(self.schema['$id'], self.schema, _CACHE)
        cls.__name__ = cls.__qualname__ = self.n
        setattr(cls, ':type', self.schema['$id'])
        setattr(cls, '__doc__', self.schema.get('description', None))
        cls._rt_defaults[':type'] = self.schema['$id']
        return cls

    def get_props(self):
        props, tys = {}, {}
        _log.debug(f"--Loading Schema '{self.schema['$id']}'")
        _log.debug(self.schema)
        try:
            for k,v in self.schema['oneOf'][0]['properties'].items():
                try:
                    props[k] = v.get('default', d[v['type']])
                    tys[k] = v
                except KeyError: pass
        except (KeyError, IndexError): pass
        try:
            for k,v in self.schema['properties'].items():
                try:
                    props[k] = v.get('default', d[v['type']])
                    tys[k] = v
                except KeyError: pass
        except (KeyError, IndexError): pass
        return props, tys

def _schemaFactory(schema, n, parents):
    @_add_defaults(schema, n, parents)
    class _abstract(*parents):
        __slots__ = tuple()
    return _abstract

class _SchemaCache(object):
    _CLASSES = {}
    def get_class(self, schema_url, class_name=None, isfile=False):
        def _make_class():
            schema = self.cache(schema_url, isfile)
            parents = [self.get_class(p['$ref'], None) for p in schema.get('allOf', [])] or [UnisObject]
            self._CLASSES[schema['$id']] = _schemaFactory(schema, class_name or schema['title'], parents)
            return self._CLASSES[schema['$id']]
        return self._CLASSES.get(schema_url, None) or _make_class()

    def cache(self, schema_url, isfile=False):
        if schema_url in _CACHE:
            return _CACHE[schema_url]
        elif isfile:
            with open(schema_url) as f:
                schema = json.load(f)
                _CACHE[schema['$id']] = schema
        else:
            r = requests.get(schema_url)
            r.raise_for_status()
            schema = r.json()
            _CACHE[schema['$id']] = schema
        return schema

def _flatten(o):
    if isinstance(o, (UnisObject, Local)):
        return {k: _flatten(v) for k,v in o}
    elif isinstance(o, List):
        return [_flatten(v) for v in o]
    elif isinstance(o, Primitive):
        return o._rt_raw
    return o
