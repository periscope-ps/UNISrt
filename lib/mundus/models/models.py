import jsonschema, uuid
from typing import NoReturn, Any, Callable

import mundus
from mundus import containers, exceptions
from mundus.models.cache import class_factory, _cache, _CACHE
from mundus.models.relationship import RelationshipList
from mundus.settings import ID_FIELD as ID_NAME, TS_FIELD as TS_NAME, TY_FIELD as TY_NAME

_sentinal, getr = object(), object.__getattribute__
class AbstractObject(object):
    """
    This class serves as the base class for all automatically generated jsonschema described classes.
    """
    __slots__ = ("_top", "_locks")
    def __init__(self, v: dict, top: 'AbstractObject'):
        for k,v in {**{"_top": top, "_locks": set()}, **v}.items():
            object.__setattr__(self,k,v)

    def _commit_and_lock(self, ls: list['AbstractObject']) -> NoReturn:
        for v in ls:
            self._locks.add(v)
        top = getr(self, "_top")
        if top.container() is None:
            raise exceptions.RemovedEntityError("Cannot modify removed entity")
        top.container().commit(top)

    def _lifter(self, n: str, v: Any) -> 'AbstractObject':
        try:
            d = getr(self, "__dict__")
        except AttributeError:
            d = {}

        if n in getr(self, "__slots__") or n in d:
            if isinstance(v, (dict, list)):
                cls = _class_factory(self._schema["$id"], f"/{n}", getr(type(self), "__name__"))
                if isinstance(v, list):
                    v = DictObjectList(v, cls, self._top)
                else:
                    v = cls(v, self._top)
                super().__setattr__(n, v)
        return v

    def __getattribute__(self, n: str) -> Any:
        try: v = super().__getattribute__(n)
        except AttributeError:
            if n not in self._defaults:
                raise
            v = self._defaults[n]
        return getr(self, "_lifter")(n, v)

    def __setattr__(self, n: str, v: Any) -> NoReturn:
        super().__setattr__(n, v)
        if n in getr(AbstractObject, "__slots__"):
            return
        if (n in getr(self, "__slots__")) or hasattr(self, "__dict__"):
            self._commit_and_lock([n])

    def _visit(self, fn: Callable[['AbstractObject'], dict], **kwargs) -> dict:
        def app(n, v):
            if not isinstance(v, (list, dict, AbstractObject)):
                return v
            elif isinstance(v, AbstractObject):
                return v._visit(fn, **kwargs)
            else:
                return self._lifter(n,v)._visit(fn, **kwargs)

        fn(self, **kwargs)
        result = {}
        if hasattr(self, "__dict__"):
            for n,v in getr(self, "__dict__").items():
                result[n] = app(n,v)
        else:
            for n in self.__slots__:
                try:
                    result[n] = app(n, getr(self, n))
                except AttributeError:
                    result[n] = app(n, self._defaults[n])
        return result

    def _get_values(self) -> dict:
        def _fn(x):
            if hasattr(x, "_defaults"):
                return {**x._defaults}
            return []
        return self._visit(_fn)
    def _merge(self, other: 'AbstractObject') -> 'AbstractObject':
        def app(n, a):
            if n not in getr(self, "_locks"):
                b = getattr(other, n, _sentinal)
                if b != _sentinal:
                    if isinstance(a, AbstractObject):
                        a._merge(b)
                    else:
                        object.__setattr__(self, n, b)

        if hasattr(self, "__dict__"):
            d = getr(self, "__dict__")
            for n,v in getr(other, "__dict__").items():
                if n not in d:
                    d[n] = v
        else:
            for n in self.__slots__:
                app(n, getattr(self, n))

    def __iter__(self): return iter(self.items())
    def items(self):
        """
        :return: Returns an iterator for the fields and values as described in the underlying schema.
        :rtype: dict_items([])

        Iterate over all of the fields in the entity.  These fields correspond with the schema used
        to generate the object class.  Classes built with a schema including the
        `additionalProperties` field set to a non-false value will include *any* key/value assigned
        to the object.
        """
        return self._get_values().items()
    def unlock(self):
        return self._visit(lambda x: setattr(x, "_locks", set()))

class DictObjectList(AbstractObject):
    """
    This class provides features for `array` type components in jsonschema described entities.
    """
    __slots__ = ('_ls', '_cls')
    def __init__(self, ls: list[dict], cls: type, top: 'AbstractObject'):
        object.__setattr__(self, "_top", top)
        object.__setattr__(self, "_locks", set())
        self._ls, self._cls = [], cls
        for v in ls or []:
            self._ls.append(v)

    def __getattribute__(self, n: str):
        return getr(self, n)
    def __setattr__(self, n: str, v: any):
        return object.__setattr__(self, n, v)
    def __getitem__(self, idx: int) -> Any:
        if isinstance(idx, int):
            v = self._ls[idx]
            if isinstance(v, self._cls):
                self._ls[idx] = v
            else:
                self._ls[idx] = self._cls(v, self._top)
            return self._ls[idx]
        else:
            for i in range(idx.start or 0, idx.stop or len(self._ls), idx.step or 1):
                v = self._ls[i]
                if isinstance(v, self._cls):
                    self._ls[i] = v
                else:
                    self._ls[i] = self._cls(v, self._top)
            return self._ls[idx]

    def _visit(self, fn: Callable[['AbstractObject'], dict], **kwargs) -> dict:
        fn(self, **kwargs)
        result = []
        for i,v in enumerate(self._ls):
            if not isinstance(v, AbstractObject):
                self._ls[i] = self._cls(v, self._top)
            result.append(self._ls[i]._visit(fn, **kwargs))
        return result

    def _merge(self, other: 'DictObjectList') -> 'DictObjectList':
        short = min(len(self._ls), len(other._ls))
        for i in range(short):
            if i not in self._locks:
                self[i]._merge(other[i])
        if short < len(other._ls):
            for v in other[short:]:
                self._ls.append(dict(v))

    def append(self, v: dict) -> NoReturn:
        """
        :param v: Raw dictionary record to be appended to the list.
        :type v: dict

        Appends a value to the list.  If given an empty dictionary, the entry will be constructed
        automatically from the underlying schema with default values.
        """
        self._ls.append(v)
        self._commit_and_lock([len(self._ls) - 1])
    def insert(self, idx: int, v: dict) -> NoReturn:
        """
        :param idx: Index to insert the value.
        :type idx: int
        :param v: Raw dictionary record to be appended to the list.
        :type v: dict

        Insert a value to the list.  If given an empty dictionary, the entry will be constructed
        automatically from the underlying schema with default values.
        """
        self._ls.insert(idx, v)
        self._commit_and_lock(range(idx, len(self._ls) - 1))
    def pop(self, idx: int) -> 'AbstractObject':
        """
        :param idx: Index to insert the value.
        :type idx: int

        Remove a value to the list.
        """
        self._ls.pop(idx)
        self._commit_and_lock(range(idx, len(self._ls) - 2))
    def __setitem__(self, idx: int, v: Any) -> NoReturn:
        self._ls[idx] = v
        self._commit_and_lock([idx])

    def __len__(self):
        return len(self._ls)
    def __iter__(self):
        for i, v in enumerate(self._ls):
            if not isinstance(v, self._cls):
                v = self._ls[i] = self._cls(v, self._top)
            yield v

class Entity(AbstractObject):
    """
    This class provides the base bahavior for entities that are described in a jsonschema 
    UNIS object.  In addition the the features provided by 
    `AbstractObject <mundus.models.AbstractObject>`, Entities handles object validation and
    container interactions.
    """
    __slots__ = ("__id", "__ts", "__container", "__callback")
    @property
    def id(self) -> str:
        return self.__id
    @property
    def ts(self) -> str:
        return self.__ts
    @property
    def selfRef(self) -> str:
        return self._links["self"].format(id=self.__id)

    def __init__(self, v: dict=None):
        if v is None:
            v = {}
        try:
            del v["selfRef"]
        except KeyError:
            pass
        self.__id, self.__ts = v.pop(ID_NAME, str(uuid.uuid4())), v.pop(TS_NAME, 0)
        v.pop(TY_NAME, None)
        super().__init__(v, self)
        containers.get_container(None).add(self)
        self.__callback = lambda x,ch: None
        mundus.events.manager.publish(self, mundus.events.types.CREATE)

    def __getattribute__(self, n: str):
        try:
            return getr(self, "_get_links")(getr(self, "_links")[n].format(**{"id": self.id}))
        except (AttributeError, KeyError):
            return super().__getattribute__(n)

    def _get_links(self, n: str) -> RelationshipList:
        return lambda: RelationshipList(self.__container, n, self)

    def _event_callback(self, channel: str):
        self.__callback(self, channel)
    def _merge(self, other: 'Entity') -> 'Entity':
        if self.id != other.id:
            raise ValueError("Cannot merge entities with different IDs")
        if self.ts >= other.ts:
            return
        return super()._merge(other)

    def __iter__(self):
        return iter(self.items())
    def items(self):
        """
        :return: Returns an iterator for the fields and values as described in the underlying schema.
        :rtype: dict_items([])

        Iterate over all of the fields in the entity.  These fields correspond with the schema used
        to generate the object class.  Classes built with a schema including the
        `additionalProperties` field set to a non-false value will include *any* key/value assigned
        to the object.
        """
        return {**self._get_values(),
                **{ID_NAME: self.__id, TS_NAME: self.__ts, TY_NAME: getr(self, TY_NAME)}}.items()
    def container(self):
        """
        :return: Returns the mundus container responsible for this object.
        :rtype: :class:`Container <mundus.containers.container.Container>`

        This function returns the container being used to maintain the connection to the ground
        truth entity.
        """
        try:
            return self.__container
        except AttributeError:
            return None
    def _set_container(self, container):
        self.__container = container
    def clone(self) -> 'Entity':
        """
        :return: Returns an exact copy of the object.
        :rtype: :class:`Entity <mundus.models.models.Entity>`

        Returns an entity with the same values as the current object.  This new entity will have a
        different id and be automatically assigned to the null container.
        """
        d = {}
        for k,v in dict(self).items():
            if k not in [ID_NAME,TS_NAME,TY_NAME]:
                d[k] = v
        return type(self)(d)
    def addCallback(self, cb: Callable[['Entity', str], NoReturn]) -> NoReturn:
        """
        :param cb: A function called when the state of this object changes.
        :type cb: callable[:class:`Entity <mundus.models.models.Entity>`, :class:`Event <mundus.events.types>`]

        Add a callback function to this object.  When the object changes state, this callback is 
        invoked with the generating event.
        """
        def _wrapper(old):
            def _f(x,ch):
                cb(x,ch)
                old(x,ch)
            return _f
        self.__callback = _wrapper(self.__callback)
    def validate(self) -> NoReturn:
        """
        :raises jsonschema.exceptions.ValidationError: Validation fails from schema discrepency.

        Raises an exception if the current state of the entity does not match the schema used
        in it's generation.
        """
        resolver = jsonschema.RefResolver(self._schema["$id"], self._schema, _CACHE)
        jsonschema.validate(dict(self), self._schema, resolver=resolver)

def get_class(schema_url: str, root_class:type=None, class_name:str=None, is_file:bool=False) -> type:
    """
    :param schema_url: Address of the schema used to generate the class.
    :type schema_url: str
    :param root_class: The base class used to generate the class.
    :type root_class: type(, optional)
    :param class_name: Name of the generated class.  This is normally generated from the schema automatically.
    :type class_name: str(, optional)
    :param is_file: Enables file reading for schema_url
    :type is_file: bool

    Generates a class based on a provided json schema.
    """
    return class_factory(schema_url, root_class or Entity, class_name, is_file)

def _class_factory(root:str, path:str, basename:str):
    return  get_class(f"{root}{path}", AbstractObject,
                      f"{basename}{''.join([v.capitalize() for v in path.split('/')])}")
