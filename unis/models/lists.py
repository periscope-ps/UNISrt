import asyncio
import itertools
import math
import types

from collections import defaultdict
from lace.logging import trace
from urllib.parse import urlparse

from unis.utils import Events, Index
from unis.models import schemaLoader
from unis.models.models import Context as oContext
from unis.rest import UnisProxy, UnisReferenceError

class _sparselist(list):
    def __len__(self):
        return len(list(filter(lambda x: not isinstance(x, type(None)), self)))

class UnisCollection(object):
    class Context(object):
        def __init__(self, obj, rt):
            self._obj, self._rt = obj, rt
        def __getattr__(self, n):
            return self._obj.__getattribute__(n)
        def __getitem__(self, i):
            return oContext(self._obj.__getitem__(i), self._rt)
        def where(self, pred):
            for v in self._obj.where(pred):
                yield oContext(v, self._rt)
        def __iter__(self):
            for v in self._obj.__iter__():
                yield oContext(v, self._rt)
        def __repr__(self):
            return self._obj.__repr__()
        def __len__(self):
            return self._obj.__len__()
    collections = {}
    
    async def _mock(self):
        return None
    @classmethod
    @trace.debug("UnisCollection")
    def get_collection(cls, name, model, runtime):
        cls.collections[name] = cls.collections.get(name, cls(name, model))
        cls.collections[name]._growth = runtime.settings["cache"]["growth"]
        cls.collections[name]._subscribe = runtime.settings["proxy"]["subscribe"]
        return UnisCollection.Context(cls.collections[name], runtime)
    
    @trace.debug("UnisCollection")
    def __init__(self, name, model):
        self._complete_cache, self._get_next = self._proto_complete_cache, self._proto_get_next
        self.name, self.model = name, model
        self._indices, self._services, self._unis = {}, [], UnisProxy(name)
        self._block_size = 10
        self._stubs, self._cache = {}, _sparselist()
        self.createIndex("id")
        self.createIndex("selfRef")
        self._loop = asyncio.get_event_loop()
        
    @trace.debug("UnisCollection")
    def __getitem__(self, i):
        self._loop.run_until_complete(self._complete_cache())
        return self._cache[i]
    
    @trace.debug("UnisCollection")
    def __setitem__(self, i, item):
        self._check_record(item)
        if self._cache[i].selfRef != item.selfRef:
            raise AttributeError("Resource selfRefs do not match")
        
        if getattr(self._cache[i], "ts", 0) < item.ts:
            for k,v in item.__dict__.items():
                self._cache[i].__dict__[k] = v
            for _,index in self._indices.items():
                index.update(i, self._cache[i])
            if self._cache[i]._getattribute("selfRef"):
                self._stubs[self._unis.refToUID(self._cache[i]._getattribute("selfRef"))] = self._cache[i]
            self._serve(Events.update, self._cache[i])
    
    @trace.info("UnisCollection")
    def load(self):
        self._loop.run_until_complete(self._complete_cache())
        return list(self._cache)
    
    @trace.info("UnisCollection")
    def get(self, hrefs):
        hrefs = list(map(self._unis.refToUID, (hrefs if isinstance(hrefs, list) else [hrefs])))
        if any(x not in self._stubs for x in hrefs):
                raise UnisReferenceError("Requested object in unknown location", hrefs)
        to_get = [v for v in hrefs if not self._stubs[v]]
        if to_get:
            self._loop.run_until_complete(self._get_next(to_get))
        return [self._stubs[href] for href in hrefs]
    

    @trace.info("UnisCollection")
    async def _foo(self):
        await asyncio.sleep(0.1)
    @trace.info("UnisCollection")
    def append(self, item):
        self._check_record(item)
        item.setCollection(self)
        i = self.index(item)
        if not isinstance(i, type(None)):
            self.__setitem__(i, item)
            return self._cache[i]
        else:
            i = len(self._cache)
            self._cache.append(item)
            for _,index in self._indices.items():
                index.update(i, oContext(item, None))
            if item.selfRef:
                self._stubs[self._unis.refToUID(item.selfRef)] = item
            self._serve(Events.new, item)
            return item
    
    @trace.info("UnisCollection")
    def remove(self, item):
        self._check_record(item)
        self._unis.delete(item)
    
    @trace.info("UnisCollection")
    def index(self, item):
        item = item if isinstance(item, oContext) else oContext(item, None)
        if item.id:
            return self._indices['id'].index(item)
        else:
            try:
                return self._cache.index(item.getObject())
            except ValueError:
                return None
    
    @trace.info("UnisCollection")
    def where(self, pred):
        op = {
            "gt": lambda b: lambda a: type(a) is type(b) and a > b, 
            "ge": lambda b: lambda a: type(a) is type(b) and a >= b,
            "lt": lambda b: lambda a: type(a) is type(b) and a < b,
            "le": lambda b: lambda a: type(a) is type(b) and a <= b,
            "eq": lambda b: lambda a: type(a) is type(b) and a == b
        }
        self._loop.run_until_complete(self._complete_cache())
        if isinstance(pred, types.FunctionType):
            for v in filter(pred, self._cache):
                yield v
        else:
            non_index = {}
            subset = set(range(len(self._cache)))
            for k,v in pred.items():
                v = v if isinstance(v, dict) else { "eq": v }
                if k in self._indices:
                    for f,v in v.items():
                        subset &= self._indices[k].subset(f, v)
                else:
                    for f,v in v.items():
                        non_index[k] = op[f](v)
            
            for i in subset:
                record = self._cache[i]
                if all([v(record._getattribute(k, None, None)) for k,v in non_index.items()]):
                    yield record
    
    @trace.info("UnisCollection")
    def createIndex(self, k):
        index = Index(k)
        self._indices[k] = index
        for i, v in enumerate(self._cache):
            index.update(i, oContext(v))
    
    @trace.info("UnisCollection")
    def updateIndex(self, v):
        i = self.index(v)
        for key, index in self._indices.items():
            index.update(i, v)
    
    @trace.info("UnisCollection")
    async def addSources(self, hrefs):
        new = self._unis.addSources(hrefs)
        if new:
            self._complete_cache, self._get_next = self._proto_complete_cache, self._proto_get_next
            stubs = (self._unis.refToUID(v['selfRef']) for v in await self._unis.getStubs(new) if 'selfRef' in v)
            self._stubs.update({ k: None for k in stubs if k not in self._stubs })
            await self._add_subscription(new)
    
    @trace.info("UnisCollection")
    def addService(self, service):
        self._services.append(service)
    
    @trace.debug("UnisCollection")
    def _check_record(self, v):
        if self.model._rt_schema["name"] not in v.names:
            raise TypeError("Resource not of correct type: got {}, expected {}".format(self.model, type(v)))
        
    @trace.debug("UnisCollection")
    def _serve(self, ty, v):
        for service in self._services:
            f = getattr(service, ty.name)
            f(oContext(v, None))
    
    @trace.debug("UnisCollection")
    async def _proto_complete_cache(self):
        self._block_size = max(self._block_size, len(self._stubs) - len(self._cache))
        await self._get_next()
        
    @trace.debug("UnisCollection")
    async def _proto_get_next(self, ids=None):
        ids = ids or []
        todo = (k for k in self._stubs.keys() if not self._stubs[k])
        while len(ids) < self._block_size:
            try:
                ids.append(next(todo))
            except StopIteration:
                self._complete_cache = self._get_next = self._mock
                break
        requests = defaultdict(list)
        for v in ids:
            requests[v[0]].append(v[1][1])
        results = await asyncio.gather(*[self._get_block(k, v, self._block_size) for k,v in requests.items()])
        self._block_size *= self._growth
        for result in itertools.chain(*results):
            model = schemaLoader.get_class(result["$schema"], raw=True)
            self.append(model(result))
    
    @trace.debug("UnisCollection")
    async def _get_block(self, source, ids, blocksize):
        source = (source, (self.name, '0'))
        if len(ids) > blocksize:
            requests = [ids[i:i + blocksize] for i in range(0, len(ids), blocksize)]
            futures = [self._get_block(source, req, blocksize) for req in requests]
            results = await asyncio.gather(*futures)
            return list(itertools.chain(*results))
        else:
            return await self._from_unis(source, kwargs={"id": ids})
    
    @trace.debug("UnisCollection")
    async def _add_subscription(self, sources):
        @trace.debug("UnisCollection._add_subscription")
        def cb(v, action):
            if action in ['POST', 'PUT']:
                try:
                    schema = v.get("\\$schema", None) or v['$schema']
                except KeyError:
                    raise ValueError("No schema in message from UNIS - {}".format(v))
                model = schemaLoader.get_class(schema, raw=True)
                if action == 'POST':
                    resource = model(v)
                    self.append(resource)
                else:
                    try:
                        index = self.indices['id'].index(v['id'])
                    except IndexError:
                        return
                    old = self._cache[index].to_JSON()
                    self[index] = model({**old, **v})
            elif action == 'DELETE':
                try:
                    i = self._indices['id'].index(v['id'])
                    v = self._cache[i]
                    v.delete()
                    self._cache[i] = None
                    del self._stubs[self._unis.refToUID(v.selfRef)]
                    self._serve(Events.delete, v)
                    
                except IndexError:
                    raise ValueError("No such element in UNIS to delete")
        if self._subscribe:
            self._subscribe = False
            await self._unis.subscribe(sources, cb)
    
    @trace.debug("UnisCollection")
    async def _from_unis(self, source, start=0, size=None, kwargs={}):
        kwargs.update({"skip": start, "limit": size})
        result = await self._unis.get(source, **kwargs)
        return result if isinstance(result, list) else [result]
    
    def __repr__(self):
        rep = ".{} {}".format(self.name, self._cache.__repr__() if self._cache else "[...]")
        return "<UnisList{}>".format(rep if hasattr(self, "name") else "")
    
    @trace.debug("UnisCollection")
    def __len__(self):
        return len(self._cache) + len([x for x,v in self._stubs.items() if not v])
    
    @trace.debug("UnisCollection")
    def __contains__(self, item):
        return item in self._cache
    
    @trace.debug("UnisCollection")
    def __iter__(self):
        self._loop.run_until_complete(self._complete_cache())
        return iter(self._cache)
