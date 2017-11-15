import asyncio
import itertools
import math
import types

from lace.logging import trace

from unis.utils import Events, Index
from unis.models import schemaLoader

class UnisCollection(object):
    async def _mock(self):
        return None
    @classmethod
    @trace.debug("UnisCollection")
    async def new_collection(cls, name, model, runtime):
        @trace.debug("UnisCollection")
        async def _get_stubs():
            result = list(await runtime._unis.get(name, None, {"fields": "selfRef"}))
            return result
        
        preload = False
        stubs = {}
        for v in await _get_stubs():
            try:
                stubs[v["selfRef"]] = None
            except KeyError:
                continue
        collection = cls(name, model, runtime, stubs)
        if runtime.settings["cache"]["mode"] == "greedy" or name in runtime.settings["preload"]:
            await collection._complete_cache()
        return collection
    
    @trace.debug("UnisCollection")
    def __init__(self, name, model, runtime, stubs):
        self.name, self.model, self._rt = name, model, runtime
        self._cache, self._stubs = [], stubs
        self._block_size = 10
        self._indices, self._services = {}, []
        self.createIndex("id")
        self.createIndex("selfRef")
        self._loop = asyncio.get_event_loop()
        
        if not self._rt.settings["proxy"]["subscribe"]:
            self._subscribe = lambda: None
    
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
                index.update(i, item)
            if self._cache[i].selfRef:
                self._stubs[self._cache[i].selfRef] = self._cache[i]
            self._serve(Events.update, self._cache[i])
    
    @trace.info("UnisCollection")
    def load(self):
        self._loop.run_until_complete(self._complete_cache())
        return list(self._cache)
    
    @trace.info("UnisCollection")
    def get(self, hrefs):
        hrefs = hrefs if isinstance(hrefs, list) else [hrefs]
        to_get = []
        for href in hrefs:
            if not self._stubs[href]:
                to_get.append(href.rsplit("/", 1)[1])
        if to_get:
            self._loop.run_until_complete(self._get_next(to_get))
        return self._stubs[hrefs[0]] if len(hrefs) == 1 else [ self._stub[href] for href in hrefs ]
    
    @trace.info("UnisCollection")
    def append(self, item):
        self._check_record(item)
        item.setRuntime(self._rt)
        item.setCollection(self.name)
        i = self._indices['id'].index(item)
        if i:
            self.__setitem__(i, item)
            return self._cache[i]
        else:
            i = len(self._cache)
            self._cache.append(item)
            for _,index in self._indices.items():
                index.update(i, item)
            if item.selfRef:
                self._stubs[item.selfRef] = item
            self._serve(Events.new, item)
            return item
    
    @trace.info("UnisCollection")
    def index(self, item):
        if hasattr(item, "id"):
            return self._indices['id'].index(item)
        else:
            return self._cache.index(item)
    
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
                if all([v(getattr(record, k, None)) for k,v in non_index.items()]):
                    yield record
    
    @trace.info("UnisCollection")
    def createIndex(self, k):
        index = Index(k)
        self._indices[k] = index
        for i, v in enumerate(self._cache):
            index.update(i, v)
    
    @trace.info("UnisCollection")
    def updateIndex(self, v):
        i = self.index(v)
        for key, index in self._indices.items():
            index.update(i, v)
    
    @trace.debug("UnisCollection")
    def _check_record(self, v):
        if self.model._rt_schema["name"] not in v.names:
            raise TypeError("Resource not of correct type: got {}, expected {}".format(self.model, type(v)))
        if v._rt_runtime and v._rt_runtime != self._rt:
            raise TypeError("Resource already member of another runtime")
    
    @trace.info("UnisCollection")
    def addService(self, service):
        self._services.append(service)
    
    @trace.debug("UnisCollection")
    def _serve(self, ty, v):
        for service in self._services:
            f = getattr(service, ty.name)
            f(v)
    
    @trace.debug("UnisCollection")
    async def _complete_cache(self):
        async def worker(jobs):
            job = await jobs.get()
            resources = []
            while job:
                start, batch = job
                resources.extend(await self._from_unis(start, batch))
                job = await jobs.get()
            await jobs.put(None)
            return resources
        
        jobs = asyncio.Queue()
        loop = asyncio.get_event_loop()
        skip = 0
        for start in range(0, len(self._stubs), self._rt.settings["proxy"]["batch"]):
            jobs.put_nowait((start, self._rt.settings["proxy"]["batch"]))
        jobs.put_nowait(None)
        
        workers = [asyncio.ensure_future(worker(jobs), loop=loop) for _ in range(self._rt.settings["proxy"]["threads"])]
        results = await asyncio.gather(*workers)
        
        for v in itertools.chain(*results):
            model = schemaLoader.get_class(v["$schema"])
            self.append(model(v, self._rt))
        self._complete_cache = self._mock
        self._get_next = self._mock
    
    @trace.debug("UnisCollection")
    async def _get_next(self, ids=[]):
        todo = (href.rsplit("/", 1)[1] for href,stub in self._stubs.items() if not stub)
        while len(ids) < self._block_size:
            try:
                ids.append(next(todo))
            except StopIteration:
                self._complete_cache = self._mock
                self._get_next = self._mock
                break
        
        results = await self._get_block(ids, self._block_size)
        self._block_size *= self._rt.settings["cache"]["growth"]
        for result in results:
            model = schemaLoader.get_class(result["$schema"])
            self.append(model(result, self._rt))
    
    @trace.debug("UnisCollection")
    async def _get_block(self, ids, blocksize):
        if len(ids) > blocksize:
            requests = [ids[i:i + blocksize] for i in range(0, len(ids), blocksize)]
            futures = [self._get_block(req, blocksize) for req in requests]
            results = await asyncio.gather(*futures)
            return list(itertools.chain(*results))
        else:
            return await self._from_unis(kwargs={"id": ids})
    
    @trace.debug("UnisCollection")
    def _subscribe(self):
        @trace.debug("UnisCollection._subscribe")
        def cb(v):
            try:
                model = schemaLoader.get_class(v["\\$schema"])
                resource = model(v)
            except KeyError:
                raise ValueError("No schema in message from UNIS")
            self.append(resource)
        asyncio.run_until_complete(self._rt._unis.subscribe(self.name, cb))
        self._subscribe = lambda: None
    
    @trace.debug("UnisCollection")
    async def _from_unis(self, start=0, size=None, kwargs={}):
        self._subscribe()
        loop = asyncio.get_event_loop()
        kwargs.update({"skip": start, "limit": size})
        result = await self._rt._unis.get(self.name, None, kwargs)
        return result if isinstance(result, list) else [result]
        

    def __repr__(self):
       return "<UnisList{}>".format(".{} {}".format(self.name, self._cache.__repr__() if self._cache else "[...]") 
                                    if hasattr(self, "name") else "")
    
    @trace.debug("UnisCollection")
    def __len__(self):
        return len(self._cache) + len([x for x, v in self._stubs.items() if not v])
    
    @trace.debug("UnisCollection")
    def __contains__(self, item):
        return item in self._cache
    
    @trace.debug("UnisCollection")
    def __iter__(self):
        self._loop.run_until_complete(self._complete_cache())
        return iter(self._cache)
    
        
