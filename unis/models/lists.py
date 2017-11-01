import asyncio
import math
import types

from lace.logging import trace

from unis.utils import Events, Index

class UnisCollection(object):
    @trace.debug("UnisCollection")
    def __init__(self, name, model, runtime):
        self.name = name
        self._model = model
        self._rt = runtime
        self._cache, self._stubs = [], {}
        self._block_size = 10
        self._indices = {}
        self._complete = False
        self.createIndex("id")
        self.createIndex("selfRef")
        
        if not subscribe:
            self._subscribe = lambda: None
        if self._rt._settings["cachemode"] == "greedy" or self.name in self._rt._settings["preload"]:
            self._complete_cache()
        else:
            self._get_stubs()
    
    @trace.debug("UnisCollection")
    def __getitem__(self, i):
        self._complete_cache()
        return self._cache[i]
    
    @trace.debug("UnisCollection")
    def __setitem__(self, i, v):
        self._check_record(v)
        if self._cache[i].selfRef != v.selfRef:
            raise AttributeError("Resource selfRefs do not match")
        
        if getattr(self._cache[i], "ts", 0) < v.ts:
            for k,v in v.__dict__.items():
                self._cache[i].__dict__[k] = v
            for _,index in self._indices.items():
                index.update(i, v)
            self._serve(Events.update, self._cache[i])
    
    @trace.info("UnisCollection")
    def get(self, hrefs):
        hrefs = hrefs if isinstance(hrefs, list) else [hrefs]
        to_get = []
        for href in hrefs:
            if not self._stub[href]:
                to_get.append(href.rsplit("/", 1)[1])
        asyncio.run_until_complete(self._get_next(to_get))
        return self._stubs[hrefs[0]] if len(hrefs) == 1 else [ self._stub[href] for href in hrefs ]
    
    @trace.info("UnisCollection")
    def append(self, item):
        self._check_record(item)
        item._runtime = self._rt
        item.setCollection(self.name)
        item.update()
        i = self._indices['id'].index(item)
        if i:
            self.__setitem__(i, item)
            return self[i]
        else:
            i = len(self._cache)
            self._cache.append(obj)
            for _,index in self._indices.items():
                index.update(index, item)
            if item.selfRef:
                self._stubs[item.selfRef] = item
            self._serve(Events.new, obj)
            return item
    
    @trace.info("UnisCollection")
    def index(self, item):
        return self._indices['id'].index(item)
    
    @trace.info("UnisCollection")
    def where(self, pred):
        op = {
            "gt": lambda b: lambda a: type(a) is type(b) and a > b, 
            "ge": lambda b: lambda a: type(a) is type(b) and a >= b,
            "lt": lambda b: lambda a: type(a) is type(b) and a < b,
            "le": lambda b: lambda a: type(a) is type(b) and a <= b,
            "eq": lambda b: lambda a: type(a) is type(b) and a == b
        }
        self._complete_cache()
        if isinstance(pred, types.FunctionType):
            for v in filter(pred, self._cache):
                yield v
        else:
            non_index = {}
            subset = set(range(len(self._cache)))
            for k,v in pred.items():
                v = v if isinstance(v, dict) else { "eq": v }
                if k in self._indices:
                    subset &= self._indices[k].subset(k, v)
                else:
                    non_index[k] = op[v[0]](v[1])
            
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
    
    @trace.debug("UnisCollection")
    def _check_record(self, v):
        if self._model._schema["name"] not in v.names:
            raise TypeError("Resource not of correct type: got {}, expected {}".format(self._model, type(v)))
        if v._runtime and v._runtime != self._rt:
            raise TypeError("Resource already member of another runtime")
    
    @trace.debug("UnisCollection")
    def _serve(self, ty, v):
        for service in self._service:
            f = getattr(service, ty.name)
            f(v)
    
    @trace.debug("UnisCollection")
    def _get_stubs(self):
        results, first = [], True
        i = 0
        while first or all(results):
            first = False
            futures = []
            for _ in range(10):
                futures.append(self._from_unis(i, 1000))
                i += 1000
            results.extend(asyncio.gather(futures))
        
        for v in itertools.chain(*results):
            self._stubs[v["selfRef"]] = None
    
    @trace.debug("UnisCollection")
    def _complete_cache(self):
        results, first = [], True
        i = 0
        while first or all(results):
            first = False
            futures = []
            for _ in range(10):
                futures.append(self._from_unis(i, 1000))
                i += 1000
            results.extend(asyncio.gather(futures))
        
        for v in itertools.chain(*results):
            model = schemaLoader.get_class(v["$schema"])
            self.append(model(v, self._rt, local=False))
        self._complete_cache = lambda: None
        self._complete = True
    
    @trace.debug("UnisCollection")
    async def _get_next(self, ids=[]):
        todo = (href.rsplit("/", 1)[1] for href,stub in self._stubs.items() if not stub)
        while len(ids) < self._block_size:
            try:
                ids.append(next(todo))
            except StopIteration:
                self._complete = True
                break
        
        results = await self._get_block(ids, self._block_size)
        self._block_size *= self._rt.settings["proxy"]["growth"]
        for result in itertools.chain(*results):
            model = schemaLoader.get_class(result["$schema"])
            self.append(model(result, self._rt, local=False))
    
    @trace.debug("UnisCollection")
    async def _get_block(self, ids, blocksize):
        if len(ids) > blocksize:
            requests = [ids[i:i + blocksize] for i in range(0, len(ids), blocksize)]
            return await asyncio.gather([self._get_block(req, blocksize) for req in requests])
        else:
            
            return [self._from_unis(kwargs={"id": ids})]
    
    @trace.debug("UnisCollection")
    def _subscribe(self):
        def cb(v):
            self.append(v)
        self._rt._unis.subscribe(self.name, _cb)
        self._subscribe = lambda: None
    
    @trace.debug("UnisCollection")
    async def _from_unis(self, start=0, size=None, kwargs={}):
        self._subscribe()
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._rt._unis.get, limit=size, kwargs={"skip": start, kwargs=kwargs})
        

    @trace.debug("UnisCollection")
    def __repr__(self):
       return "<UnisList.{} {}>".format(self.name, self._cache.__repr__ if self._cache else "[...]")
    
    @trace.debug("UnisCollection")
    def __len__(self):
        return len(self._cache) + len(stubs)
    
    @trace.debug("UnisCollection")
    def __contains__(self, item):
        return item in self._cache
    
    @trace.debug("UnisCollection")
    def __iter__(self):
        if not self._complete:
            self._block_size = len(self._stubs)
            self._complete_cache()
        return iter(self._cache)
    
        
