import asyncio
import uuid

from collections import defaultdict
from lace.logging import trace

from unis.models import schemaLoader
from unis.models.lists import UnisCollection
from unis.models.models import Context
from unis.rest import UnisReferenceError, UnisProxy

from urllib.parse import urlparse

class ObjectLayer(object):
    @trace.debug("OAL")
    def __init__(self, runtime):
        self.settings, self._cache, self._pending = runtime.settings, {}, set()
    
    def __getattr__(self, n):
        try:
            return super(ObjectLayer, self).__getattribute__(n)
        except AttributeError:
            try:
                return self._cache[n]
            except KeyError:
                raise AttributeError("{n} not found in ObjectLayer".format(n = n))
    
    @trace.debug("OAL")
    def find(self, href):
        try:
            return self._cache[urlparse(href).path.split('/')[1]].get(href)
        except UnisReferenceError as e:
            self.addSources([e.href])
            return self._cache[urlparse(href).path.split('/')[1]].get(href)
    
    @trace.info("OAL")
    def flush(self):
        if self._pending:
            cols = defaultdict(list)
            list(map(lambda r: cols[r.getCollection().name].append(r), self._pending))
            asyncio.get_event_loop().run_until_complete(asyncio.gather(*[self._do_update(v,k) for k,v in cols.items()]))
        
    @trace.info("OAL")
    def update(self, resource):
        if resource.selfRef:
            if resource.getObject() not in self._pending:
                self._pending.add(resource.getObject())
                if not self.settings['defer_update']:
                    asyncio.get_event_loop().run_until_complete(self._do_update([resource], resource.getCollection().name))
                
    @trace.debug("OAL")
    async def _do_update(self, resources, collection):
        self._cache[collection].locked = True
        response = []
        map(lambda x: x.validate(), resources)
        try:
            response = await self._cache[collection]._unis.post(resources)
        except:
            raise
        finally:
            response = response if isinstance(response, list) else [response]
            for r in resources:
                r = Context(r, self)
                resp = next(o for o in response if o['id'] == r.id)
                r.__dict__["selfRef"] = resp["selfRef"]
                self._cache[collection].updateIndex(r)
            list(map(self._pending.remove, resources))
            self._cache[collection].locked = False
    
    @trace.info("OAL")
    def addSources(self, hrefs):
        loop = asyncio.get_event_loop()
        proxy = UnisProxy(None)
        proxy.addSources(hrefs)
        for r in loop.run_until_complete(proxy.getResources()):
            ref = (urlparse(r['href']).path.split('/')[1], r['targetschema']['items']['href'])
            if ref[0] not in ['events', 'data']:
                col = UnisCollection.get_collection(ref[0], schemaLoader.get_class(ref[1], raw=True), self)
                self._cache[col.name] = col
        loop.run_until_complete(asyncio.gather(*[c.addSources(hrefs) for c in self._cache.values()]))
    
    @trace.info("OAL")
    def preload(self):
        _p = lambda c: c.name in self.settings['preload'] or self.settings['cache']['mode'] == 'greedy'
        futures = [c._complete_cache() for c in self._cache.values() if _p(c)]
        asyncio.get_event_loop().run_until_complete(asyncio.gather(*futures))
        
    @trace.info("OAL")
    def insert(self, res, uid=None):
        try:
            res = schemaLoader.get_class(res["$schema"]) if isinstance(res, dict) else res
        except KeyError:
            raise ValueError("No schema in dict, cannot continue")
            
        res.id = uid or res.id or str(uuid.uuid4())
        res.setRuntime(self)
        self._cache[self.getModel(res.names)].append(res.getObject())
        return res
        
    @trace.info("OAL")
    def getModel(self, names):
        try:
            return next(c.name for c in self._cache.values() if c.model._rt_schema["name"] in list(names))
        except StopIteration:
            raise ValueError("Resource type {n} not found in ObjectLayer".format(n=names))
    
    @trace.info("OAL")
    def about(self):
        return list(self._cache.keys())
    
    @trace.info("OAL")
    def shutdown(self):
        self.flush()
        list(map(lambda c: c.shutdown(), self._cache.values()))
    @trace.debug("OAL")
    def __contains__(self, resource):
        try:
            col = next(c for c in self._cache.values() if c.model._rt_schema["name"] in resource.names)
            return isinstance(resource, type) or resource in self._cache[col.name]
        except StopIteration:
            return False
