import asyncio
import re
import uuid

from collections import defaultdict
from lace.logging import trace

from unis.models import schemaLoader
from unis.models.lists import UnisCollection
from unis.rest import UnisProxy

class ObjectLayer(object):
    @trace.debug("OAL")
    def __init__(self, runtime):
        async def _build_cols():
            def get_name(r):
                return re.compile('http[s]?://([^:/]+)(?::([0-9]{1,5}))?/(?P<col>[a-zA-Z]+)$').match(r["href"]).group("col")
            futures = []
            for r in await self._unis.getResources():
                name = get_name(r)
                if name not in ['events', 'data']:
                    futures.append(UnisCollection.new_collection(name, schemaLoader.get_class(r['targetschema']['items']['href']), self))
            for c in await asyncio.gather(*futures):
                self._cache[c.name] = c
            
        self.__dict__.update(**{"settings": runtime.settings, "_cache": {}, "_models": {}, "_rt": runtime,
                              "_unis": UnisProxy(runtime.settings["unis"]), "_pending": set()})
        asyncio.get_event_loop().run_until_complete(_build_cols())
    
    def __getattr__(self, n):
        try:
            return self._cache[n]
        except KeyError:
            raise AttributeError("{n} not found in ObjectLayer".format(n = n))
    
    @trace.debug("OAL")
    def find(self, href):
        matches = re.compile('http[s]?://([^:/]+(?::[0-9]{1,5}))/(?P<col>[a-zA-Z]+)/(\S+)$').match(href)
        try:
            return getattr(self, matches.group("col")).get(href)
        except AttributeError:
            raise ValueError("href must be a direct uri to a unis resource - Got ", href)
    
    @trace.info("OAL")
    def flush(self):
        if not self._pending:
            return
        cols = defaultdict(list)
        list(map(lambda r: cols[r.getCollection()].append(r), self._pending))
        asyncio.get_event_loop().run_until_complete(asyncio.gather(*[self._do_update(v,k) for k,v in cols.items()]))
        
    @trace.info("OAL")
    def update(self, resource):
        if resource.selfRef:
            if self.settings["defer_update"]:
                self._pending.add(resource)
            elif resource not in self._pending:
                asyncio.get_event_loop().run_until_complete(self._do_update([resource], resource.getCollection()))
                
    @trace.debug("OAL")
    async def _do_update(self, resources, collection):
        list(map(self._pending.add, resources))
        self._cache[collection].locked = True
        response = []
        map(lambda x: x.validate(), resources)
        try:
            response = list(await self._rt._unis.post(resources))
        except:
            raise
        finally:
            for r in resources:
                resp = next(o for o in list(response) if o['id'] == r.id)
                r.__dict__["selfRef"] = resp["selfRef"]
                self._cache[collection].updateIndex(r)
            list(map(self._pending.remove, resources))
            self._cache[collection].locked = False
            
    @trace.info("OAL")
    def insert(self, res, uid=None):
        try:
            res = schemaLoader.get_class(res["$schema"]) if isinstance(res, dict) else res
        except KeyError:
            raise ValueError("No schema in dict, cannot continue")
            
        res.id = uid or res.id or str(uuid.uuid4())
        return self._cache[self.getModel(res.names)].append(res)
        
    @trace.info("OAL")
    def getModel(self, names):
        try:
            return next(v.name for v in self._models.values() if v.model._rt_schema["name"] in list(names))
        except StopIteration:
            raise ValueError("Resource type {n} not found in ObjectLayer".format(n=names))
    
    @trace.info("OAL")
    def about(self):
        return [v.name for k,v in self._cache.items()]
    
    @trace.info("OAL")
    def shutdown(self):
        self.flush()
        self._unis.shutdown()
    
    @trace.debug("OAL")
    def __contains__(self, resource):
        try:
            col = next(v for v in self._models.values() if v.model._schema["name"] in resource.names)
            return isinstance(resource, type) or resource in self._cache[col.name]
        except StopIteration:
            return False
