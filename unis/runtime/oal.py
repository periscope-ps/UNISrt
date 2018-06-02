import asyncio
import uuid

from collections import defaultdict
from lace.logging import trace

from unis.models import schemaLoader
from unis.models.lists import UnisCollection
from unis.models.models import Context
from unis.rest import UnisProxy, UnisClient
from unis.exceptions import UnisReferenceError
from unis.utils import async

from urllib.parse import urlparse

class ObjectLayer(object):
    @trace.debug("OAL")
    def __init__(self, settings):
        self.settings, self._cache, self._pending = settings, {}, set()
    
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
            res = self._cache[urlparse(href).path.split('/')[1]].get([href])
            return res
        except UnisReferenceError as e:
            new_sources = [{'url': r, 'default': False, 'enabled': True} for r in e.hrefs]
            self.addSources(new_sources)
            return self._cache[urlparse(href).path.split('/')[1]].get([href])
    
    @trace.info("OAL")
    def flush(self):
        if self._pending:
            cols = defaultdict(list)
            [cols[r.getSource(), r.getCollection().name].append(r) for r in self._pending]
            self._do_update(cols)
    
    @trace.info("OAL")
    def update(self, res):
        if res.selfRef:
            if res not in self._pending:
                self._pending.add(res)
                if not self.settings['proxy']['defer_update']:
                    self._do_update({(res.getSource(), res.getCollection().name): [res]})
    
    @trace.debug("OAL")
    def _do_update(self, pending):
        request = {}
        for (cid, collection), reslist in pending.items():
            self._cache[collection].locked = True
            items = []
            for item in reslist:
                item.validate()
                items.append(item.to_JSON())
            request[(cid, collection)] = items
        
        response = []
        try:
            response = UnisProxy.post(request)
        except:
            raise
        finally:
            for (_, col), items in pending.items():
                for r in items:
                    r = r if isinstance(r, Context) else Context(r, self)
                    resp = next(o for o in response if o['id'] == r.id)
                    r.__dict__["selfRef"] = resp["selfRef"]
                    self._cache[col].updateIndex(r)
                    self._pending.remove(r)
                self._cache[col].locked = False
    
    @trace.info("OAL")
    def addSources(self, hrefs):
        proxy = UnisProxy()
        clients = proxy.addSources(hrefs, self.settings['namespace'])
        if not clients:
            return
        for r in async.make_async(proxy.getResources, clients):
            ref = (urlparse(r['href']).path.split('/')[1], r['targetschema']['items']['href'])
            if ref[0] not in ['events', 'data']:
                model = schemaLoader.get_class(ref[1], raw=True)
                col = UnisCollection.get_collection(ref[0], model, self)
                self._cache[col.name] = col
        async.make_async(asyncio.gather, *[c.addSources(clients) for c in self._cache.values()])
    
    @trace.info("OAL")
    def preload(self):
        _p = lambda c: c.name in self.settings['cache']['preload'] or self.settings['cache']['mode'] == 'greedy'
        values = [c.load() for c in self._cache.values() if _p(c)]
        
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
        UnisClient.shutdown()
    @trace.debug("OAL")
    def __contains__(self, resource):
        try:
            col = next(c for c in self._cache.values() if c.model._rt_schema["name"] in resource.names)
            return isinstance(resource, type) or resource in self._cache[col.name]
        except StopIteration:
            return False
