import asyncio, uuid

from collections import defaultdict
from lace.logging import trace

from unis.models import schemaLoader
from unis.models.lists import UnisCollection
from unis.models.models import Context
from unis.rest import UnisProxy, UnisClient
from unis.exceptions import UnisReferenceError
from unis.utils import asynchronous

from urllib.parse import urlparse

@trace("unis.runtime")
class ObjectLayer(object):
    """
    The :class:`ObjectLayer <unis.runtime.oal.ObjectLayer>` contains the collections of 
    resources and provides functionality for inserting and removing resources.
    All functions documented herein have passthrough to the :class:`Runtime <unis.runtime.runtime.Runtime>`
    object associated with the :class:`ObjectLayer <unis.runtime.oal.ObjectLayer>` and should be called
    indirectly from the :class:`Runtime <unis.runtime.runtime.Runtime>` object.
    
    The :class:`ObjectLayer <unis.runtime.oal.ObjectLayer>` includes properties for each collection of resource
    types as indicated by its consituent client data stores.  Because of this, it is impossible
    to fully document these properties.  The :attr:`Runtime.collections <unis.runtime.runtime.Runtime.collections>`
    property may be used to examine a list of these properties.  In practise, most applications will
    have out-of-band knowledge of what collections it requires.  The following are an incomplete list
    of potential :class:`UnisCollection <unis.models.lists.UnisCollection>` caches.
    
    .. attribute:: nodes
        
        :class:`UnisCollection <unis.models.lists.UnisCollection>` containing cached resources that are associated
        with remote data store ``nodes`` endpoints.
        
        .. note:: This is an example of many possible automatically generated properties.
    
    .. attribute:: links
        
        :class:`UnisCollection <unis.models.lists.UnisCollection>` containing cached resources that are associated
        with remote data store ``links`` endpoints.
        
        .. note:: This is an example of many possible automatically generated properties.
    
    .. attribute:: ports
        
        :class:`UnisCollection <unis.models.lists.UnisCollection>` containing cached resources that are associated
        with remote data store ``ports`` endpoints.
        
        .. note:: This is an example of many possible automatically generated properties.
    """
    def __init__(self, settings):
        self.settings, self._pending, self._services = settings, set(), []
    
    def __getattr__(self, n):
        try:
            return super(ObjectLayer, self).__getattribute__(n)
        except AttributeError:
            try:
                return self._cache(n)
            except KeyError as e:
                raise AttributeError("{n} not found in ObjectLayer".format(n = n)) from e

    def _cache(self, n=None):
        return UnisCollection.from_name(n, self)
    
    def find(self, href):
        """
        :param str href: link to the reference to locate.
        :return: :class:`UnisObject <unis.models.models.UnisObject>`
        
        ``find`` will locate any existing resource and return it as a :class:`UnisObject <unis.models.models.UnisObject>`.
        The resource will be resolved whether it is located in the local :class:`UnisCollection <unis.models.lists.UnisCollection>`
        or in a remote data store.
        """
        col = urlparse(href).path.split('/')[1]
        try:
            return self._cache(col).get([href])
        except UnisReferenceError as e:
            new_sources = [{'url': r, 'default': False, 'enabled': True} for r in e.hrefs]
            self.addSources(new_sources)
            return self._cache(col).get([href])
    
    
    def flush(self):
        """
        When the containing runtime is set to ``deferred_mode``, flush forces all locally staged
        changes to each modified resource's respective remote data store.  In ``immediate_mode``
        the backend update happens automatically as soon as a :class:`UnisObject <unis.models.models.UnisObject>`
        is modified and the flush function need not be called.
        """
        if self._pending:
            cols = defaultdict(list)
            [cols[r.getSource(), r.getCollection().name].append(r) for r in self._pending]
            self._do_update(cols)
    
    def _update(self, res):
        if res.selfRef:
            if res not in self._pending:
                res._staged = True
                self._pending.add(res)
                if not self.settings['proxy']['defer_update']:
                    self._do_update({(res.getSource(), res.getCollection().name): [res]})
    
    def _do_update(self, pending):
        request = {}
        for (cid, collection), reslist in pending.items():
            self._cache(collection).pre_flush(reslist)
            self._cache(collection).locked = True
            valid = all([i.validate() for i in reslist])
            items = [i.to_JSON() for i in reslist]
            for item in items:
                if 'ts' in item:
                    del item['ts']
            request[(cid, collection)] = items
        
        response = []
        try:
            response = UnisProxy.post(request)
        except:
            raise
        finally:
            for (_, col), items in pending.items():
                self._cache(col).post_flush(items)
                for r in items:
                    r = r if isinstance(r, Context) else Context(r, self)
                    try:
                        resp = next(o for o in response if o['id'] == r.id)
                    except StopIteration:
                        continue
                    r.__dict__["selfRef"] = resp["selfRef"]
                    self._cache(col).updateIndex(r)
                    try: self._pending.remove(r)
                    except KeyError: continue
                    r._staged = False
                self._cache(col).locked = False
    
    def addSources(self, hrefs):
        """
        :param list[str] hrefs: list of remote data store urls
        
        ``addSource`` includes a new remote data store to be tracked and managed by the
        runtime.  This function is called automatically when a new :class:`Runtime <unis.runtime.runtime.Runtime>`
        is created and when a new remote store is detected by reference in a accessed property.
        This function may be used to add new data stores manually, but should do so sparingly.
        """
        proxy = UnisProxy()
        clients = proxy.addSources(hrefs, self.settings['namespace'])
        if not clients:
            return
        for r in asynchronous.make_async(proxy.getResources, clients):
            ref = (urlparse(r['href']).path.split('/')[1], r['targetschema']['items']['href'])
            if ref[0] not in ['events', 'data']:
                model = schemaLoader.get_class(ref[1], raw=True)
                col = UnisCollection.get_collection(ref[0], model, self)
                for service in self._services:
                    service.attach(col)
                    
        asynchronous.make_async(asyncio.gather, *[c.addSources(clients) for c in self._cache()])
        asynchronous.make_async(asyncio.gather, proxy.subscribe_connect(clients))
    
    def _preload(self):
        _p = lambda c: c.name in self.settings['cache']['preload'] or self.settings['cache']['mode'] == 'greedy'
        values = [c.load() for c in self._cache() if _p(c)]
        
    def _insert(self, res):
        try:
            res = schemaLoader.get_class(res["$schema"]) if isinstance(res, dict) else res
        except KeyError:
            raise ValueError("No schema in dict, cannot continue")
            
        res.getObject().__dict__['id'] = res.id or str(uuid.uuid4())
        res.setRuntime(self)
        res.setObject(self._cache(self.getModel(res.names)).append(res.getObject()))
        return res

    def _remove(self, res):
        self._cache(self.getModel(res.names)).remove(res)
        
    def getModel(self, names):
        """
        :param list[str] names: list of potential names for a model.
        :return: string collection name
        
        ``getModel`` determines which collection is associated with a given resource by resource name.
        In order to support json schema style inheritence, each resource contains a list of names
        similar to the python MRO.
        """
        try:
            return next(c.name for c in self._cache() if c.model._rt_schema["name"] in list(names))
        except StopIteration:
            raise ValueError("Resource type {n} not found in ObjectLayer".format(n=names))
    
    def about(self):
        """
        :return: list of collection names
        
        Returns a list including all names for each collection in the :class:`ObjectLayer <unis.runtime.oal.ObjectLayer>`.
        """
        return [c.name for c in self._cache()]
    
    def shutdown(self):
        self.flush()
        UnisClient.shutdown()
    def __contains__(self, resource):
        try:
            col = next(c for c in self._cache() if c.model._rt_schema["name"] in resource.names)
            return isinstance(resource, type) or resource in self._cache(col.name)
        except StopIteration:
            return False
