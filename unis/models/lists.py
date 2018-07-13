import asyncio
import itertools
import math
import types

from collections import defaultdict, namedtuple
from lace.logging import trace
from urllib.parse import urlparse

from unis.exceptions import UnisReferenceError
from unis.models import schemaLoader
from unis.models.models import Context as oContext
from unis.rest import UnisProxy, UnisClient
from unis.utils import Events, Index, async

class _sparselist(list):
    def __len__(self):
        return len(self.valid_list())
    def valid_list(self):
        return list(filter(lambda x: not isinstance(x, str), self))

_rkey = namedtuple('ResourceKey', ['uid', 'cid'])
class UnisCollection(object):
    """
    :param str name: The name of the collection.
    :param runtime: The runtime associated with this collection.
    :type runtime: :class:`Runtime <unis.runtime.runtime.Runtime>`
    
    The :class:`UnisCollection <UnisCollection>` maintains the local cache for a corrosponding endpoint
    in the constituent remote data store.  It provides functionality for searching and filtering resources
    by property.
    
    .. warning:: Do not construct UnisCollections directly, use get_collection to generate correctly namespaced instance.
    """
    class Context(object):
        def __init__(self, obj, rt):
            self._obj, self._rt = obj, rt
        def __getattr__(self, n):
            return self._obj.__getattribute__(n)
        def __getitem__(self, i):
            return oContext(self._obj.__getitem__(i), self._rt)
        def where(self, pred):
            for v in self._obj.where(pred, self._rt):
                yield oContext(v, self._rt)
        def __iter__(self):
            for v in self._obj.__iter__():
                yield oContext(v, self._rt)
        def __repr__(self):
            return self._obj.__repr__()
        def __len__(self):
            return self._obj.__len__()
    collections = {}
    
    @classmethod
    @trace.debug("UnisCollection")
    def get_collection(cls, name, model, runtime):
        """
        :param str name: The name of the collection.
        :param model: The class of resources to be stored in the collection.
        :param runtime: The :class:`Runtime <unis.Runtime>` instance linked to the collection.
        :type model: :class:`UnisObject <unis.models.models.UnisObject>` class
        :type runtime: :class:`Runtime <unis.runtime.runtime.Runtime>`
        
        Factory constructor for :class:`UnisCollection <UnisCollection>`.  This function is used
        to generate a collection using the namespace from the provided :class:`Runtime <unis.runtime.runtime.Runtime>`.
        """
        namespace = "{}::{}".format(runtime.settings['namespace'], name)
        collection = cls.collections.get(namespace, None) or cls(name, model)
        collection._growth = max(collection._growth, runtime.settings['cache']['growth'])
        collection._subscribe |= runtime.settings['proxy']['subscribe']
        cls.collections[namespace] = collection
        return UnisCollection.Context(collection, runtime)
    @classmethod
    def from_name(cls, name, runtime):
        """
        :param str name: Name of the collection to query.
        :param runtime: Instance associated with the collection.
        :type runtime: :class:`Runtime <unis.runtime.runtime.Runtime>`
        
        Locate an existing collection within a runtime by name.
        """
        if name:
            namespace = "{}::{}".format(runtime.settings['namespace'], name)
            return UnisCollection.Context(cls.collections[namespace], runtime)
        else:
            results = []
            for ns,col in cls.collections.items():
                if ns.startswith(runtime.settings['namespace']):
                    results.append(UnisCollection.Context(col, runtime))
            return results
    def __init__(self, name, model):
        self._complete_cache, self._get_next = self._proto_complete_cache, self._proto_get_next
        self.name, self.model = name, model
        self._indices, self._services, self._unis = {}, [], UnisProxy(name)
        self._block_size = 10
        self._growth, self._subscribe = 0, False
        self._stubs, self._cache = {}, _sparselist()
        self.createIndex("id")
        self.createIndex("selfRef")
        self._loop = asyncio.get_event_loop()
        self._callbacks = []
        
    @trace.debug("UnisCollection")
    def __getitem__(self, i):
        if i >= len(self._cache):
            self._complete_cache()
        return self._cache[i]
    
    @trace.debug("UnisCollection")
    def __setitem__(self, i, item):
        self._check_record(item)
        old_id = self._cache[i]._getattribute('id', None)
        new_id = item._getattribute('id', None)
        if old_id != new_id:
            raise AttributeError("Resource ids {} and {} do not match".format(new_id, old_id))
        
        if getattr(self._cache[i], "ts", 0) < item.ts:
            for k,v in item.__dict__.items():
                self._cache[i].__dict__[k] = v
            for _,index in self._indices.items():
                index.update(i, oContext(self._cache[i], None))
            self._stubs[item._getattribute('id', None)] = self._cache[i]
    
    @trace.info("UnisCollection")
    def update(self, item):
        """
        :param item: Resource to update.
        
        .. warning:: This function is for internal use.
        
        Dispatch :class:`UnisObject <unis.models.models.UnisObject>` to registered 
        :class:`RuntimeServices <unis.services.abstract.RuntimeService>` when updates to the resource
        occur.
        """
        self._serve(Events.update, item)
    @trace.info("UnisCollection")
    def load(self):
        """
        :return: List of :class:`UnisObjects <unis.models.models.UnisObject>`.
        
        Force the collection to pull and cache all remote resources matching the collection.
        """
        self._complete_cache()
        return self._cache.valid_list()
    
    @trace.info("UnisCollection")
    def get(self, hrefs):
        """
        :param list[str] hrefs: List of urls to resources to query.
        :return: List of :class:`UnisObjects <unis.models.models.UnisObject>`
        
        Search for one or more resources and include them in the collection.  If the resources
        are local, they are immediately returned.
        """
        ids = [urlparse(r).path.split('/')[-1] for r in hrefs]
        try:
            to_get = [_rkey(uid, self._stubs[uid]) for uid in ids if isinstance(self._stubs[uid], str)]
        except KeyError:
            raise UnisReferenceError("Requested object in unregistered instance", hrefs)
        if to_get:
            self._get_next(to_get)
        return [self._stubs[uid] for uid in ids]
    
    @trace.info("UnisCollection")
    def append(self, item):
        """
        :param item: Resource to be added to the collection
        :type item: :class:`UnisObject <unis.models.models.UnisObject>`
        :return: :class:`UnisObject <unis.models.models.UnisObject>`
        
        Append a resource to the collection.  If the resource already appears in the collection
        the resource already in the collection is updated with values from the appended instance
        then returned.
        
        .. note:: This function is called automatically by :meth:`ObjectLayer.insert <unis.runtime.oal.ObjectLayer.insert>`
        """
        self._check_record(item)
        i = self.index(item)
        if i is not None:
            uid = item._getattribute('id', None)
            self.__setitem__(i, item)
            if uid not in self._stubs or isinstance(self._stubs[uid], str):
                self._stubs[uid] = self._cache[i]
            return self._cache[i]
        else:
            item.setCollection(self)
            self._stubs[item._getattribute('id', None)] = item
            i = len(self._cache)
            self._cache.append(item)
            for _,index in self._indices.items():
                index.update(i, oContext(item, None))
            self._serve(Events.new, item)
            return item
    
    @trace.info("UnisCollection")
    def remove(self, item):
        """
        :param item: Resource to be removed from the collection
        :type item: :class:`UnisObject <unis.models.models.UnisObject>`
        :return: :class:`UnisObject <unis.models.models.UnisObject>`
        
        Remove a resource from the collection.
        """
        self._check_record(item)
        try:
            self._unis.delete(item.getSource(), item._getattribute('id', None))
        except UnisReferenceError:
            return
    
    @trace.info("UnisCollection")
    def index(self, item):
        """
        :param item: Resource to be removed from the collection
        :type item: :class:`UnisObject <unis.models.models.UnisObject>`
        :return: integer index ofr the resource in the collection
        
        Gets the index of the object within the collection.
        """
        item = item if isinstance(item, oContext) else oContext(item, None)
        if item.id:
            return self._indices['id'].index(item)
        else:
            return None
    
    @trace.info("UnisCollection")
    def where(self, pred, ctx=None):
        """
        :param pred: Predicate used to filter resources.
        :param ctx: This parameter must be left empty when called by external sources.
        :type pred: callable or dictionary
        :return: Generator of :class:`UnisObject <unis.models.models.UnisObject>`
        
        ``where`` filters members of the collection by a provided predicate.  The predicate
        can take one of two forms.  If the predicate is a dictionary, each key corresponds with
        an attribute in the collection of objects.  The values of the dictionary maybe be a
        value to compare or another dictionary.  The inner dictionary may have keys in 
        "gt", "ge", "lt", "le", or "eq".  The value is then compared using the corresponding 
        comparitor.::
        
            pred = {"value": {"gt": 500}, "type": "test_nodes"}
            valid_nodes = nodes.where(pred)
        """
        op = {
            "gt": lambda b: lambda a: a > b, 
            "ge": lambda b: lambda a: a >= b,
            "lt": lambda b: lambda a: a < b,
            "le": lambda b: lambda a: a <= b,
            "eq": lambda b: lambda a: a == b
        }
        self._complete_cache()
        if isinstance(pred, types.FunctionType):
            for v in filter(lambda x: pred(oContext(x, ctx)), self._cache):
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
                try:
                    if all([v(record._getattribute(k, ctx, None)) for k,v in non_index.items()]):
                        yield record
                except TypeError:
                    pass
    
    @trace.info("UnisCollection")
    def createIndex(self, k):
        """
        :param str k: Key for the new index
        
        Generate an index for faster querying over a specified field.
        """
        if k not in self._indices:
            index = Index(k)
            self._indices[k] = index
            for i, v in enumerate(self._cache):
                index.update(i, oContext(v, None))
    
    @trace.info("UnisCollection")
    def updateIndex(self, v):
        """
        :param v: Resource to update index values.
        :type v: :class:`UnisObject <unis.models.models.UnisObject>`
        
        Update the index values for a modified or new resource.
        """
        i = self.index(v)
        for key, index in self._indices.items():
            index.update(i, v)
    
    @trace.info("UnisCollection")
    async def addSources(self, cids):
        """
        :param list[str] cids: List of :class:`CIDs <unis.rest.unis_client.CID>` to add
        :rtype: coroutine
        
        ``addSources`` includes a new data store with data matching the collection.
        
        .. warning:: This function is for internal use only.
        """
        self._complete_cache, self._get_next = self._proto_complete_cache, self._proto_get_next
        for v in filter(lambda x: 'selfRef' in x, await self._unis.getStubs(cids)):
            uid = urlparse(v['selfRef']).path.split('/')[-1]
            self._stubs[uid] = UnisClient.resolve(v['selfRef'])
        await self._add_subscription(cids)
    
    @trace.info("UnisCollection")
    def addService(self, service):
        """
        :param service: Service to include in the collection.
        :type service: :class:`RuntimeService <unis.services.abstract.RuntimeService>`
        
        Include a service to be run when resources contained in this collection are
        created, modified, or deleted.
        
        .. warning:: This function is for internal use only.
        """
        self._services.append(service)
    @trace.info("UnisCollection")
    def addCallback(self, cb):
        """
        :param callable cb: Function to attach to the collection.
        
        Include a callback function when resources in this collection are
        created, modified, or deleted.
        
        ``cb`` must match the prototype
        
        **Parameters:**
        
        * **resource:** resource to inspect
        * **type:** event type in ["new", "update", "delete"]
        """
        self._callbacks.append(cb)
    @trace.debug("UnisCollection")
    def _check_record(self, v):
        if self.model._rt_schema["name"] not in v.names:
            raise TypeError("Resource not of correct type: got {}, expected {}".format(self.model, type(v)))
        
    @trace.debug("UnisCollection")
    def _serve(self, ty, v):
        ctx = oContext(v, None)
        v._callback(ty.name)
        [cb(ctx, ty.name) for cb in self._callbacks]
        for service in self._services:
            f = getattr(service, ty.name)
            f(ctx)
    
    @trace.debug("UnisCollection")
    def _proto_complete_cache(self):
        self._block_size = max(self._block_size, len(self._stubs) - len(self._cache))
        self._get_next()
    
    @trace.debug("UnisCollection")
    def _proto_get_next(self, ids=None):
        ids = ids or []
        todo = (_rkey(k,v) for k,v in self._stubs.items() if isinstance(v, str))
        while len(ids) < self._block_size:
            try:
                ids.append(next(todo))
            except StopIteration:
                self._complete_cache = lambda: None
                self._get_next = lambda x: None
                break
        requests = defaultdict(list)
        for v in ids:
            requests[v.cid].append(v.uid)
        futs = [self._get_block(k,v,self._block_size) for k,v in requests.items()]
        results = async.make_async(asyncio.gather, *futs)
        self._block_size *= self._growth
        for result in itertools.chain(*results):
            model = schemaLoader.get_class(result["$schema"], raw=True)
            self.append(model(result))
    
    @trace.debug("UnisCollection")
    async def _get_block(self, source, ids, blocksize):
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
                    resource = self.append(resource)
                else:
                    try:
                        resource = self.get([v['selfRef']])[0]
                    except UnisReferenceError:
                        uid = urlparse(v['selfRef']).path.split('/')[-1]
                        cid = UnisClient.resolve(v['selfRef'])
                        self._proto_get_next([_rkey(uid, cid)])
                        resource = self.get([v['selfRef']])[0]
                    for k,v in v.items():
                        resource.__dict__[k] = v
                self.update(resource)
            elif action == 'DELETE':
                try:
                    i = self._indices['id'].index(v['id'])
                    v = self._cache[i]
                    v.delete()
                    self._cache[i] = None
                    del self._stubs[v._getattribute('id', None)]
                    self._serve(Events.delete, v)
                    
                except IndexError:
                    raise ValueError("No such element in UNIS to delete")
        if self._subscribe:
            await self._unis.subscribe(sources, cb)
    
    @trace.debug("UnisCollection")
    async def _from_unis(self, source, start=0, size=None, kwargs={}):
        kwargs.update({"skip": start, "limit": size})
        result = await self._unis.get(source, **kwargs)
        return result if isinstance(result, list) else [result]
    
    def __repr__(self):
        rep = ".{} {}".format(self.name, self._cache.__repr__() if self._cache and len(self._cache) < 4 else "[...]")
        return "<UnisList{}>".format(rep if hasattr(self, "name") else "")
    
    @trace.debug("UnisCollection")
    def __len__(self):
        return len(self._cache) + len([x for x,v in self._stubs.items() if not v])
    
    @trace.debug("UnisCollection")
    def __contains__(self, item):
        return item in self._cache
    
    @trace.debug("UnisCollection")
    def __iter__(self):
        self._complete_cache()
        return iter(self._cache)
