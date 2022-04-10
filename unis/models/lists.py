import asyncio, logging, itertools, math, types

from collections import defaultdict, namedtuple
from lace.logging import trace
from threading import RLock
from urllib.parse import urlparse

from unis.exceptions import UnisReferenceError, CollectionIndexError, UnisAttributeError, ConnectionError
from unis.models import schemaLoader
from unis.models.models import DeletedResource, Context as oContext
from unis.rest import UnisProxy, UnisClient
from unis.utils import Events, Index, UniqueIndex, asynchronous

MAX_QUERY_COUNT=1600

class _sparselist(list):
    def __len__(self):
        return len(self.valid_list())
    def valid_list(self):
        return list(filter(lambda x: x and not isinstance(x, str), self))
    def full_length(self):
        return super(_sparselist, self).__len__()
    
_rkey = namedtuple('ResourceKey', ['uid', 'cid'])
@trace("unis.models")
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
        def __setitem__(self, i, v):
            return self._obj.__setitem__(i, v)
        def where(self, pred):
            for v in self._obj.where(pred, self._rt):
                yield oContext(v, self._rt)
        def first_where(self, pred):
            v = self._obj.first_where(pred, self._rt)
            return None if isinstance(v, type(None)) else oContext(v, self._rt)
        def load(self):
            return [oContext(v, self._rt) for v in self._obj.load()]
        def __iter__(self):
            for v in self._obj.__iter__():
                yield oContext(v, self._rt)
        def __repr__(self):
            return self._obj.__repr__()
        def __len__(self):
            return self._obj.__len__()
    collections = {}
    
    @classmethod
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
        if name is not None:
            namespace = "{}::{}".format(runtime.settings['namespace'], name)
            if namespace not in cls.collections:
                raise KeyError(f"Invalid collection name '{name}'")
            return UnisCollection.Context(cls.collections[namespace], runtime)
        else:
            results = []
            for ns,col in cls.collections.items():
                if ns.startswith(runtime.settings['namespace']):
                    results.append(UnisCollection.Context(col, runtime))
            return results
    def __init__(self, name, model):
        self._lock = RLock()
        self._complete_cache, self._get_next = self._proto_complete_cache, self._proto_get_next
        self.name, self.model = name, model
        self._indices, self._services, self._unis = {}, [], UnisProxy(name)
        self._block_size = 10
        self._growth, self._subscribe = 0, False
        self._stubs, self._cache = {}, _sparselist()
        self.createIndex("id", unique=True)
        self.createIndex("selfRef", unique=True)
        self._loop = asyncio.get_event_loop()
        self._callbacks = []
        self._cids = set()
        
    def __getitem__(self, i):
        if i >= self._cache.full_length():
            self._complete_cache()
        return self._cache[i]
    
    def __setitem__(self, i, item):
        self._check_record(item)
        if not self._cache[i].merge(item, None):
            return None
        with self._lock:
            for k, index in self._indices.items():
                if self._cache[i]._getattribute(k, None, None) is not None:
                    index.update(i, self._cache[i]._getattribute(k, None))
        return self._cache[i]

    def pre_flush(self, items):
        """
        :param items: List of items to notify
        :type items: List[:class:`UnisObject <unis.models.models.UnisObject>`]

        Dispatch :class:`UnisObjects <unis.models.models.UnisObject>` to registered
        :class:`RuntimeServices <unis.service.abstract.RuntimeService>` before resource is
        flushed.
        """
        [self._serve(Events.preflush, item.getObject()) for item in items]
    def post_flush(self, items):
        """
        :param items: List of items to notify
        :type items: List[:class:`UnisObject <unis.models.models.UnisObject>`]

        Dispatch :class:`UnisObjects <unis.models.models.UnisObject>` to registered
        :class:`RuntimeServices <unis.service.abstract.RuntimeService>` after resource is
        flushed.
        """
        [self._serve(Events.postflush, item.getObject()) for item in items]
    def update(self, item, internal=False):
        """
        :param item: Resource to update.
        
        .. warning:: This function is for internal use.
        
        Dispatch :class:`UnisObject <unis.models.models.UnisObject>` to registered 
        :class:`RuntimeServices <unis.services.abstract.RuntimeService>` when updates to the resource
        occur.
        """
        self._serve(Events.internalupdate if internal else Events.update, item)
    def load(self):
        """
        :return: List of :class:`UnisObjects <unis.models.models.UnisObject>`.
        
        Force the collection to pull and cache all remote resources matching the collection.
        """
        self._complete_cache()
        return self._cache.valid_list()
    
    def get(self, hrefs):
        """
        :param list[str] hrefs: List of urls to resources to query.
        :return: List of :class:`UnisObjects <unis.models.models.UnisObject>`
        
        Search for one or more resources and include them in the collection.  If the resources
        are local, they are immediately returned.
        """
        with self._lock:
            ids = [urlparse(r).path.split('/')[-1] for r in hrefs]
            try:
                to_get = [_rkey(uid, self._stubs[uid]) for uid in ids if isinstance(self._stubs[uid], str) or not self._subscribe]
            except KeyError as e:
                raise UnisReferenceError("Requested object in unregistered instance", hrefs) from e
        if to_get:
            self._get_next(to_get)
        with self._lock:
            return [self._stubs[uid] for uid in ids]
    
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
        return self._validate_append(item)[1]

    def _validate_append(self, item):
        self._check_record(item)
        try:
            i = self.index(item)
        except CollectionIndexError:
            item.setCollection(self)
            with self._lock:
                self._stubs[item._getattribute('id', None)] = item
            i = self._cache.full_length()
            self._cache.append(item)
            with self._lock:
                for k, index in self._indices.items():
                    if item._getattribute(k, None, None) is not None:
                        index.update(i, item._getattribute(k, None))
            self._serve(Events.new, item)
            return (True, item)

        uid = item._getattribute('id', None)
        if not self.__setitem__(i, item):
            return (False, self._cache[i])
        with self._lock:
            if uid not in self._stubs or isinstance(self._stubs[uid], str):
                self._stubs[uid] = self._cache[i]
            return (True, self._cache[i])

    def remove(self, item):
        """
        :param item: Resource to be removed from the collection
        :type item: :class:`UnisObject <unis.models.models.UnisObject>`
        :return: :class:`UnisObject <unis.models.models.UnisObject>`
        
        Remove a resource from the collection.

        .. note:: In the case of resources in back end data stores, this operation will delete the back end resource as well.
        """
        self._check_record(item)
        try:
            self._unis.delete(item.getSource(), item.id)
        except (UnisReferenceError, ConnectionError):
            self._remove_record(item)
    
    def index(self, item):
        """
        :param item: Resource to be removed from the collection
        :type item: :class:`UnisObject <unis.models.models.UnisObject>`
        :return: integer index ofr the resource in the collection
        
        Gets the index of the object within the collection.
        """
        item = item if isinstance(item, oContext) else oContext(item, None)
        with self._lock:
            idx = self._indices['id']
        return idx.index(item.id)

    def first_where(self, pred, ctx=None):
        """
        :param pred: Predicate used to filter resources.
        :param ctx: This parameter must be left empty when called by external sources.
        :type pred: callable or dictionary
        :return: :class:`UnisObject <unis.models.models.UnisObject>` or None
        
        As :func:`UnisCollection.where <unis.models.list.UnisCollection.where>` but returns
        only the first resource matching the request.
        """
        try:
            return next(self.where(pred, ctx))
        except StopIteration:
            return None
    
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
            with self._lock:
                for v in self._cache:
                    try:
                        if v and pred(oContext(v, ctx)): yield v
                    except UnisAttributeError:
                        pass
        else:
            non_index = {}
            with self._lock:
                subset = set(range(self._cache.full_length()))
            for k,v in pred.items():
                v = v if isinstance(v, dict) else { "eq": v }
                with self._lock:
                    if k in self._indices.keys():
                        for f,v in v.items():
                            try:
                                subset &= self._indices[k].subset(f, v)
                            except CollectionIndexError: pass
                    else:
                        for f,v in v.items():
                            non_index[k] = op[f](v)
            for i in subset:
                record = self._cache[i]
                try:
                    if record and all([f(record._getattribute(k, ctx, None)) for k,f in non_index.items()]):
                        yield record
                except (TypeError,UnisAttributeError):
                    pass
    
    def createIndex(self, k, unique=False):
        """
        :param str k: Key for the new index
        
        Generate an index for faster querying over a specified field.
        """
        with self._lock:
            if k not in self._indices:
                self._indices[k] = UniqueIndex(k) if unique else Index(k)
                for i, v in enumerate(self._cache):
                    if v._getattribute(k, None, None) is not None:
                        self._indices[k].update(i, v._getattribute(k, None))
                    
    def updateIndex(self, res):
        """
        :param res: Resource to update index values.
        :type res: :class:`UnisObject <unis.models.models.UnisObject>`
        
        Update the index values for a modified or new resource.
        """
        with self._lock:
            i = self.index(res)
            for k, index in self._indices.items():
                v = getattr(res, k, None)
                if v is None:
                    index.remove(i)
                index.update(i, v)
            
    async def addSources(self, cids):
        """
        :param list[str] cids: List of :class:`CIDs <unis.rest.unis_client.CID>` to add
        :rtype: coroutine
        
        ``addSources`` includes a new data store with data matching the collection.
        
        .. warning:: This function is for internal use only.
        """
        with self._lock:
            self._complete_cache, self._get_next = self._proto_complete_cache, self._proto_get_next
            await self._update_stubs(cids)
        await self._add_subscription(cids)

    async def _update_stubs(self, cids):
        for v in filter(lambda x: 'selfRef' in x, await self._unis.getStubs(cids)):
            uid = urlparse(v['selfRef']).path.split('/')[-1]
            if uid not in self._stubs:
                try: self._stubs[uid] = UnisClient.resolve(v['selfRef'])
                except UnisReferenceError:
                    pass
        self._cids.update(cids)
        
    def addService(self, service):
        """
        :param service: Service to include in the collection.
        :type service: :class:`RuntimeService <unis.services.abstract.RuntimeService>`
        
        Include a service to be run when resources contained in this collection are
        created, modified, or deleted.
        
        .. warning:: This function is for internal use only.
        """
        with self._lock:
            self._services.append(service)
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
        with self._lock:
            self._callbacks.append(cb)
    def _remove_record(self, v):
        v = v if isinstance(v, oContext) else oContext(v, None)
        try:
            i = self.index(v)
            with self._lock:
                [index.remove(i) for index in self._indices.values()]
                self._cache[i] = None
        except CollectionIndexError:
            pass

        with self._lock:
            try: del self._stubs[v._getattribute('id')]
            except KeyError: pass
            
        v._delete()
        self._serve(Events.delete, v)
        v.setObject(DeletedResource())
        v.setRuntime(None)

    def _check_record(self, v):
        if self.model._rt_schema["name"] not in v.names:
            raise TypeError("Resource not of correct type: got {}, expected {}".format(self.model, type(v)))

    def _serve(self, ty, v):
        ctx = oContext(v, None)
        v._callback(ty.name)
        tocall = []
        with self._lock:
            [cb(ctx, ty.name) for cb in self._callbacks]
            for service in self._services:
                tocall.append(getattr(service, ty.name))
        [f(ctx) for f in tocall]
    
    def _proto_complete_cache(self):
        if not self._subscribe:
            asynchronous.make_async(self._update_stubs, self._cids)
        with self._lock:
            self._block_size = min(MAX_QUERY_COUNT, max(self._block_size, len(self._stubs) - len(self._cache)))
        self._get_next()
    
    def _proto_get_next(self, ids=None):
        ids = ids or []
        with self._lock:
            ids += [_rkey(k,v) for k,v in self._stubs.items() if isinstance(v,str) or not self._subscribe]
        if self._subscribe and self._block_size >= len(ids):
            self._complete_cache, self._get_next = lambda: None, lambda x=None: None
        requests = defaultdict(list)
        for v in ids[:self._block_size]:
            src = v.cid if isinstance(v.cid, str) else v.cid.getSource()
            requests[src].append(v.uid)

        futs = [self._get_block(k,v,self._block_size) for k,v in requests.items()]
        results = asynchronous.make_async(asyncio.gather, *futs)

        self._block_size *= self._growth
        for result in itertools.chain(*results):
            model = schemaLoader.get_class(result["$schema"], raw=True)
            self.append(model(result))
    
    async def _get_block(self, source, ids, blocksize):
        if len(ids) > blocksize:
            requests = [ids[i:i + blocksize] for i in range(0, len(ids), blocksize)]
            futures = [self._get_block(source, req, blocksize) for req in requests]
            results = await asyncio.gather(*futures)
            return list(itertools.chain(*results))
        else:
            return await self._from_unis(source, kwargs={"id": ids})
    
    async def _add_subscription(self, sources):
        @trace.tlong("unis.models.UnisCollection._add_subscription")
        def cb(v, action):
            if action in ['POST', 'PUT']:
                try:
                    schema = v['$schema'] = v.get('\\$schema', None) or v['$schema']
                    try: del v['\\$schema']
                    except KeyError: pass
                except KeyError as e:
                    raise ValueError("No schema in message from UNIS - {}".format(v)) from e
                model = schemaLoader.get_class(schema, raw=True)
                if action == 'POST':
                    resource = model(v)
                    changed, resource = self._validate_append(resource)
                    if not changed: return
                else:
                    try:
                        resource = self.get([v['selfRef']])[0]
                    except UnisReferenceError:
                        uid = urlparse(v['selfRef']).path.split('/')[-1]
                        cid = UnisClient.resolve(v['selfRef'])
                        self._proto_get_next([_rkey(uid, cid)])
                        try: resource = self.get([v['selfRef']])[0]
                        except UnisReferenceError: return
                    resource.__dict__['ts'] = v['ts']
                self.update(resource)
            elif action == 'DELETE':
                try:
                    i = self._indices['id'].index(v['id'])
                    res = self._cache[i]
                    self._remove_record(res)
                except CollectionIndexError as e:
                    logging.getLogger('unis.index').warn("No such element in UNIS to delete - {}".format(v['id']))
        if self._subscribe:
            await self._unis.subscribe(sources, cb)
    
    async def _from_unis(self, source, start=0, size=None, kwargs={}):
        kwargs.update({"skip": start, "limit": size})
        result = await self._unis.get([source], **kwargs)
        return result if isinstance(result, list) else [result]
    
    def __repr__(self):
        with self._lock:
            rep = ".{} {}".format(self.name, self._cache.__repr__() if self._cache and len(self._cache) < 4 else "[...]")
            return "<UnisList{}>".format(rep if hasattr(self, "name") else "")
    
    def __len__(self):
        with self._lock:
            return len(self._cache) + len([x for x,v in self._stubs.items() if isinstance(v, str)])
    
    def __contains__(self, item):
        with self._lock:
            return item in self._cache
    
    def __iter__(self):
        with self._lock:
            self._complete_cache()
            return iter(self._cache.valid_list())
