import json
import time
import types
import bisect
import uuid
import re

from unis.utils.pubsub import Events
from unis.models.models import schemaLoader
from unis import logging

class DataCollection(object):
    def __init__(self, source, runtime, subscribe=True, pull_history=False):
        def mean(x, ts, prior, state):
            state["sum"] += x
            state["count"] += 1
            return state["sum"] / state["count"]
        def jitter(x, ts, prior, state):
            state["count"] += 1
            delta = x - state["mean"]
            state["mean"] += delta / state["count"]
            return prior + (delta * (x - state["mean"]))
            
        re_str = "(?P<source>http[s]?://[^/])/metadata/(?P<mid>[^/])"
        groups = re.match(re_str, source)
        if not groups:
            raise ValueError("Bad href in metadata resource - {}".format(source))
        self.metadataId = groups.group('mid')
        self._source = groups.group('source')
        self._cache = []
        self._functions = {}
        self._at = 0 if pull_history else int(time.time() * 1000000)
        self._runtime = runtime
        self._ready = False
        
        if not subscribe:
            self._subscribe = lambda: False
        elif not pull_history:
            self._ready = self._subscribe()
        
        self.attachFunction("min", lambda x, ts, prior, state: x if isinstance(prior, type(None)) or prior > x else prior)
        self.attachFunction("max", lambda x, ts, prior, state: x if not prior or prior < x else prior)
        self.attachFunction("mean", mean, state={"sum": 0, "count": 0})
        self.attachFunction("jitter", jitter, 0, {"mean": 0, "count": 0}, lambda x, state: x / max(state["count"] - 1, 1))
        self.attachFunction("last", lambda x, ts, prior, state: x)
        
    def __len__(self):
        pass
    def __getitem__(self, key):
        pass
    def __setitem__(self, i, k):
        raise RuntimeError("Cannot set values to a data collection")
        
    def __getattribute__(self, n):
        if n != "_functions" and  n in self._functions:
            self.load()
            func, pp, meta = self._functions[n]
            return pp(*meta)
        return super(DataCollection, self).__getattribute__(n)
        
    @logging.info("DataCollection")
    def attachFunction(self, n, f, default=None, state={}, post_process=lambda x, s: x):
        self._functions[n] = (f, post_process, (default, state))
        
    @logging.info("DataCollection")
    def load(self):
        if not self._ready:
            for record in CollectionIterator(self, source=self._source, sort="ts:1", ts="gt={ts}".format(ts = self._at)):
                self._process(record)
            self._ready = self._subscribe()
            
    @logging.info("DataCollection")
    def _process(self, record):
        self._at = int(max(self._at, record["ts"]))
        for k, v in self._functions.items():
            func, pp, meta = v
            prior, state = meta
            value = func(record["value"], record["ts"], prior, state)
            self._functions[k] = (func, pp, (value, state))
    
    @logging.debug("DataCollection")
    def _subscribe(self):
        def _callback(v):
            for k, v in v["data"].items():
                for record in v:
                    self._process(record)
            
        self._runtime._unis.subscribe("data/{i}".format(i=self.metadataId), _callback, self.source)
        self._subscribe = lambda: True
        return True
    


class UnisCollection(object):
    @logging.debug("UnisCollection")
    def __init__(self, href, collection, model, runtime, auto_sync=True, subscribe=True):
        self._cache = []
        self._services = []
        self._indices = { "id": []}
        self._runtime = runtime
        self._model = model
        self._rangeset = set()
        self._do_sync = auto_sync
        self._full = False
        self.collection = collection
        self.locked = False
        
        if not subscribe:
            self._subscribe = lambda: None
        
    def __repr__(self):
        if getattr(self, '_full', False):
            tmpOut = []
            for item in self:
                tmpOut.append(item)
            return tmpOut.__repr__()
        else:
            return "<UnisList.{} pending[...]>".format(getattr(self, 'collection', '<no-name>'))
    def __len__(self):
        return self._cache.__len__()
    def __getitem__(self, i):
        if not self._full and self._do_sync:
            self.sync()
        return self._cache[i]
    def __setitem__(self, i, obj):
        if self._model._schema["name"] not in obj.names:
            raise TypeError("Resource not of correct type: got {t1}, expected {t2}".format(t1=self._model, t2=type(obj)))
        if self._cache[i].id != obj.id:
            raise AttributeError("Resource ids must match when setting to UnisCollections")
        
        tmpOld = self._cache[i]
        tmpOld.__dict__["selfRef"] = obj.selfRef
        if (not (getattr(tmpOld, "ts", None) and getattr(obj, "ts", None))) or tmpOld.ts < obj.ts:
            for k,v in obj.__dict__.items():
                tmpOld.__dict__[k] = v
            self._serve(Events.update, self._cache[i])
        #else:
        #    raise ValueError("Attempted to insert an older object than current version into collection")
    def __iter__(self):
        if not self._do_sync or self._full:
            return iter(self._cache)
        return MixedCollectionIterator(self)
    def __contains__(self, item):
        return item in self._cache
        
    @logging.debug("UnisCollection")
    def _indexitem(self, k, ls, item, index):
        v = (getattr(item, k), index) if getattr(item, k, None) else None
        if v:
            index_keys = [i[0] for i in ls]
            ls.insert(bisect.bisect_left(index_keys, v[0]), v)
        
    @logging.debug("UnisCollection")
    def _serve(self, ty, resource):
        for service in self._services:
            f = getattr(service, ty.name)
            f(resource)
    
    @logging.info("UnisCollection")
    def append(self, obj):
        if obj._runtime and obj._runtime != self._runtime:
            raise ValueError("Resource already belongs to another runtime")
        if self._model._schema["name"] not in obj.names:
            raise TypeError("Resource not of correct type: got {t1}, expected {t2}".format(t1=self._model, t2=type(obj)))
            
        obj._runtime = self._runtime
        obj.setCollection(self.collection)
        
        if obj.remoteObject():
            obj.update()
        
        if getattr(obj, "id", None):
            keys = [k for k, v in self._indices["id"]]
            index = bisect.bisect_left(keys, obj.id)
            if index < len(self._indices["id"]) and obj.id  == self._indices["id"][index][0]:
                self[self._indices["id"][index][1]] = obj
                return
        
        index = len(self._cache)
        self._rangeset.add(index)
        self._cache.append(obj)
        self._serve(Events.new, obj)
        
        for k, ls in self._indices.items():
            self._indexitem(k, ls, obj, index)
    
    @logging.info("UnisCollection")
    def addService(self, service):
        self._services.append(service)
    
    @logging.info("UnisCollection")
    def createIndex(self, k):
        ls = []
        self._indices[k] = ls
        for i, item in enumerate(self._cache):
            self._indexitem(k, ls, item, i)
    @logging.info("UnisCollection")
    def updateIndex(self, item):
        def get_index():
            for i, v in enumerate(self._cache):
                if v == item:
                    return i
            raise ValueError("Item does not exist in collection")
            
        i = get_index()
        for k, ls in self._indices.items():
            new_ls = list(filter(lambda x: x[1] != i, ls))
            self._indexitem(k, new_ls, item, i)
            self._indices[k] = new_ls
        self._serve(Events.update, self._cache[i])
    @logging.info("UnisCollection")
    def hasValue(self, k, v):
        if k in self._indices:
            keys = [item[0] for item in self._indices[k]]
            slices = slice(bisect.bisect_left(keys, v), bisect.bisect_right(keys, v))
            return len(self._indices[k][slices]) > 0
        else:
            raise ValueError("No key {k} in collection".format(k=k))
    
    @logging.info("UnisCollection")
    def where(self, pred, use_index = True):
        funcs = {
            "lt": lambda a, b: a < b, "le": lambda a, b: a <= b,
            "gt": lambda a, b: a > b, "ge": lambda a, b: a >= b,
            "eq": lambda a, b: a == b
        }
        slices = { 
            "lt": lambda x, ls: slice(0, bisect.bisect_left(ls, x)),
            "le": lambda x, ls: slice(0, bisect.bisect_right(ls, x)),
            "gt": lambda x, ls: slice(bisect.bisect_right(ls, x), len(ls)),
            "ge": lambda x, ls: slice(bisect.bisect_left(ls, x), len(ls)),
            "eq": lambda x, ls: slice(bisect.bisect_left(ls, x), bisect.bisect_right(ls, x)) 
        }
        if isinstance(pred, types.FunctionType):
            for v in filter(pred, self):
                yield v
        elif isinstance(pred, dict):
            no_index = {}
            iterator = self
            
            if not self._do_sync:
                iterator = MixedCollectionIterator(self, force=True, **pred)
                for v in iterator:
                    yield v
            else:
                if self._full and use_index:
                    sets = [self._rangeset]
                    for k, v in pred.items():
                        if not isinstance(v, dict):
                            v = { "eq": v }
                        
                        if k in self._indices:
                            keys = [item[0] for item in self._indices[k]]
                            for k2, v2 in v.items():
                                func = slices[k2]
                                sets.append(set([item[1] for item in self._indices[k][func(v2, keys)]]))
                        else:
                            no_index[k] = v
                    iterator = (self[i] for i in set.intersection(*sets))
                else:
                    for k, v in pred.items():
                        if not isinstance(v, dict):
                            pred[k] = {"eq": v}
                    no_index = pred
                
                for item in iterator:
                    valid = True
                    for k, v in no_index.items():
                        for k2, v2 in v.items():
                            p = funcs[k2]
                            if not p(getattr(item, k, None), v2):
                                valid = False
                                break
                        if not valid:
                            break
                    if valid:
                        yield item
        else:
            raise ValueError("where expects function or dictionary predicate list - got {v}".format(v = type(pred)))
    
    # Subscribe to unis websocket to ensure {} query is valid
    @logging.debug("UnisCollection")
    def _subscribe(self):
        def _callback(v):
            if "\\$schema" in v.get("data", {}):
                model = schemaLoader.get_class(v["data"]["\\$schema"])
            else:
                raise ValueError("Bad message from UNIS")
            resource = model(v["data"], self._runtime, local_only=False)
            try:
                while self.locked:
                    pass
                self.append(resource)
            except ValueError:
                pass
        
        self._runtime._unis.subscribe(self.collection, _callback)
        self._subscribe = lambda: None
    
    @logging.debug("UnisCollection")
    def _fromId(self, uid):
        keys = [item[0] for item in self._indices["id"]]
        index = bisect.bisect_left(keys, uid)
        if index < len(self._indices["id"]):
            return self[self._indices["id"][index][1]]
        else:
            raise KeyError("No object with id {}".format(uid))
        
        
    @logging.info("UnisCollection")
    def sync(self):
        for i in MixedCollectionIterator(self, force=True):
            pass
        
        
class CollectionIterator(object):
    @logging.debug("CollectionIterator")
    def __init__(self, ls, source=None, **kwargs):
        self.ls = ls
        self.index = 0
        self.local_cache = []
        self.kwargs = kwargs
        if source:
            self.url = "{}/{}".format(source, ls.collection)
        else:
            self.url = "#/{}".format(ls.collection)
        self.limit = 100
        
    @logging.debug("CollectionIterator")
    def __iter__(self):
        return self
    def next(self):
        return self.__next__()
    @logging.debug("CollectionIterator")
    def __next__(self):
        resource = self._get_next()
        result =  self.processResult(resource)
        return result
    
    @logging.debug("CollectionIterator")
    def _get_next(self):
        if self.index % self.limit == 0:
            self.kwargs['skip'] = self.index
            self.local_cache = self.ls._runtime._unis.get(self.url, limit=self.limit, kwargs=self.kwargs)
        if not self.local_cache:
            self.finalize()
        
        self.index += 1
        return self.local_cache.pop(0)
        
    @logging.debug("CollectionIterator")
    def finalize(self):
        raise StopIteration()
    
    @logging.debug("CollectionIterator")
    def processResult(self, res):
        return res


class MixedCollectionIterator(CollectionIterator):
    @logging.debug("MxiedIterator")
    def __init__(self, ls, force=False, **kwargs):
        self.local_done = False
        self._seen = set()
        super(MixedCollectionIterator, self).__init__(ls, **kwargs)
        self._complete = not (force or self.ls._do_sync)
        self._store_sync = self.ls._do_sync
        self.ls._do_sync = False
    
    @logging.debug("MxiedIterator")
    def __next__(self):
        # Return all locally stored values first
        if not self.local_done:
            if self.index < len(self.ls):
                result = self.ls[self.index]
                self.index += 1
                self._seen.add(getattr(result, "id", None))
                return result
            else:
                self.local_done = True
                self.index = 0
        
        if not self._complete:
            return super(MixedCollectionIterator, self).__next__()
        raise RuntimeError("Iterator outside object bounds")
    
    @logging.debug("MxiedIterator")
    def finalize(self):
        self._complete = True
        self.ls._full = True
        self.ls._do_sync = self._store_sync
        self.ls._subscribe()
        super(MixedCollectionIterator, self).finalize()
        
    @logging.debug("MxiedIterator")
    def processResult(self, res):
        while True:
            if "$schema" in res:
                model = schemaLoader.get_class(res["$schema"])
            else:
                raise TypeError("Bad resource from UNIS")
            result = model(res, self.ls._runtime, local_only=False)
            if result.id in self._seen:
                res = self._get_next()
            else:
                self.ls.append(result)
                self._seen.add(result.id)
                return result
