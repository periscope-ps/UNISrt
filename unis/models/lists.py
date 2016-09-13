
import json
import types
import bisect

from unis.utils.pubsub import Events

class UnisCollection(object):
    def __init__(self, href, collection, model, runtime):
        self._cache = []
        self._indices = { "id": []}
        self._subscribed = False
        self._runtime = runtime
        self._href = href
        self._model = model
        self._rangeset = set()
        self.collection = collection
    def __repr__(self):
        return self._cache.__repr__()
    def __len__(self):
        return self._cache.__len__()
    def __getitem__(self, i):
        return self._cache[i]
    def __setitem__(self, i, obj):
        if type(obj) != self._model:
            raise TypeError("Resource not of correct type: got {t1}, expected {t2}".format(t1=self._model, t2=type(obj)))
        if self._cache[i].id != obj.id:
            raise AttributeError("Resource ids must match when setting to UnisCollections")
        
        tmpOld = self._cache[i]
        tmpOld.__dict__["selfRef"] = obj.selfRef
        if (not (getattr(tmpOld, "ts", None) and getattr(obj, "ts", None))) or tmpOld.ts < obj.ts:
            for k,v in obj.__dict__.items():
                tmpOld.__dict__[k] = v
            self._runtime._publish(Events.update, self._cache[i])
    def __iter__(self):
        if self._subscribed:
            return iter(self._cache)
        return ul_iter(self)
    def _indexitem(self, k, ls, item, index):
        v = (getattr(item, k), index) if getattr(item, k, None) else None
        if v:
            index_keys = [i[0] for i in ls]
            ls.insert(bisect.bisect_left(index_keys, v[0]), v)
    
    def append(self, obj):
        keys = [k for k, v in self._indices["id"]]
        i = bisect.bisect_left(keys, obj.id)
        if i < len(self._indices["id"]) and obj.id  == self._indices["id"][i][0]:
            self[self._indices["id"][i][1]] = obj
        else:
            obj._runtime = self._runtime
            obj._collection = self.collection
            obj.setDeferred(self._runtime.defer_update)
            if obj.remoteObject():
                obj.update()
            index = len(self._cache)
            self._rangeset.add(index)
            self._cache.append(obj)
            for k, ls in self._indices.items():
                self._indexitem(k, ls, obj, index)
            self._runtime._publish(Events.new, self._cache[index])
    
    def createIndex(self, k):
        ls = []
        self._indices[k] = ls
        for i, item in enumerate(self._cache):
            self._indexitem(k, ls, item, i)
    def updateIndex(self, item):
        def get_index():
            for key, i in self._indices["id"]:
                if key == item.id:
                    return i
            raise ValueError("Item does not exist in collection")
        
        i = get_index()
        for k, ls in self._indices.items():
            if k != "id":
                new_ls = list(filter(lambda x: x[1] != i, ls))
                self._indexitem(k, new_ls, item, i)
                self._indices[k] = new_ls
        self._runtime._publish(Events.update, self._cache[i])
    
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
            "eq": lambda x, ls: slice(bisect.bisect_left(keys, x), bisect.bisect_right(keys, x)) 
        }
        if isinstance(pred, types.FunctionType):
            for v in filter(pred, self):
                yield v
        elif isinstance(pred, dict):
            no_index = {}
            iterator = self
            if self._subscribed and use_index:
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
                if valid:
                    yield item
        else:
            raise ValueError("where expects function or dictionary predicate list - got {v}".format(v = type(pred)))
    
    # Subscribe to unis websocket to ensure {} query is valid
    def subscribe(self):
        def _callback(v):
            v = json.loads(v)
            resource = self._model(v["data"], self._runtime, True, self._runtime.defer_update, False)
            self.append(resource)
        
        if not self._subscribed:
            self._runtime._unis.subscribe(self.collection, _callback)
            self._subscribed = True
            


class ul_iter(object):
    def __init__(self, ls):
        self.ls = ls
        self.index = 0
        self.PAGE_SIZE = 100
        self.local_cache = []
        self.complete = {}
        self.local_done = False
    
    def __iter__(self):
        return self
    
    def next(self):
        return self.__next__()
    def __next__(self):
        # Return all locally stored values first
        if not self.local_done:
            if self.index < len(self.ls):
                result = self.ls[self.index]
                self.complete[result.id] = result
                self.index += 1
                return result
            else:
                index = 0

        # Could check for an empty local cache here, but results in an extra db hit at the end of the list
        if self.index % self.PAGE_SIZE == 0:
            self.local_cache = self.ls._runtime._unis.get(self.ls._href, limit = self.PAGE_SIZE, skip = self.index)
        if not self.local_cache:
            self.ls.subscribe()
            raise StopIteration()
            
        result = self.ls._model(self.local_cache.pop(0), self.ls._runtime, defer=self.ls._runtime.defer_update, local_only=False)
        self.index += 1
        if result.id in self.complete:
            return self.__next__()
        else:
            self.ls.append(result)
            self.complete[result.id] = result
            return result
