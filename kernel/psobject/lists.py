import json
import types

from kernel.psobject import factory
from kernel.psobject.objects import UnisObject

class UnisCollection(list):
    def __init__(self, href, collection, schema, runtime):
        self.__cache__ = {}
        self._queries = []
        self._subscribed = False
        self._runtime = runtime
        self._href = href
        self._schema = schema
        self.collection = collection
        self.subscribe()
        
    # where(p) accepts either a function or a dict containing key/value pairs to compare to elements
    # in the mongodb style.  In the case of the dict, where first attempts to find a less expressive
    # query to use as a basis and filters that query.  Failing that, it will iterate through the
    # unis backend store filling in the cache as it goes.  In the case of a function or lambda,
    # where applies the function to the unis backend store.
    def where(self, pred):
        if isinstance(pred, types.FunctionType):
            for v in filter(pred, self):
                yield v
        elif isinstance(pred, dict):
            query = None
            tmpResults = []
            for q in self._queries:
                query = q["matches"]
                for k, v in q["predicate"].items():
                    if pred.get(k, None) != v:
                        # query is too precise, discard
                        query = None
            
            if not query:
                for v in ul_iter(self, **pred):
                    yield v
            else:
                for v in query:
                    member = True
                    for k, val in pred.items():
                        if getattr(v, k, None) != val:
                            member = False
                    if member:
                        tmpResults.append(v)
                        yield v
            self._queries.append({ "predicate": pred, "matches": tmpResults})
        else:
            raise ValueError("where expects function or dictionary predicate list - got {v}".format(v = type(pred)))
    
    
    # Future proofing function for more complex caching schemes
    def collect_garbage(self):
        pass
    
    # Subscribe to unis websocket to ensure {} query is valid
    def subscribe(self):
        if not self._subscribed:
            self._runtime._unis.subscribe(self.collection, self._callback)
    def _callback(self, v):
        v = json.loads(v)
        resource = UnisObject(v["data"], self._runtime, False, False)
        self[resource.id] = resource
        
    def __len__(self):
        return len(self.__cache__.values())
        
    def __getitem__(self, key):
        if key not in self.__cache__:
            self._runtime.find("#/{c}/{k}".format(c = self.collection, k = key))
        return self.__cache__[key]
    
    def __setitem__(self, key, obj):
        factory.validate(obj, self._schema)
        if not factory.remoteObject(obj):
            raise AttributeError("UnisObject must not be Virtual and must contain a valid schema")
        if key in self.__cache__:
            tmpOld = self.__cache__[key]
            if tmpOld.ts < obj.ts:
                for k,v in obj.__dict__.items():
                    tmpOld.__dict__[k] = v
        else:
            self.collect_garbage()
            obj.update()
            self.__cache__[key] = obj
        
    def __delitem__(self, key):
        toremove = []
        for q in self._queries:
            if q["predicate"] == {}:
                toremove.append(q)
            else:
                for o in q["matches"]:
                    if o.id == key:
                        toremove.append(q)
        for q in toremove:
            self._queries.remove(q)
        
        del self.__cache__[key]
    
    def __contains__(self, item):
        if isinstance(item, UnisObject):
            return item.id in self.__cache__
        else:
            return item in self.__cache__
    
    def __iter__(self):
        for q in self._queries:
            if q["predicate"] == {}:
                return iter(self.__cache__.values())
        return ul_iter(self)

class ul_iter(object):
    def __init__(self, ls, **kwargs):
        self.ls = ls
        self.index = 0
        self.PAGE_SIZE = 100
        self.local_cache = []
        self.matches = []
        self.complete = {}
        self.query = kwargs
    
    def __iter__(self):
        return self
    
    def next(self):
        return self.__next__()
    def __next__(self):
        # Could check for an empty local cache here, but results in an extra db hit at the end of the list
        if self.index % self.PAGE_SIZE == 0:
            self.local_cache = self.ls._runtime._unis.get(self.ls._href, limit = self.PAGE_SIZE, skip = self.index, **self.query)
        if not self.local_cache:
            if not self.query and not {} in self.ls._queries:
                self.ls._queries.append({"predicate": {}, "matches": self.matches})
            raise StopIteration()
            
        result = UnisObject(self.local_cache.pop(0), self.ls._runtime, False, False)
        self.index += 1
        if result.id in self.complete:
            return self.__next__()
        else:
            self.ls[result.id] = result
            self.matches.append(factory.reference(result))
            self.complete[result.id] = result.ts
            return result
