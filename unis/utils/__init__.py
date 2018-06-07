import bisect
import functools

from lace.logging import trace

from unis.utils.pubsub import Events

class Index(object):
    @trace.debug("Index")
    def __init__(self, key):
        self.key = key
        self._keys, self._indices, self._items = [], [], []
    
    @trace.info("Index")
    def index(self, item, value=False):
        try:
            v = item if value else getattr(item, self.key)
        except AttributeError:
            return None
        s = bisect.bisect_left(self._keys, v)
        e = bisect.bisect_right(self._keys, v)
        for i in range(s, e):
            a, b = self._items[i], item
            sr1, sr2 = getattr(a, 'selfRef', True), getattr(b, 'selfRef', False)
            if a.getObject() == b.getObject() or (sr1 and sr2 and sr1 == sr2):
                return self._indices[i]
    
    @trace.info("Index")
    def subset(self, rel, v):
        slices = {
            "gt": lambda v: set(self._indices[bisect.bisect_right(self._keys, v):]),
            "ge": lambda v: set(self._indices[bisect.bisect_left(self._keys, v):]),
            "lt": lambda v: set(self._indices[:bisect_left(self._keys, v)]),
            "le": lambda v: set(self._indices[:bisect_right(self._keys, v)]),
            "eq": lambda v: set(self._indices[bisect.bisect_left(self._keys, v):bisect.bisect_right(self._keys, v)]),
            "in": lambda v: set().union(**[slices['eq'](x) in v])
        }
        return slices[rel](v)
        
    @trace.info("Index")
    def update(self, index, item):
        i = functools.reduce(lambda a,b: b if b == index else a, self._indices, None)
        if i:
            self._keys.pop(i)
            self._indices.pop(i)
            self._items.pop(i)
        try:
            key = getattr(item, self.key)
        except AttributeError:
            return
        i = bisect.bisect_left(self._keys, key)
        self._keys.insert(i, key)
        self._indices.insert(i, index)
        self._items.insert(i, item)
