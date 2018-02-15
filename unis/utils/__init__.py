import bisect
import functools

from lace.logging import trace

from unis.utils.pubsub import Events
from unis.models.models import UnisObject

class Index(object):
    @trace.debug("Index")
    def __init__(self, key):
        self.key = key
        self._keys, self._indices, self._items = [], [], []
    
    @trace.info("Index")
    def index(self, item):
        try:
            v = getattr(item, self.key) if isinstance(item, UnisObject) else item
        except AttributeError:
            return None
        s = bisect.bisect_left(self._keys, v)
        
        for i in range(s, len(self._keys)):
            a, b = self._items[i], item
            if a.getObject() == b.getObject() or getattr(a, 'selfRef', True) == getattr(b, 'selfRef', False):
                return self._indices[i]
    
    @trace.info("Index")
    def subset(self, rel, v):
        slices = {
            "gt": lambda: set(self._indices[bisect.bisect_right(self._keys, v):]),
            "ge": lambda: set(self._indices[bisect.bisect_left(self._keys, v):]),
            "lt": lambda: set(self._indices[:bisect_left(self._keys, v)]),
            "le": lambda: set(self._indices[:bisect_right(self._keys, v)]),
            "eq": lambda: set(self._indices[bisect.bisect_left(self._keys, v):bisect.bisect_right(self._keys, v)])
        }
        return slices[rel]()
        
    @trace.info("Index")
    def update(self, index, item):
        i = functools.reduce(lambda a,b: b if b == index else a, self._indices, None)
        if i:
            self._keys.pop(i)
            self._indices.pop(i)
            self._items.pop(i)
        key = getattr(item, self.key, None)
        i = bisect.bisect_left(self._keys, key)
        self._keys.insert(i, key)
        self._indices.insert(i, index)
        self._items.insert(i, item)
