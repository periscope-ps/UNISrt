import bisect

from pubsub import Events

class Index(object):
    def __init__(self, key):
        self.key = key
        self._ls = []
    
    def index(self, item):
        for k,i in self._ls:
            if k == getattr(item, self.key, None):
                return i
        return None
    
    def subset(self, rel, v):
        keys = [item[0] for item in self._ls]
        values = [item[1] for item in self._ls]
        slices = {
            "gt": lambda: set(values[bisect.bisect_right(keys, v):]),
            "ge": lambda: set(values[bisect.bisect_left(keys, v):]),
            "lt": lambda: set(values[:bisect_left(keys, v)]),
            "le": lambda: set(values[:bisect_right(keys, v)]),
            "eq": lambda: set(values[bisect.bisect_left(keys, v):bisect.bisect_right(keys, v))
        }
        return slices[rel]()
        
    def update(self, i, item):
        try:
            self._ls = [ x for x in self._ls if x[1] != i ]
            v = getattr(item, self.key)
            self._ls.insert(bisect.bisect_left([x[0] for x in self._ls], v), (v, i))
        except ValueError:
            pass
        except AttributeError:
            pass
