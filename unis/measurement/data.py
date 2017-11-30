import asyncio
import math
import time

from lace.logging import trace

class Function(object):
    def __init__(self, fn=None, prior=0):
        self._fn = fn
        self._prior = prior
    @property
    def prior(self, x):
        return self._prior
    def apply(self, x, ts):
        return self._fn(x, self._prior)
    def postprocess(self, x):
        return x

class Last(Function):
    def apply(self, x, ts):
        return x
class Min(Function):
    def apply(self, x, ts):
        return min(self.prior, x)
class Max(Function):
    def apply(self, x, ts):
        return max(self.prior, x)
class Mean(Function):
    def __init__(self):
        self.count, self.total = 0, 0
    def apply(self, x, ts):
        self.count, self.total = self.count+1, self.total+1
        return self.total / self.count
class Jitter(Function):
    def __init__(self):
        self.count, self.mean = 0, 0
    def apply(self, x, ts):
        self.count += 1
        delta = x - self.mean
        self.mean += delta / self.count
        return self.prior + (delta * (x - self.mean))
    def postprocess(self, x):
        return x / max(self.count - 1, 1)

class DataCollection(object):
    @trace.debug("DataCollection")
    def __init__(self, mid, rt, fns=None):
        self._len, self._href, self._fn, self._rt = 0, "data/{}".format(mid), [], rt
        self._at = 0 if rt.settings["measurements"]["read_history"] else int(time.time() * 1000000)
        if not rt.settings["measurements"]["subscribe"]:
            self._subscribe = lambda s: False
        list(map(lambda f: self.attachFunction(f[0], f[1]), (fns or {}).items()))
    @trace.info("DataCollection")
    def attachFunction(self, n, fn, doc=""):
        def _get(self):
            self.load()
            return fn.postprocess(fn.prior)
        if not isinstance(fn, Function):
            fn = Function(fn=fn)
        self._fn.append(fn)
        setattr(self, n, property(_get, doc=doc))
        
    @trace.debug("DataCollection")
    def __len__(self):
        return self._len
    @trace.debug("DataCollection")
    def __getattribute__(self, n):
        return super(DataCollection, self).__getattribute__(n)
    @trace.debug("DataCollection")
    def _process(self, record):
        self._len += 1
        for f in self._fn:
            f._prior = f.apply(record['value'], record['ts'])
    @trace.debug("DataCollection")
    def _subscribe(self):
        def cb(v):
            list(map(self._process, v['data'].values()))
        asyncio.get_event_loop().run_until_complete(self._rt._unis.subscribe(self._href, cb))
        self._subscribe = lambda: True
    
    @trace.info("DataCollection")
    def load(self):
        if self._subscribe():
            kwargs = { "sort": "ts:1", "ts": "gt={}".format(self._at) }
            list(map(self._process, self._rt._unis.get(self._href, None, kwargs=kwargs)))
            self._at = int(time.time() * 1000000)


