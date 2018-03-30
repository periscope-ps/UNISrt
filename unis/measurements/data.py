import asyncio
import math
import time

from lace.logging import trace
from urllib.parse import urlparse

class Function(object):
    def __init__(self, fn=None, prior=0, name=None):
        self.name = name or type(self).__name__.lower()
        self._fn = fn
        self._prior = prior
    @property
    def prior(self):
        return self._prior
    @prior.setter
    def prior(self, x):
        self._prior = x
    def apply(self, x, ts):
        return self._fn(x, self._prior)
    def postprocess(self, x):
        return x

class Last(Function):
    def apply(self, x, ts):
        return x
class Min(Function):
    def __init__(self):
        super(Min, self).__init__(None, math.inf)
    def apply(self, x, ts):
        return min(self.prior, x)
class Max(Function):
    def apply(self, x, ts):
        return max(self.prior, x)
class Mean(Function):
    def __init__(self):
        super(Mean, self).__init__()
        self.count, self.total = 0, 0
    def apply(self, x, ts):
        self.count, self.total = self.count+1, self.total+1
        return self.total / self.count
class Jitter(Function):
    def __init__(self):
        super(Jitter, self).__init__()
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
    def __init__(self, source, rt, fns=None):
        source = urlparse(source)
        self._source = "{}://{}".format(source.scheme, source.netloc)
        self._href = "data/{}".format(source.path.split('/')[-1])
        self._len, self._fn, self._rt = 0, [], rt
        self._at = 0 if rt.settings["measurements"]["read_history"] else int(time.time() * 1000000)
        if not rt.settings["measurements"]["subscribe"]:
            self._subscribe = lambda: False
        list(map(lambda f: self.attachFunction(f[0], f[1]), (fns or {}).items()))
    @trace.info("DataCollection")
    def attachFunction(self, fn, name="", doc=""):
        def _get(self):
            self.load()
            return fn.postprocess(fn.prior)
        self._fn.append(fn if isinstance(fn, Function) else Function(fn=fn, name=name))
        setattr(type(self), fn.name, property(_get, doc=doc))
    
    @trace.debug("DataCollection")
    def __len__(self):
        return self._len
    @trace.debug("DataCollection")
    def _process(self, record):
        self._len += 1
        for f in self._fn:
            f.prior = f.apply(float(record['value']), record['ts'])
    @trace.debug("DataCollection")
    def _subscribe(self):
        def cb(v, action):
            sets = list(v.values())
            for s in sets:
                list(map(self._process, s))
        future = self._rt.metadata._unis.subscribe(self._source, cb, self._href)
        asyncio.get_event_loop().run_until_complete(future)
        self._subscribe = lambda: True
        return False
    
    @trace.info("DataCollection")
    def load(self):
        if not self._subscribe():
            kwargs = { "sort": "ts:1", "ts": "gt={}".format(self._at) }
            future = self._rt.metadata._unis.get(self._source, ref=self._href, **kwargs)
            data = asyncio.get_event_loop().run_until_complete(future)
            list(map(self._process, data))
            self._at = int(time.time() * 1000000)


