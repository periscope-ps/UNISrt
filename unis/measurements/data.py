import asyncio
import math
import time

from unis.rest import UnisProxy
from unis.utils import async

from collections import defaultdict
from lace.logging import trace
from threading import Timer
from urllib.parse import urlparse

class Function(object):
    """
    :param callable fn: (optional) callback to use in place of apply.
    :param number initial: (optional) The initial value of the streaming result.
    :param str name: (optional) The name of the function.
    
    Create a :class:`Function <unis.measurements.data.Function>`.  :class:`Functions <unis.measurements.data.Function>`
    convert standard callback style functions into streaming functions.  ``fn`` must expose the following 
    interface:
    
        **parameters:** 
    
        * **value:** The new reading from the measurement.
        * **prior:** The most recent previous result (starts with ``initial``).
    
        **returns:** Number value result of the computation.
    """
    def __init__(self, fn=None, initial=0, name=None):
        self.name = name or type(self).__name__.lower()
        self._fn = fn
        self._prior = initial
    @property
    def prior(self):
        """
        :returns: Integer value of the previous result.
        
        Getter for the previous computation result.
        """
        return self._prior
    @prior.setter
    def prior(self, x):
        """
        :param number x: Value from the previous computation
        
        Setter for the previous computation result.
        """
        self._prior = x
    def apply(self, x, ts):
        """
        :param number x: The new reading from the measurement.
        :param int ts: The timestamp of the new reading.
        :returns: Number value of the streaming computation.
        
        Apply the computation to the value recieved from the stream.
        """
        return self._fn(x, self._prior)
    def postprocess(self, x):
        """
        :param number x: The most recently computed result.
        :returns: A non-streaming result for a compution.
        
        Some streaming computations require a final analysis to be done
        on the value when returning a result which does not change the
        actual value of the streaming result.
        
        Overriding :meth:`Function.postprocess <unis.measurements.data.Function.postprocess>`
        allows for this type of stream postprocessing.
        """
        return x

class Last(Function):
    """
    Return the last measurement as is, discard previous measurement when
    a new one is recieved.
    """
    def apply(self, x, ts):
        return x
class Min(Function):
    """
    Return the minimum value seen so far from the stream.
    """
    def __init__(self):
        super(Min, self).__init__(None, math.inf)
    def apply(self, x, ts):
        return min(self.prior, x)
class Max(Function):
    """
    Return the maximum value seen so far from the stream.
    """
    def apply(self, x, ts):
        return max(self.prior, x)
class Mean(Function):
    """
    Return the streaming mean value of the data.
    """
    def __init__(self):
        super(Mean, self).__init__()
        self.count, self.total = 0, 0
    def apply(self, x, ts):
        self.count, self.total = self.count+1, self.total+1
        return self.total / self.count
class Jitter(Function):
    """
    Returns the jitter of the stream.
    """
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
    """ 
    :param str source: href pointing to the data source to analyze.
    :param rt: Instance owning this measurement.
    :param fns: List of initial functions.
    :type rt: :class:`Runtime <unis.runtime.runtime.Runtime>`
    :type fns: list[callable or :class:`Function <unis.measurements.data.Function>`]
    
    Collection of measurement values from a specific remote data source.
    """
    @trace.debug("DataCollection")
    def __init__(self, md, rt, fns=None):
        self._source = md.getSource()
        self._unis = UnisProxy("data/{}".format(md.id))
        self._mid = md.id
        self._len, self._fns, self._rt = 0, {}, rt
        self._at = 0 if rt.settings["measurements"]["read_history"] else int(time.time() * 1000000)
        self._pending = []
        self._batch = int(rt.settings["measurements"].get("batch_size", 0))
        self._batch_delay = int(rt.settings["measurements"].get("batch_until", 0))
        self._timer = None
        self.read_only = False
        
        if not rt.settings["measurements"]["subscribe"]:
            self._subscribe = lambda: False
        list(map(lambda f: self.attachFunction(f[0], f[1]), (fns or {}).items()))

    def __getattr__(self, n):
        if n in self._fns.keys():
            self.load()
            return self._fns[n].postprocess(self._fns[n].prior)
        return super().__getattr__(n)
        
    @trace.info("DataCollection")
    def append(self, val, ts=0):
        """
        :param Any val: The value to append to the measurement
        :param int ts: The timestamp of the measurement in microseconds

        Add a data point to the corresponding measurement.
        """
        def cb():
            if self._pending:
                self._push()
            self._timer = None

        if self.read_only:
            raise AttributeError("Dataset is read only until measurement is flushed")
        self._pending.append({'ts': ts or int(time.time() * 1000000), 'value': val})
        if len(self._pending) >= self._batch:
            self._push()
        elif self._batch_delay and not self._timer:
            self._timer = Timer(self._batch_delay / 1000.0, cb)

    @trace.info("DataCollection")
    def _push(self):
        data = {'mid': self._mid, 'data': self._pending}
        self._unis.post({(self._source, self._unis._name): [data]})
        self._pending = []
        
    @trace.info("DataCollection")
    def attachFunction(self, fn, name="", doc=""):
        """
        :param fn: Function to attach to the data stream.
        :param str name: (optional) New attribute name to associate with the function.
        :param str doc: (optional) Document string for the attribute.
        
        Attach a function to the data stream.
        """
        fn = fn if isinstance(fn, Function) else Function(fn=fn, name=name)
        self._fns[fn.name] = fn
    
    @trace.debug("DataCollection")
    def __len__(self):
        return self._len
    @trace.debug("DataCollection")
    def _process(self, record):
        self._len += 1
        self._at = max(self._at, int(record['ts']))
        for f in self._fns.values():
            f.prior = f.apply(float(record['value']), record['ts'])
    @trace.debug("DataCollection")
    def _subscribe(self):
        def cb(v, action):
            sets = list(v.values())
            for s in sets:
                list(map(self._process, s))
        async.make_async(self._unis.subscribe, [self._source], cb)
        self._subscribe = lambda: True
        return False
    
    @trace.info("DataCollection")
    def load(self):
        if not self._subscribe():
            kwargs = { "sort": "ts:1", "ts": "gt={}".format(self._at) }
            data = async.make_async(self._unis.get, [self._source], **kwargs)
            list(map(self._process, data))

