import time

from unis import config

from unis import threads

# TODO: Handle disconnections with remote
#       Do we *need* a specialized datacollection? Can we just modify collections to work?

class DataCollection(object):
    def __init__(self, md, registered=True):
        s = config.Configuration()
        self._md, self.registered = md, registered
        self._at = 0 if s.meas_read_hist else int(time.time() * 1000000)
        self._len, self._fns = 0, {}
        self._pending, self._batch = [], s.meas_batch_post
        self._timer, self._alive = None, s.meas_live

    def load(self):
        def _event(col, data, action):
            sets = list(data.values())
            [self._process(v) for s in sets for v in s]
        if not self._alive:
            p, c = f"data/{md.id}", md.get_container()._client
            c.subscribe(p, _event)
        q = f"sort=ts:1&ts=gt={self._at}"
        [self._process(v) for v in c.get(p, query=q)]
        self._alive = True
        
