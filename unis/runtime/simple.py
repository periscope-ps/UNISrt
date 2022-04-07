import requests, bson, json
from urllib.parse import urlparse
from functools import wraps

from unis.models.models import UnisObject

def convert_key(fn):
    @wraps(fn)
    def _wrap(self, key=None, *args, **kwargs):
        if isinstance(key, dict):
            if 'id' not in key: raise ValueError("Key must be of type 'str'")
            key = key['id']
        return fn(self, *args, urlparse(key).path.split('/')[-1], **kwargs)
    return _wrap

class SimpleCollection(object):
    def __init__(self, name, url):
        self.name, self.url = name, url
        self._cache, self._last = {}, 0
        self._live = True

    def _query(self):
        if self._live:
            docs = bson.loads(requests.get(f"{self.url}/{self.name}?ts=gt={self._last}", headers={"Accept":"application/perfsonar+bson"}).content)
            for d in docs.values():
                self._cache[d['id']] = d
                self._last = max(self._last, d.get('ts', 0))

    def insert(self, document):
        if isinstance(document, UnisObject): document = document.to_JSON()
        try: del document['ts']
        except ValueError: pass
        requests.post(f"{self.url}/{self.name}", json.dumps(document),
                      headers={'Content-Type': 'application/perfsonar+json'})
        self._query()

    @convert_key
    def update(self, key):
        document = self._cache[key]
        try: del document['ts']
        except ValueError: pass
        requests.post(f"{self.url}/{self.name}", json.dumps(document),
                      headers={'Content-Type': 'application/perfsonar+json'})
        self._query()

    def transaction(self):
        class _CollectionTransaction(object):
            def __init__(s): s.c = self
            def __enter__(self):
                self.c._query()
                self.c._live = False
                return self.c
            def __exit__(self):
                self.c._live = True
                self.c._query()

        return _CollectionTransaction(self)

    @convert_key
    def __getitem__(self, key):
        self._query()
        return self._cache[key]

    @convert_key
    def __delitem__(self, key):
        requests.delete(f"{url}/{self.name}/{key}")
        del self._cache[key]

    def __setitem__(self, key, document): raise NotImplementedError()
    
    def __iter__(self):
        self._query()
        for v in sorted(self._cache.values(), key=lambda x: x['ts']): yield v

    def __len__(self):
        return len(self._cache)
    
class SimpleCache(object):
    def __init__(self, url):
        self._cols = {}
        info = requests.get(url).json()
        for c in info:
            name = c['href'].split('/')[-1]
            self._cols[name] = SimpleCollection(name, url)
    def __getattr__(self, n):
        if n in self._cols: return self._cols[n]
        else: return super().__getattr__(n)
