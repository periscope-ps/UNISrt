import aiohttp, asyncio, functools, bson

from mundus.exceptions import ConnectionError
from mundus import options

from urllib.parse import urlparse, urlunparse, urlencode

def _session_raises(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        if Client.session is None:
            raise ConnectionError("Client has been closed, cannot make new connections")
        return fn(*args, **kwargs)
    return wrapper

class Client(object):
    session = None

    def __init__(self, url):
        if Client.session is None:
            asyncio.get_event_loop().run_until_complete(self.start_session())
        self.url = urlparse(url)

    def _listify(self, raw):
        try:
            return [raw[str(i)] for i in range(len(raw))]
        except KeyError:
            return raw
    def _dictify(self, raw):
        if isinstance(raw, list):
            return {str(i):raw[i] for i in range(len(raw))}
        else:
            return raw

    def check_ws(self):
        pass

    @_session_raises
    async def get(self, path, **kwargs):
        url = urlunparse((*(self.url[:2]), path, '', urlencode(kwargs), ''))
        async with self.session.get(url) as resp:
            result = self._listify(bson.loads(await resp.read()))
            await asyncio.sleep(0)
            return result

    @_session_raises
    async def post(self, path, data):
        url = urlunparse((*(self.url[:2]), path, '', '', ''))
        data = bson.dumps(self._dictify(data))
        async with self.session.post(url, data=data) as resp:
            return self._listify(bson.loads(await resp.read()))

    @_session_raises
    async def put(self, path, data):
        url = urlunparse((*(self.url[:2]), path, '', '', ''))
        data = bson.dumps(self._dictify(data))
        async with self.session.put(url, data=data) as resp:
            return self._listify(bson.loads(await resp.read()))

    @_session_raises
    async def delete(self, path):
        url = urlunparse((*(self.url[:2]), path, '', '', ''))
        async with self.session.delete(url) as resp:
            return self._listify(bson.loads(await resp.read()))

    async def close(self):
        pass

    @classmethod
    async def start_session(cls):
        timeout = aiohttp.ClientTimeout(total=options.get("conn.timeout"))
        headers = {"Accept": "application/bson", "Content-Type": "application/bson"}
        cls.session = aiohttp.ClientSession(headers=headers, timeout=timeout)
    @classmethod
    async def close_session(cls):
        if cls.session is not None:
            await cls.session.close()
        await asyncio.sleep(0)
        cls.session = None
