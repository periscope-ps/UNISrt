import asyncio
import aiohttp, requests, websockets
import bson, json
import itertools

from collections import defaultdict
from urllib.parse import urljoin, urlparse
from lace.logging import trace

from unis.settings import MIME

class UnisError(Exception):
    pass
class UnisReferenceError(UnisError):
    def __init__(self, msg, href):
        super(UnisReferenceError, self).__init__(msg)
        self.href = href
class ConnectionError(UnisError):
    def __init__(self, msg, code):
        super(ConnectionError, self).__init__(msg)
        self.status = code

class ReferenceDict(dict):
    def __getitem__(self, n):
        try:
            return super(ReferenceDict, self).__getitem__(n)
        except (KeyError, IndexError):
            raise UnisReferenceError("No unis instance at requested location - {}".format(n), n)

loop = asyncio.new_event_loop()
class UnisProxy(object):
    @trace.debug("UnisProxy")
    def __init__(self, collection):
        self.clients = ReferenceDict()
        self._name = collection
        if not loop.is_running():
            asyncio.get_event_loop().run_in_executor(None, loop.run_forever)
    
    @trace.info("UnisProxy")
    def shutdown(self):
        async def close():
            list(map(lambda t: t.cancel(), [t for t in asyncio.Task.all_tasks(loop) if t != asyncio.Task.current_task(loop)]))
            await asyncio.sleep(0.1)
            loop.stop()
        asyncio.run_coroutine_threadsafe(close(), loop)
        asyncio.get_event_loop().run_until_complete(asyncio.gather(*[c.shutdown() for c in self.clients.values()]))
    
    @trace.info("UnisProxy")
    def refToUID(self, source, full=True):
        url = urlparse(source)
        if len(url.path.split('/')) != 3 and full:
            raise ValueError("Cannot convert reference to UUID:collection:id tuple - {}".format(source))
        uid = UnisClient.fqdns[url.netloc]
        return uid, tuple(filter(lambda x: x, url.path.split('/')[-2:]))
        
    @trace.info("UnisProxy")
    def addSources(self, sources):
        new = []
        for s in sources:
            client = UnisClient(loop=loop, **s)
            if client.uid not in self.clients:
                new.append(s['url'])
                self.clients[client.uid] = client
        return new
    @trace.info("UnisProxy")
    async def getResources(self, source=None):
        return await self._gather(self._collect_funcs(source, "getResources"))
    
    @trace.info("UnisProxy")
    async def getStubs(self, source):
        return await self._gather(self._collect_funcs(source, "getStubs"), self._name)
        
    @trace.info("UnisProxy")
    async def get(self, source=None, ref=None, **kwargs):
        return await self._gather(self._collect_funcs(source, "get"), ref or self._name, **kwargs)
    
    @trace.info("UnisProxy")
    async def post(self, resources):
        msgs = defaultdict(list)
        for r in resources:
            msgs[self.refToUID(r.getSource(), False)[0]].append(r.to_JSON())
        results = await asyncio.gather(*[self.clients[k].post(self._name, json.dumps(v)) for k,v in msgs.items()])
        return list(itertools.chain(*[list(r) for r in results]))
    
    @trace.info("UnisProxy")
    async def put(self, href, data):
        source = self.refToUID(href)
        return await self.clients[source[0]].put("/".join(source[1]), data)
    
    @trace.info("UnisProxy")
    async def delete(self, data):
        source = self.refToUID(data.selfRef)
        return await self.clients[source[0]].delete("/".join(source[1]), data.to_JSON())
    
    @trace.info("UnisProxy")
    async def subscribe(self, source, callback, ref=None):
        return await self._gather(self._collect_funcs(source, "subscribe"), ref or self._name, callback)
        
    @trace.debug("UnisProxy")
    def _collect_funcs(self, source, n):
        if source:
            source = source if isinstance(source, list) else [source]
            source = [self.clients[(s if isinstance(s, tuple) else self.refToUID(s, False))[0]] for s in source]
            return [getattr(s, n) for s in source]
        return [getattr(c, n) for c in self.clients.values()]
    
    @trace.debug("UnisProxy")
    async def _gather(self, funcs, *args, **kwargs):
        results = await asyncio.gather(*[f(*args, **kwargs) for f in funcs])
        return list(itertools.chain(*results))
    

class _SingletonOnUUID(type):
    fqdns, instances = ReferenceDict(), {}
    def __init__(self, *args, **kwargs):
        return super(_SingletonOnUUID, self).__init__(*args, **kwargs)
    def __call__(cls, url, **kwargs):
        if not hasattr(cls, '_session'):
            cls._session, cls._shutdown = aiohttp.ClientSession(loop=asyncio.get_event_loop()), False
        url = urlparse(url)
        authority = "{}://{}".format(url.scheme, url.netloc)
        uuid = cls.fqdns[url.netloc] = kwargs['uid'] = cls.fqdns.get(url.netloc, None) or cls.get_uuid(authority)
        cls.instances[uuid] = cls.instances.get(uuid, None) or super(_SingletonOnUUID, cls).__call__(authority, **kwargs)
        return cls.instances[uuid]
    
    @classmethod
    def get_uuid(cls, url):
        headers = { 'Content-Type': 'application/perfsonar+json', 'Accept': MIME['PSJSON'] }
        resp = requests.get(urljoin(url, "about"), headers=headers)
        if 200 <= resp.status_code <= 299:
            config = resp.json()
        else:
            raise ConnectionError("Error from server", resp.status_code)
        return config['uid']

class UnisClient(metaclass=_SingletonOnUUID):
    @trace.debug("UnisClient")
    def __init__(self, url, loop, **kwargs):
        def _handle_exception(future):
            if not future.cancelled and future.exception():
                raise future.exception()
        async def _listen():
            while True:
                msg = json.loads(await self._socket.recv())
                list(map(lambda cb: cb(msg['data'], msg['headers']['action']), self._channels[msg['headers']['collection']]))
        async def _make_socket():
            url = 'ws{}://{}/subscribe/nodes'.format("s" if self._ssl else "", urlparse(self._url).netloc)
            return await websockets.connect(url, loop=loop, ssl=self._ssl)
        self.uid = kwargs['uid']
        self._url = url
        self._verify, self._ssl = kwargs.get("verify", False), kwargs.get("ssl", None)
        self._socket = asyncio.run_coroutine_threadsafe(_make_socket(), loop).result(timeout=1)
        self._channels = defaultdict(list)
        asyncio.run_coroutine_threadsafe(_listen(), loop).add_done_callback(_handle_exception)
    
    @trace.debug("UnisClient")
    async def _do(self, f, *args, **kwargs):
        async with f(*args, verify_ssl=self._verify, ssl_context=self._ssl, **kwargs) as resp:
            return await self._check_response(resp, False)
    
    @trace.info("UnisClient")
    async def getResources(self):
        url, headers = self._get_conn_args("")
        return await self._do(self._session.get, self._url, headers=headers)
    
    @trace.info("UnisClient")
    async def getStubs(self, collection):
        url, headers = self._get_conn_args(collection, fields="selfRef")
        return await self._do(self._session.get, url, headers=headers)
    
    @trace.info("UnisClient")
    async def get(self, collection, **kwargs):
        url, headers = self._get_conn_args(collection, **kwargs)
        return await self._do(self._session.get, url, headers=headers)
    
    @trace.info("UnisClient")
    async def post(self, collection, data):
        url, headers = self._get_conn_args(collection)
        data = json.dumps(data) if isinstance(data, dict) else data
        return await self._do(self._session.post, url, data=data, headers=headers)
    
    @trace.info("UnisClient")
    async def put(self, ref, data):
        url, headers = self._get_conn_args(ref)
        data = json.dumps(data) if isinstance(data, dict) else data
        return await self._do(self._session.put, url, data=data, headers=headers)
    
    @trace.info("UnisClient")
    async def delete(self, ref):
        url, headers = self._get_conn_args(ref)
        return await self._do(self._session.delete, url, headers=headers)
    
    @trace.info("UnisClient")
    async def subscribe(self, collection, callback):
        async def _add_channel():
            await self._socket.send(json.dumps({'query':{}, 'resourceType': collection}))
        if not self._shutdown:
            self._channels[collection].append(callback)
            asyncio.run_coroutine_threadsafe(_add_channel(), loop)
        return []
    
    @trace.debug("UnisClient")
    def _get_conn_args(self, ref, **kwargs):
        headers = { 'Content-Type': 'application/perfsonar+json', 'Accept': MIME['PSJSON'] }
        makelist = lambda v: ",".join(v) if isinstance(v, list) else v
        params = "?{}".format("&".join(["=".join([k, makelist(v)]) for k,v in kwargs.items() if v]))
        return urljoin(self._url, "{}{}".format(urlparse(ref).path, params if params[1:] else "")), headers
    
    @trace.debug("UnisClient")
    async def _check_response(self, r, read_as_bson=True):
        if 200 <= r.status <= 299:
            try:
                body = await r.read()
                resp = bson.loads(body) if r.content_type == MIME['PSBSON'] else json.loads(str(body, 'utf-8'))
                return resp if isinstance(resp, list) else [resp]
            except Exception as exp:
                return r.status
        elif 400 <= r.status <= 499:
            raise Exception("Error from unis server [bad request] - [{exp}] {t}".format(exp = r.status, t = r.text))
        else:
            raise Exception("Error from unis server - [{exp}] {t}".format(exp = r.status, t = r.text))
    
    async def shutdown(self):
        self._shutdown = True
        if not self._session.closed:
            await self._session.close()
        if self._socket:
            self._socket.close()
