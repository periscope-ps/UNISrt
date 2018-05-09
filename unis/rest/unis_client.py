import asyncio
import aiohttp, requests, websockets
#import bson, json
import json
import itertools

from collections import defaultdict
from urllib.parse import urljoin, urlparse
from lace.logging import trace

from unis.settings import MIME
from unis.utils import async

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
        self._name = collection
        if not loop.is_running():
            asyncio.get_event_loop().run_in_executor(None, loop.run_forever)
    
    @trace.info("UnisProxy")
    def shutdown(self):
        async def close():
            [t.cancel() for t in asyncio.Task.all_tasks(loop) if t != asyncio.Task.current_task(loop)]
            await asyncio.sleep(0.1)
            loop.stop()
        asyncio.run_coroutine_threadsafe(close(), loop)
        UnisClient.shutdown()
    
    @trace.info("UnisProxy")
    def refToUID(self, source, full=True):
        url = urlparse(source)
        if len(url.path.split('/')) != 3 and full:
            raise ValueError("Failed convertion to UUID:collection:id tuple - {}".format(source))
        uid = UnisClient(url.netloc).uid
        return uid, tuple(filter(lambda x: x, url.path.split('/')[-2:]))
        
    @trace.info("UnisProxy")
    def addSources(self, sources):
        new = []
        for s in sources:
            client, isnew = UnisClient.find(loop=loop, **s)
            if isnew:
                new.append(s['url'])
        return new
    @trace.info("UnisProxy")
    async def getResources(self, src=None):
        async with aiohttp.ClientSession() as sess:
            return await self._gather(self._collect_funcs(src, "getResources"), session=sess)
    
    @trace.info("UnisProxy")
    async def getStubs(self, src):
        async with aiohttp.ClientSession() as sess:
            return await self._gather(self._collect_funcs(src, "getStubs"), self._name, session=sess)
        
    @trace.info("UnisProxy")
    async def get(self, src=None, ref=None, **kwargs):
        async with aiohttp.ClientSession() as sess:
            ref = ref or self._name
            return await self._gather(self._collect_funcs(src, "get"), ref, session=sess, **kwargs)
    
    @trace.info("UnisProxy")
    def post(self, collections):
        async def _f(msgs):
            async with aiohttp.ClientSession() as sess:
                futs = [UnisClient(k[1]).post(k[0], json.dumps(v), sess) for k,v in msgs.items()]
                return await asyncio.gather(*futs)
        msgs = defaultdict(list)
        for col, resources in collections.items():
            for r in resources:
                msgs[(col, self.refToUID(r.getSource(), False)[0])].append(r.to_JSON())
        results = defaultdict(list)
        for col, res in async.make_async(_f, msgs):
            results[col].extend(res)
        return results
    
    @trace.info("UnisProxy")
    async def put(self, href, data):
        source = self.refToUID(href)
        async with aiohttp.ClientSession() as session:
            return await UnisClient(source[0]).put("/".join(source[1]), data, session)
    
    @trace.info("UnisProxy")
    async def delete(self, data):
        source = self.refToUID(data.selfRef)
        async with aiohttp.ClientSession() as session:
            return await UnisClient(source[0]).delete("/".join(source[1]), data.to_JSON(), session)
    
    @trace.info("UnisProxy")
    async def subscribe(self, source, cb, ref=None):
        return await self._gather(self._collect_funcs(source, "subscribe"), ref or self._name, cb)
        
    @trace.debug("UnisProxy")
    def _collect_funcs(self, src, n):
        if source:
            src = src if isinstance(src, list) else [src]
            src = [UnisClient(s[0]) if s isinstance(s, tuple) else UnisClient(s) for s in src]
            return [getattr(s, n) for s in source]
        return [getattr(c, n) for c in UnisClient.instances]
    
    @trace.debug("UnisProxy")
    async def _gather(self, funcs, *args, **kwargs):
        results = await asyncio.gather(*[f(*args, **kwargs) for f in funcs])
        return list(itertools.chain(*results))
    

class _SingletonOnUUID(type):
    fqdns, instances = ReferenceDict(), {}
    def __call__(cls, url, **kwargs):
        if not hasattr(cls, '_shutdown'):
            cls._shutdown = False
        return cls.find(url, **kwargs)[0]
    
    @classmethod
    def get_uuid(cls, url):
        headers = { 'Content-Type': 'application/perfsonar+json', 'Accept': MIME['PSJSON'] }
        resp = requests.get(urljoin(url, "about"), headers=headers)
        if 200 <= resp.status_code <= 299:
            config = resp.json()
        else:
            raise ConnectionError("Error from server", resp.status_code)
        return config['uid']
    
    @classmethod
    def find(cls, url, **kwargs):
        url = urlparse(url)
        authority = "{}://{}".format(url.scheme, url.netloc)
        uuid = cls.fqdns.get(url.netloc) or cls.get_uuid(authority)
        cls.fqdns[url.netloc] = kwargs['uid'] = uuid
        cls.instances[uuid] = cls.instances.get(uuid)
        if not cls.instances[uuid]:
            cls.instances[uuid] = super(_SingletonOnUUID, cls).__call__(authority, **kwargs)
            return cls.instance[uuid], True
        return cls.instance[uuid], False
    
class UnisClient(metaclass=_SingletonOnUUID):
    @trace.debug("UnisClient")
    def __init__(self, url, loop, **kwargs):
        def _handle_exception(future):
            if not future.cancelled() and future.exception():
                raise future.exception()
        async def _listen():
            while True:
                msg = json.loads(await self._socket.recv())
                for cb in self._channels[msg['headers']['collection']]:
                    cb(msg['data'], msg['headers']['action'])
        async def _make_socket():
            url = 'ws{}://{}/subscribe/nodes'
            url = url.format("s" if self._ssl else "", urlparse(self._url).netloc)
            return await websockets.connect(url, loop=loop, ssl=self._ssl)
        self.uid = kwargs['uid']
        self._url = url
        self._verify, self._ssl = kwargs.get("verify", False), kwargs.get("ssl")
        try:
            self._socket = asyncio.run_coroutine_threadsafe(_make_socket(), loop).result(timeout=1)
        except Exception as exp:
            return
        finally:
            self._channels = defaultdict(list)
        asyncio.run_coroutine_threadsafe(_listen(), loop).add_done_callback(_handle_exception)
    
    @trace.debug("UnisClient")
    async def _do(self, f, *args, **kwargs):
        async with f(*args, verify_ssl=self._verify, ssl_context=self._ssl, **kwargs) as resp:
            return await self._check_response(resp, False)
    
    @trace.info("UnisClient")
    async def getResources(self, session):
        url, headers = self._get_conn_args("")
        return await self._do(session.get, self._url, headers=headers)
    
    @trace.info("UnisClient")
    async def getStubs(self, collection, session):
        url, headers = self._get_conn_args(collection, fields="selfRef")
        return await self._do(session.get, url, headers=headers)
    
    @trace.info("UnisClient")
    async def get(self, collection, session, **kwargs):
        url, headers = self._get_conn_args(collection, **kwargs)
        return await self._do(session.get, url, headers=headers)
    
    @trace.info("UnisClient")
    async def post(self, collection, data, session):
        url, headers = self._get_conn_args(collection)
        data = json.dumps(data) if isinstance(data, dict) else data
        return (collection, await self._do(session.post, url, data=data, headers=headers))
    
    @trace.info("UnisClient")
    async def put(self, ref, data, session):
        url, headers = self._get_conn_args(ref)
        data = json.dumps(data) if isinstance(data, dict) else data
        return await self._do(session.put, url, data=data, headers=headers)
    
    @trace.info("UnisClient")
    async def delete(self, ref, session):
        url, headers = self._get_conn_args(ref)
        return await self._do(session.delete, url, headers=headers)
    
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
        hdr = { 'Content-Type': 'application/perfsonar+json', 'Accept': MIME['PSJSON'] }
        makelist = lambda v: ",".join(v) if isinstance(v, list) else v
        params = "?{}".format("&".join(["=".join([k, makelist(v)]) for k,v in kwargs.items() if v]))
        return urljoin(self._url, "{}{}".format(urlparse(ref).path, params if params[1:] else "")), hdr
    
    @trace.debug("UnisClient")
    async def _check_response(self, r, read_as_bson=True):
        if 200 <= r.status <= 299:
            try:
                body = await r.read()
                resp = json.loads(str(body, 'utf-8'))
                return resp if isinstance(resp, list) else [resp]
            except Exception as exp:
                return r.status
        elif 400 <= r.status <= 499:
            raise Exception("Error from unis server [bad request] - [{}] {}".format(r.status, r.text))
        else:
            raise Exception("Error from unis server - [{}] {}".format(r.status, r.text))
    
    @classmethod
    def shutdown(cls):
        [c._shutdown() for c in cls.instances.values()]
    
    def _shutdown(self):
        self._shutdown = True
        if self._socket:
            self._socket.close()
