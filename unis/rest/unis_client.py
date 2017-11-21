import aiohttp
import asyncio
import bson
import itertools
import json
import re
import websockets

from collections import defaultdict
from lace.logging import trace

from unis.settings import MIME

class UnisError(Exception):
    pass

class UnisReferenceError(UnisError):
    def __init__(self, msg, href):
        super(UnisReferenceError, self).__init__(msg)
        self.href = href

loop = asyncio.new_event_loop()
class UnisProxy(object):
    @trace.debug("UnisProxy")
    def __init__(self, conns):
        re_str = 'http[s]?://([^:/]+)(:([0-9]{1,5}))?$'
        self.clients = {}
        self._session = None 
        self.default_source = "http://localhost:8888"
        asyncio.get_event_loop().run_in_executor(None, loop.run_forever)
        for conn in conns:
            if conn.get("enabled", True):
                if not re.compile(re_str).match(conn["url"]):
                    raise ValueError("unis url is malformed - {}".format(conn["url"]))
                if "default" in conn and conn["default"]:
                    self.default_source = conn["url"]
                self.clients[conn['url']] = UnisClient(conn, self)
    
    @trace.info("UnisProxy")
    def shutdown(self):
        async def close():
            list(map(lambda t: t.cancel(), [t for t in asyncio.Task.all_tasks(loop) if t != asyncio.Task.current_task(loop)]))
            await asyncio.sleep(0.1)
            loop.stop()
            
        asyncio.run_coroutine_threadsafe(close(), loop)
        self._session.close()
    
    @trace.info("UnisProxy")
    async def getResources(self, source=None):
        if not self._session:
            self._session = aiohttp.ClientSession()
        if source:
            if source in self.clients:
                return await self.clients[source].getResources()
            else:
                raise ValueError("No unis instance at requested location - {}".format(source))
        else:
            results = await self._query_all([client.getResources for client in self.clients.values()])
            return results
    
    @trace.info("UnisProxy")
    async def get(self, href, source=None, kwargs={}):
        if not self._session:
            self._session = aiohttp.ClientSession()
        if source:
            if source in self.clients:
                result = await self.clients[source].get(href, kwargs)
                return result if isinstance(result, list) else [result]
            else:
                raise ValueError("No unis instance at requested location - {}".format(source))
        else:
            try:
                result = await self.clients[self._source_from_ref(href)].get(href, kwargs)
                return result if isinstance(result, list) else [result]
            except ValueError:
                return await self._query_all([client.get for client in self.clients.values()], href, kwargs)
    
    @trace.info("UnisProxy")
    async def post(self, resources):
        if not self._session:
            self._session = aiohttp.ClientSession()
        msgs = {}
        resources = resources if isinstance(resources, list) else [resources]
        for resource in resources:
            source = resource.getSource() or self.default_source
            col = resource.getCollection()
            k = "{}.{}".format(source, col)
            if k not in msgs:
                msgs[k] = (source, col, [])
            msgs[k][2].append(resource.to_JSON())
        results = await asyncio.gather(*[self.clients[d[0]].post("#/{}".format(d[1]), json.dumps(d[2])) for d in msgs])
        return list(itertools.chain(*[list(r) for r in results]))
    
    @trace.info("UnisProxy")
    async def put(self, href, data):
        if not self._session:
            self._session = aiohttp.ClientSession()
        source = self._source_from_ref(href)
        if source in self.clients:
            return await self.clients[source].put(href, data)
        else:
            raise ValueError("No unis instance at requested location - {}".format(source))
    
    @trace.info("UnisProxy")
    async def delete(self, data):
        if not self._session:
            self._session = aiohttp.ClientSession()
        source = data.getSource() or self.default_source
        if source in self.clients:
            return await self.clients[source].delete(data.to_JSON())
        else:
            raise ValueError("No unis client at requested location - {}".format(source))
    
    @trace.info("UnisProxy")
    async def subscribe(self, collection, callback, source=None):
        if source:
            return await self.clients[source].subscribe(collection, callback)
        return await self._query_all([client.subscribe for client in self.clients.values()], collection, callback)
    
    @trace.debug("UnisProxy")
    async def _query_all(self, funcs, *args):
        results = await asyncio.gather(*[f(*args) for f in funcs])
        return list(itertools.chain(*results))
    
    @trace.debug("UnisProxy")
    def _source_from_ref(self, href):
        re_str = "(?P<source>http[s]?://(?P<host>[^:/]+)(?::(?P<port>[0-9]{1,4}))?)"
        match = re.compile(re_str).match(href)
        if match:
            return match.group("source")
        raise ValueError("href must be a full url - {}".format(href))
        
class UnisClient(object):
    @trace.debug("UnisClient")
    def __init__(self, kwargs, proxy):
        re_str = 'http[s]?://(?P<host>[^:/]+)(?::(?P<port>[0-9]{1,5}))?$'
        if not re.compile(re_str).match(kwargs['url']):
            raise ValueError("unis url is malformed")
        
        self._url = kwargs["url"]
        self._verify = kwargs.get("verify", False)
        self._ssl = kwargs.get("cert", None)
        self._shutdown = False
        self._socket = None
        self._threads = []
        self._proxy = proxy
        self._channels = defaultdict(list)
    
    @trace.info("UnisClient")
    def shutdown(self):
        self._shutdown = True
        if self._socket:
            self._socket.close()
            list(map(lambda t: t.close(), self._threads))
    
    @trace.info("UnisClient")
    async def getResources(self):
        headers = { 'Content-Type': 'application/perfsonar+json',
                    'Accept': MIME['PSJSON'] }
        async with self._proxy._session.get(self._url, verify_ssl=self._verify, ssl_context=self._ssl, headers=headers) as resp:
            return await self._check_response(resp, False)
                
    @trace.info("UnisClient")
    async def get(self, url, kwargs={}):
        args = self._get_conn_args(url)
        args["url"] = self._build_query(args, **kwargs)
        async with self._proxy._session.get(args['url'], verify_ssl=self._verify, ssl_context=self._ssl, headers=args["headers"]) as resp:
            return await self._check_response(resp, False)
    
    @trace.info("UnisClient")
    async def post(self, url, data):
        args = self._get_conn_args(url)
        if isinstance(data, dict):
            data = json.dumps(data)
        async with self._proxy._session.post(args['url'], data=data, verify_ssl=self._verify, ssl_context=self._ssl) as resp:
            return await self._check_response(resp, False)
                
    @trace.info("UnisClient")
    async def put(self, url, data):
        args = self._get_conn_args(url)
        if isinstance(data, dict):
            data = json.dumps(data)
        async with self._proxy._session.put(args['url'], data=data, verify_ssl=self._verify, ssl_context=self._ssl) as resp:
            return await self._check_response(resp, False)
    
    @trace.info("UnisClient")
    async def delete(self, url):
        args = self._get_conn_args(url)
        async with self._proxy._session.delete(args['url'], verify_ssl=self._verify, ssl_context=self._ssl) as resp:
            return await self._check_response(resp, False)
    
    @trace.info("UnisClient")
    async def subscribe(self, collection, callback):
        async def _listen(ws):
            async for msg in ws:
                msg = json.loads(msg)
                list(map(lambda cb: cb(msg['data']), self._channels[msg['headers']['collection']]))
        
        if not self._shutdown:
            self._channels[collection].append(callback)
            if not self._socket:
                m = re.match('http[s]?://(?P<host>[^:/]+)(?::(?P<port>[0-9]{1,5}').compile(self._url)
                url = 'ws{}://{}:{}/subscribe/{}'.format("s" if self._ssl else "", m.group('host'), m.group('port') or 80, collection)
                self._socket = await websockets.connect(url, self._ssl)
                asyncio.run_coroutine_threadsafe(_updater(self._socket, collection), loop)
            else:
                await self._socket.send(json.dumps({'query':{}, 'resourceType':collection}))
        
    @trace.debug("UnisClient")
    def _build_query(self, args, **kwargs):
        q = ""
        for k,v in kwargs.items():
            if v:
                v = ",".join(v) if isinstance(v, list) else v
                q += "{k}={v}&".format(k = k, v = v)
        
        return "{b}?{q}".format(b=args["url"], q=q[:-1])
    
    @trace.debug("UnisClient")
    def _get_conn_args(self, url):
        re_str = "{full}|{rel}|{name}".format(full = 'http[s]?://(?P<host>[^:/]+)(?::(?P<port>[0-9]{1,4}))?/(?P<col1>[a-zA-Z]+)(?:/(?P<uid1>[^/]+))?$',
                                              rel  = '#/(?P<col2>[a-zA-Z]+)(?:/(?P<uid2>[^/]+))?$',
                                              name = '(?P<col3>[a-zA-Z]+)$')
        matches = re.compile(re_str).match(url)
        collection = matches.group("col1") or matches.group("col2") or matches.group("col3")
        uid = matches.group("uid1") or matches.group("uid2")
        return { "collection": collection,
                 "url": "{u}/{c}{i}".format(u = self._url, c = collection, i = "/" + uid if uid else ""),
                 "headers": { 'Content-Type': 'application/perfsonar+json',
                              'Accept': MIME['PSJSON'] } }
    
    @trace.debug("UnisClient")
    async def _check_response(self, r, read_as_bson=True):
        if 200 <= r.status <= 299:
            try:
                body = await r.read()
                if r.content_type == MIME['PSBSON']:
                    return bson.loads(body)
                else:
                    return json.loads(str(body, 'utf-8'))
            except Exception as exp:
                return r.status
        elif 400 <= r.status <= 499:
            raise Exception("Error from unis server [bad request] - {t} [{exp}]".format(exp = r.status, t = r.text))
        else:
            raise Exception("Error from unis server - {t} [{exp}]".format(exp = r.status_code, t = r.text))
