import asyncio, requests, socket, websockets as ws
import copy, itertools, json

from aiohttp import ClientSession, ClientResponse
from collections import defaultdict
from lace.logging import trace
from lace.logging import getLogger
from requests.exceptions import ConnectionError as RequestsConnectionError
from urllib.parse import urljoin, urlparse
from websockets.exceptions import ConnectionClosed

from unis.settings import MIME
from unis.exceptions import ConnectionError, UnisReferenceError
from unis.utils import async

class CID(str):
    pass

class ReferenceDict(dict):
    def __getitem__(self, n):
        """ Internal object get item method
        :param n: Name to look up in the object
        
        :type n: str
        :rtype: Any
        """
        try:
            return super(ReferenceDict, self).__getitem__(n)
        except (KeyError, IndexError):
            raise UnisReferenceError("No unis instance at location - {}".format(n), [n])

class UnisProxy(object):
    @trace.debug("UnisProxy")
    def __init__(self, col=None):
        """ Internal initializiation method
        :param col: Name of the collection owning this proxy
        
        :type col: str
        :rtype: None
        """
        self._name = col
    
    @trace.info("UnisProxy")
    def addSources(self, sources):
        """ Add a remote data source to this proxy.  Returns a list of client identifiers.
        :param sources: List of remote endpoints to connect to
        
        :type sources: List[Dict[str, Any]]
        :rtype: List[CID]
        """
        new = []
        old = copy.copy(list(UnisClient.instances.keys()))
        for s in sources:
            client = UnisClient(**s)
            if client.virtual and s['default']:
                raise ConnectionError("Failed to connect to default client", 404)
            if not client.virtual and client.uid not in old:
                new.append(CID(client.uid))
        return new
    
    @trace.info("UnisProxy")
    async def getResources(self, src=None):
        """ Query remote data for collection types.  Returns a list of dictionaries.
        :param src: List of client identifiers to query
        
        :type src: List[CID]
        :rtype: List[Dict[str, Any]]
        """
        src = src or []
        async with ClientSession() as sess:
            return await self._gather(self._collect_fn(src, "getResources"), sess=sess)
    
    @trace.info("UnisProxy")
    async def getStubs(self, src):
        """ Query minimal cache data for this proxy.  This function must be called when creating
        a new collection.   Returns a list of dictionaries.
        :param src: List of client identifiers to query
        
        :type src: List[CID]
        :rtype: List[Dict[str, Any]]
        """
        async with ClientSession() as sess:
            return await self._gather(self._collect_fn(src, "getStubs"), self._name, sess=sess)
    
    @trace.info("UnisProxy")
    async def get(self, src=None, **kwargs): 
        """ Request the full contents of a set of records.  Returns a list of dictionaries.
        
        :param src: List of client identifiers to request
        :param **kwargs: Request parameters to remote data store
        :type src: List[CID]
        :type **kwargs: str
        :rtype: List[Dict[str, Any]]
        """
        src = src or []
        async with ClientSession() as sess:
            return await self._gather(self._collect_fn([src], "get"), self._name, sess=sess, **kwargs)

    @classmethod
    def post(cls, cols):
        """ Submit the contents of a set of records to a data source.  Returns a list of dictionaries.
        
        :param cols: Dictionary containing the resources to be submitted.
        :type src: Dict[Tuple[CID, str], List[UnisObject]]
        :rtype: List[Dict[str, Any]]
        """
        async def _f():
            async with ClientSession() as sess:
                futs = [UnisClient.instances[i].post(n,v,sess) for (i,n),v in cols.items()]
                results = await asyncio.gather(*futs)
            return list(itertools.chain(*results))
        return async.make_async(_f)
    
    @trace.info("UnisProxy")
    async def put(self, src, rid, data):
        """ Submit the contents of a set of records to update a data source.  
        Returns a list of dictionaries.
        :param src: Client identifier for target data store
        :param rid: Resource identifier for resource to update
        :param data: Fields to update in the resource
        
        :type src: CID
        :type rid: str
        :type data: Dict[str, Any]
        :rtype: List[Dict[str, Any]]
        """
        async with ClientSession() as sess:
            return await UnisClient.instances[src].put("/".join([self._name, rid]), data, sess)
    
    @trace.info("UnisProxy")
    async def delete(self, src, rid):
        """ Delete a resource from a data store.  Returns a list of dictionaries.
        :param src: Client identifier for target data store
        :param rid: Resource identifier for resource to update
        
        :type src: CID
        :type rid: str
        :rtype: List[Dict[str, Any]]
        """
        async with ClientSession() as sess:
            return await UnisClient.instances[src].delete("/".join(self._name, rid), sess)
    
    @trace.info("UnisProxy")
    async def subscribe(self, src, cb):
        """ Subscribe to push messages from a data store.  Returns a list of dictionaries.
        :param src: List of client identifiers for target data stores
        :param cb: Callback function for data updates
        
        :type src: CID
        :type rid: Callable[[Dict[str, Any], str], None]
        :rtype: List[Dict[str, Any]]
        """
        return await self._gather(self._collect_fn(src, "subscribe"), self._name, cb)
    
    @trace.debug("UnisProxy")
    def _collect_fn(self, src, fn):
        """
        :param src: List of client identifiers for target data stores
        :param fn: Function name to gather from clients
        
        :type src: List[CID]
        :type fn: str
        :rtype: List[Callable[..., List[Dict[str, Any]]]]
        """
        if src:
            return [getattr(UnisClient.instances[s], fn) for s in src]
        return [getattr(c, fn) for c in UnisClient.instances.values()]
    
    @trace.debug("UnisProxy")
    async def _gather(self, fn, *args, **kwargs):
        """
        :param fn: List of functions to call
        :param *args: Positional arguments for the functions
        :param **kwargs: Keyword arguments for the functions
        
        :type fn: List[Callable[..., List[Dict[str, Any]]]]
        :type *args: Any
        :type **kwargs: Any
        :rtype: List[Dict[str, Any]]
        """
        results = await asyncio.gather(*[f(*args, **kwargs) for f in fn])
        return list(itertools.chain(*results))


class _SingletonOnUID(type):
    fqdns, instances, virtuals = ReferenceDict(), {}, {}
    def __call__(cls, url, *args, **kwargs):
        url = urlparse(url)
        authority = "{}://{}".format(url.scheme, url.netloc)
        try:
            uuid = cls.fqdns[url.netloc] = CID(cls.fqdns.get(url.netloc) or cls.get_uuid(authority))
        except RequestsConnectionError:
            kwargs['virtual'] = True
            kwargs['url'] = authority
            cls.virtuals[authority] = cls.virtuals.get(url) or super().__call__(*args, **kwargs)
            return cls.virtuals[authority]
        
        if uuid not in cls.instances:
            kwargs.update({'url': authority})
            cls.instances[uuid] = super().__call__(*args, **kwargs)
            cls.instances[uuid].uid = uuid
            return cls.instances[uuid]
        return cls.instances[uuid]
    
    @classmethod
    def get_uuid(cls, url):
        """ Query a backend uuid for a client from a endpoint url
        :param url: Endpoint url for the client
        
        :type url: str
        :rtype: str
        """
        headers = { 'Content-Type': 'application/perfsonar+json', 'Accept': MIME['PSJSON'] }
        resp = requests.get(urljoin(url, "about"), headers=headers)
        if 200 <= resp.status_code <= 299:
            config = resp.json()
        else:
            raise ConnectionError("Error from server", resp.status_code)
        return config['uid']
    
    @classmethod
    def resolve(cls, url):
        """ Attempts to find a uuid for a url string, if the url string corosponds to an
        unregistered instance, it will throw a UnisReferenceError.
        :param url: Endpoint url for the client
        
        :type url: str
        :rtype: CID
        """
        url = urlparse(url)
        authority = "{}://{}".format(url.scheme, url.netloc)
        try:
            uuid = cls.fqdns[url.netloc]
        except UnisReferenceError:
            uuid = cls.get_uuid(authority)
            if uuid not in cls.instances:
                raise
        return CID(uuid)
    
class UnisClient(metaclass=_SingletonOnUID):
    @trace.debug("UnisClient")
    def __init__(self, url, **kwargs):
        """
        :param url: Endpoint url for the client
        :param **kwargs: Parameters to the client
        
        :type url: str
        :type **kwargs: Any
        :rtype: None
        """
        self.loop = asyncio.new_event_loop()
        self._open, self._socket = True, None
        self._virtual = kwargs.get('virtual', False)
        asyncio.get_event_loop().run_in_executor(None, self.loop.run_forever)
        self._url, self._verify, self._ssl = url, kwargs.get("verify", False), kwargs.get("ssl")
        self._channels = defaultdict(list)
        
        if not self._virtual:
            f = asyncio.run_coroutine_threadsafe(self._listen(self.loop), self.loop)
            f.add_done_callback(self._handle_exception)

    @property
    def virtual(self):
        return self._virtual
    @virtual.setter
    def virtual(self, v):
        self._virtual = v
        if not v:
            if self._socket is None:
                f = asyncio.run_coroutine_threadsafe(self._listen(self.loop), self.loop)
                f.add_done_callback(self._handle_exception)
    async def check(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            url = urlparse(self._url)
            sock.connect((url.hostname, url.port))
            return True
        except socket.error:
            return False
        finally:
            sock.close()

    def _handle_exception(self, future):
        if not future.cancelled() and future.exception():
            raise future.exception()
    async def _listen(self, loop):
        opts = { "ssl": 's' if self._ssl else '',
                 "auth": urlparse(self._url).netloc}
        ref = 'ws{ssl}://{auth}/subscribe'.format(**opts)
        self._socket = False
        while True:
            while not self._socket:
                try:
                    fut = ws.connect(ref, loop=loop, ssl=self._ssl)
                    self._socket = await asyncio.wait_for(fut, timeout=1)
                except OSError:
                    import time
                    msg = "[{}]No websocket connection, retrying...".format(urlparse(self._url).netloc)
                    time.sleep(1)
                    getLogger("unisrt").warn(msg)
                except asyncio.TimeoutError:
                    msg = "[{}]No websocket connection, retrying...".format(urlparse(self._url).netloc)
                    getLogger("unisrt").warn(msg)
            
            try:
                while True:
                    msg = json.loads(await self._socket.recv())
                    for cb in self._channels[msg['headers']['collection']]:
                        cb(msg['data'], msg['headers']['action'])
            except ConnectionClosed:
                if self._open:
                    msg = "[{}]Lost websocket connection, retrying...".format(urlparse(url).netloc)
                    getLogger("unisrt").warn(msg)
                    self._socket = False
                else:
                    raise
        

    @trace.debug("UnisClient")
    async def _do(self, fn, *args, **kwargs):
        """ Execute a remote call
        :param fn: Function to call
        :param *args: Positional arguments
        :param **kwargs: Keyword arguments
        
        :type fn: Callable[..., ClientResponse]
        :type *args: Any
        :type **kwargs: Any
        :rtype: List[Dict[str, Any]]
        """
        async with fn(*args, verify_ssl=self._verify, ssl_context=self._ssl, **kwargs) as resp:
            return await self._check_response(resp)
    
    @trace.info("UnisClient")
    async def getResources(self, sess):
        """
        :param sess: Session object for request
        
        :type sess: ClientSession
        :rtype: List[Dict[str, Any]]
        """
        url, hdr = self._get_conn_args("")
        return await self._do(sess.get, self._url, headers=hdr)
    
    @trace.info("UnisClient")
    async def getStubs(self, col, sess):
        """
        :param col: Name of the collection to retrieve stubs from
        :param sess: Session object for request
        
        :type col: str
        :type sess: ClientSession
        :rtype: List[Dict[str, Any]]
        """
        url, hdr = self._get_conn_args(col, fields="selfRef")
        return await self._do(sess.get, url, headers=hdr)

    @trace.info("UnisClient")
    async def get(self, col, sess, **kwargs): 
        """
        :param col: Name of the collection to retrieve stubs from
        :param sess: Session object for request
        :param **kwargs: Keyword arguments to the request
        
        :type col: str
        :type sess: ClientSession
        :type **kwargs: Any
        :rtype: List[Dict[str, Any]]
        """
        url, hdr = self._get_conn_args(col, **kwargs)
        return await self._do(sess.get, url, headers=hdr)

    @trace.info("UnisClient")
    async def post(self, col, data, sess):
        """
        :param col: Name of the collection to retrieve stubs from
        :param data: Dictionary containing the data to send to store
        :param sess: Session object for request
        
        :type col: str
        :type data: Dict[str, Any]
        :type sess: ClientSession
        :rtype: List[Dict[str, Any]]
        """
        url, hdr = self._get_conn_args(col)
        return await self._do(sess.post, url, data=json.dumps(data), headers=hdr)

    @trace.info("UnisClient")
    async def put(self, col, data, sess):
        """
        :param col: Name of the collection to retrieve stubs from
        :param data: Dictionary containing the data to send to store
        :param sess: Session object for request
        
        :type col: str
        :type data: Dict[str, Any]
        :type sess: ClientSession
        :rtype: List[Dict[str, Any]]
        """
        url, hdr = self._get_conn_args(col)
        return await self._do(sess.put, url, data=json.dumps(data), headers=hdr)

    @trace.info("UnisClient")
    async def delete(self, col, sess):
        """
        :param col: Name of the collection to retrieve stubs from
        :param sess: Session object for request
        
        :type col: str
        :type sess: ClientSession
        :rtype: List[Dict[str, Any]]
        """
        url, hdr = self._get_conn_args(col)
        return await self._do(sess.delete, url, headers=hdr)
    
    @trace.info("UnisClient")
    async def subscribe(self, col, cb):
        """
        :param col: Name of the collection to retrieve stubs from
        :param cb: Callback function for messages
        
        :type col: str
        :type cb: Callable[..., None]
        :rtype: List[Dict[str, Any]]
        """
        async def _add_channel():
            await self._socket.send(json.dumps({'query':{}, 'resourceType': col}))
        self._channels[col].append(cb)
        asyncio.run_coroutine_threadsafe(_add_channel(), self.loop)
        return []
    
    @trace.debug("UnisClient")
    def _get_conn_args(self, ref, **kwargs):
        """
        :param ref: Reference string pointing to the request endpoint
        :param **kwargs: Keyword arguments to add to the request
        
        :type ref: str
        :type **kwargs: Any
        :rtype: Tuple[str, Dict[str, str]]
        """
        def _mkls(v):
            return ",".join(v) if isinstance(v, list) else v
        hdr = { 'Content-Type': 'application/perfsonar+json', 'Accept': MIME['PSJSON'] }
        params = "?{}".format("&".join(["=".join([k, _mkls(v)]) for k,v in kwargs.items() if v]))
        path = "{}{}".format(urlparse(ref).path, params if params[1:] else "")
        return urljoin(self._url, path), hdr

    @trace.debug("UnisClient")
    async def _check_response(self, r):
        """
        :param r: Resonse object from the data store
        
        :type ref: ClientResponse
        :rtype: List[Dict[str, Any]]
        """
        if 200 <= r.status <= 299:
            try:
                resp = json.loads(str(await r.read(), 'utf-8'))
                return resp if isinstance(resp, list) else [resp]
            except Exception as exp:
                return r.status
        else:
            raise ConnectionError("Error from unis - [{}] {}".format(r.status, r.text), r.status)
    
    @classmethod
    def shutdown(cls):
        """
        :rtype: None
        """
        for c in cls.instances.values():
            c._shutdown()
        for c in cls.virtuals.values():
            c._shutdown()
        _SingletonOnUID.fqdns = ReferenceDict()
        _SingletonOnUID.instances = ReferenceDict()
    
    @trace.debug("UnisClient")
    def _shutdown(self):
        async def close(loop):
            [t.cancel() for t in asyncio.Task.all_tasks(loop) if t != asyncio.Task.current_task(loop)]
            if self._socket:
                await self._socket.close()
            else:
                await asyncio.sleep(0.1)
            loop.stop()
        """
        :rtype: None
        """
        self._open = False
        asyncio.run_coroutine_threadsafe(close(self.loop), self.loop)
