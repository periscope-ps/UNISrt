import asyncio, requests, socket, websockets as ws
import copy, itertools, json, ssl

from aiohttp import ClientSession
from aiohttp.client_exceptions import ClientConnectionError
from collections import defaultdict
from lace.logging import trace
from lace.logging import getLogger
from requests.exceptions import ConnectionError as RequestsConnectionError
from urllib.parse import urljoin, urlparse
from websockets.exceptions import ConnectionClosed

from unis.settings import MIME
from unis.exceptions import ConnectionError, UnisReferenceError
from unis.utils import asynchronous

class CID(str):
    """
    The Client Identifier [:class:`CID <unis.rest.unis_client.CID>`] class is used to uniquely identify a client instance.
    :class:`CID <unis.rest.unis_client.CID>` is distinguished from a python string for typing purposes.
    """
    pass

class ReferenceDict(dict):
    """
    :class:`ReferenceDict <ReferenceDict>` inherits from the builtin dict class
    but raises a :class:`UnisReferenceError <unis.exceptions.UnisReferenceError>` 
    on ``KeyError`` or ``IndexError``.
    """
    def __getitem__(self, n):
        try:
            return super(ReferenceDict, self).__getitem__(n)
        except (KeyError, IndexError) as e:
            raise UnisReferenceError("No unis instance at location - {}".format(n), [n]) from e

@trace("unis.rest")
class UnisProxy(object):
    """
    :param str col: Name of the collection owning this proxy
    
    :class:`UnisProxy <unis.rest.unis_client.UnisProxy>` represents a collection of connections to remote data
    stores all relating to a provided collection **col** of resources.  This object
    maintains collection level metadata for threads and connection pooling.
    """
    def __init__(self, col=None):
        self._name = col
    
    def addSources(self, sources, ns, subscribe=True):
        """
        :param list[dict] sources: List of remote endpoints to connect to
        :param str ns: Namespace fort the source.
        :return: list of :class:`CIDs <unis.rest.unis_client.CID>`.
        
        Add a remote data source to this proxy.  Returns a list of client identifiers.
        The **sources** dictionary includes the following fields:
        
        * **url:** *str* scheme and authority pair to the client i.e. ``http://localhost:8888``.
        * **virtual:** (optional) Indicates a client as a virtual (disconnected) instance.
        * **verify:** (optional) If true, verify the SSL certificate.
        * **ssl:** (optional) *str* path to a file containing the SSL certificate.
        """
        new = []
        old = [c.uid for c in list(UnisClient.instances.values()) if ns in c.namespaces]
        for s in sources:
            client = UnisClient(**s, subscribe=subscribe)
            if client.virtual and s['default']:
                raise ConnectionError("Failed to connect to default client", 404)
            if not client.virtual and client.uid not in old:
                new.append(CID(client.uid))
            client.namespaces.add(ns)
        return new
    
    async def getResources(self, src=None):
        """
        :param src: List of client identifiers to query.
        :type src: list[:class:`CIDs <unis.rest.unis_client.CID>`]
        :return: list of dictionaries containing collection descriptions.
        :rtype: coroutine

        Query remote data for collection types.  Returns a list of dictionaries.
        """
        src = src or []
        async with ClientSession() as sess:
            return await self._gather(self._collect_fn(src, "getResources"), sess=sess)
    
    async def getStubs(self, src):
        """
        :param src: List of client identifiers to query.
        :type src: list[:class:`CIDs <unis.rest.unis_client.CID>`]
        :return: list of dictionaries containing selfRefs for each resource in the collection.
        :rtype: coroutine

        Query minimal cache data for this proxy.  This function must be called when creating
        a new collection.   Returns a list of dictionaries.
        """
        async with ClientSession() as sess:
            return await self._gather(self._collect_fn(src, "getStubs"), self._name, sess=sess)
    
    async def get(self, src=None, **kwargs): 
        """
        :param src: List of client identifiers to request.
        :param str \*\*kwargs: Request parameters to remote data store.
        :type src: list[:class:`CID <unis.rest.unis_client.CID>`]
        :return: list of dictionaries containing resources matching the request.
        :rtype: coroutine
        
        Request the full contents of a set of records.  Returns a list of dictionaries.
        The kwargs may contain a filter for limiting the results to resources that satisfy
        logical relationships in a ``key: value`` style.
        """
        src = src or []
        async with ClientSession() as sess:
            return await self._gather(self._collect_fn(src, "get"), self._name, sess=sess, **kwargs)

    @classmethod
    def post(cls, cols):
        """ 
        :param cols: Dictionary containing the resources to be submitted.
        :type cols: dict[tuple[:class:`CID <unis.rest.unis_client.CID>`, str], List[dict[str, str]]]
        :return: list of dictionaries containing the updated values for the posted resources.
        
        Submit the contents of a set of records to a data source.  The ``cols`` parameter is a dictionary wherein 
        :class:`UnisObjects <unis.models.models.UnisObject>` are keyed by a (:class:`CID <unis.rest.unis_client.CID>`, ``collection_name``) pair.
        The resulting dictionaries contain the entire resource including all fields whether altered or not.
        """
        async def _f():
            async with ClientSession() as sess:
                futs = [UnisClient.instances[i].post(n,v,sess) for (i,n),v in cols.items()]
                results = await asyncio.gather(*futs)
            return list(itertools.chain(*results))
        return asynchronous.make_async(_f)
    
    async def put(self, src, rid, data):
        """ 
        :param src: Client identifier for target data store.
        :param str rid: Resource identifier for resource to update.
        :param dict[str,str] data: Fields to update in the resource.
        :type src: :class:`CID <unis.rest.unis_client.CID>`
        :return: list of dictionaries containing the altered resources.
        :rtype: coroutine
        
        Submit the contents of a set of records to update a data source. 
        This update replaces old records in-situ and does not generate a new version.
        Recommended for only small continuous changes such as touching a resource.
        Returns a list of dictionaries.
        """
        async with ClientSession() as sess:
            return await UnisClient.instances[src].put("/".join([self._name, rid]), data, sess)
    
    def delete(self, src, rid):
        """
        :param src: Client identifier for target data store.
        :param str rid: Resource identifier for resource to update.
        :type src: :class:`CID <unis.rest.unis_client.CID>`
        :return: list of dictionaries containing the deleted resource.
        :rtype: coroutine
        
        Delete a resource from a data store.  Returns a list of dictionaries.
        """
        async def awrap():
            async with ClientSession() as sess:
                return await UnisClient.instances[src].delete("/".join([self._name, rid]), sess)
        return asynchronous.make_async(awrap)

    async def subscribe(self, src, cb):
        """ 
        :param src: List of client identifiers for target data stores
        :param cb: Callback function for data updates
        :type src: list[:class:`CID <unis.rest.unis_client.CID>`]
        :type cb: callable
        :return: An empty list.
        :rtype: coroutine
        
        Subscribe to push messages from a data store.  Returns a list of dictionaries.
        
        ``callback`` must have the following signiture:
        
        **Parameters:** 
        
        * **resource** (*dict*) - dictionary representation of the resource.
        *  **action** (*str*) - The action [``PUT``, ``POST``] generating the event.
        """
        return await self._gather(self._collect_fn(src, "subscribe"), self._name, cb)
    
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
    def __call__(cls, url, *args, subscribe=True, **kwargs):
        ref = urlparse(url)
        authority = "{}://{}".format(ref.scheme, ref.netloc.strip('/'))
        if not ref.netloc:
            raise ValueError("invalid url - {}".format(url))
        try:
            uuid = cls.fqdns[ref.netloc] = CID(cls.fqdns.get(ref.netloc) or cls.get_uuid(authority, **kwargs))
        except RequestsConnectionError:
            kwargs['virtual'] = True
            kwargs['url'] = authority
            cls.virtuals[authority] = cls.virtuals.get(authority) or super().__call__(*args, **kwargs)
            return cls.virtuals[authority]
        
        if uuid not in cls.instances:
            kwargs.update({'url': authority})
            cls.instances[uuid] = super().__call__(*args, **kwargs)
            cls.instances[uuid].uid = uuid
            if subscribe:
                cls.instances[uuid].connect()
            return cls.instances[uuid]
        return cls.instances[uuid]
    
    @classmethod
    @trace.tshort("unis.rest.UnisClient")
    def get_uuid(cls, url, **kwargs):
        """ Query a backend uuid for a client from a endpoint url
        :param url: Endpoint url for the client
        
        :type url: str
        :rtype: str
        """
        if not getattr(cls, "cert", None):
            cls.cert = kwargs.get("ssl", None)
        if not getattr(cls, "verify", None):
            cls.verify = kwargs.get("verify", False)
        headers = { 'Content-Type': 'application/perfsonar+json', 'Accept': MIME['PSJSON'] }
        try:
            resp = requests.get(urljoin(url, "about"), cert=(cls.cert), timeout=0.1, verify=cls.verify, headers=headers)
        except RequestsConnectionError as e:
            raise UnisReferenceError("Cannot connect to remote /about", url) from e
        if 200 <= resp.status_code <= 299:
            config = resp.json()
        else:
            raise ConnectionError("Error from server", resp.status_code)
        return config['uid']
    
    @classmethod
    @trace.tshort("unis.rest.UnisClient")
    def resolve(cls, url):
        """ Attempts to find a uuid for a url string, if the url string corosponds to an
        unregistered instance, it will throw a UnisReferenceError.
        
        :param url: Endpoint url for the client
        
        :type url: str
        :rtype: CID
        """
        url = urlparse(url)
        authority = "{}://{}".format(url.scheme, url.netloc.strip('/'))
        try:
            uuid = cls.fqdns[url.netloc]
        except UnisReferenceError as e:
            cls.fqdns[url.netloc] = uuid = cls.get_uuid(authority)
            if uuid not in cls.instances:
                raise
        return CID(uuid)


class VirtualClient(metaclass=_SingletonOnUID):
    pass
@trace("unis.rest")
class UnisClient(metaclass=_SingletonOnUID):
    """
    :param str url: Endpoint url for the client
    :param bool virtual: Use a client as a virtual (disconnected) instance
    :param bool verify: Verify SSL certificate
    :param str ssl: File containing the ssl certificate
    
    :class:`UnisClient <unis.rest.unis_client.UnisClient>` maintains the connection to a specific data store.
    This includes keeping a websocket and associated threads alive for subscription events
    and poviding restful functions for individual requests to the associated data store.
    """
    def __init__(self, url, **kwargs):
        self.namespaces = set()
        self.loop = asyncio.new_event_loop()
        self._open, self._socket = True, None
        self._virtual = kwargs.get('virtual', False)
        try: asyncio.get_event_loop().run_in_executor(None, self.loop.run_forever)
        except RuntimeError:
            asyncio.set_event_loop(asyncio.new_event_loop())
            asyncio.get_event_loop().run_in_executor(None, self.loop.run_forever)

        url = (lambda x: f"{x.scheme}://{x.netloc.strip('/')}")(urlparse(url))
        self._url, self._verify, self._ssl = url, kwargs.get("verify", False), kwargs.get("ssl")
        self._channels, self._lock = defaultdict(list), True
        self._sslcontext=None
        if self._ssl:
            self._sslcontext = ssl.create_default_context(purpose=ssl.Purpose.CLIENT_AUTH)
            self._sslcontext.load_cert_chain(self._ssl)
        
    @property
    def virtual(self):
        return self._virtual
    @virtual.setter
    def virtual(self, v):
        self._virtual = v
        if not v:
            if self._socket is None:
                self.connect()
    async def check(self):
        """
        :rtype: boolean
        
        Attempt to ping the remote data store.  Returns True if a response is
        recieved from the remote data store otherwise returns false.
        """
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
                    fut = ws.connect(ref, loop=loop, ssl=self._sslcontext)
                    self._socket = await asyncio.wait_for(fut, timeout=1, loop=loop)
                    self._lock = True
                    for col in self._channels.keys():
                        await self._socket.send(json.dumps({'query':{}, 'resourceType': col}))
                    self._lock = False
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
                    msg = "[{}]Lost websocket connection, retrying...".format(urlparse(self._url).netloc)
                    getLogger("unisrt").warn(msg)
                    self._socket = False
                else:
                    raise
        

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
        try:
            async with fn(*args, ssl=self._sslcontext, timeout=10, **kwargs) as resp:
                return await self._check_response(resp)
        except (asyncio.TimeoutError, ClientConnectionError):
            arg = args[0][:60] + ('...' if len(args[0]) > 60 else '')
            getLogger("unisrt").warn("[{}] Timeout on request to instance '{}', deferring {}".format(arg, self._url, fn.__name__.upper()))
            getLogger("unisrt").debug("[{}] Timeout on request to instance '{}', deferring {}".format(args[0], self._url, fn.__name__.upper()))
            return []
    
    def connect(self):
        if not self._virtual and not self._socket:
            f = asyncio.run_coroutine_threadsafe(self._listen(self.loop), self.loop)
            f.add_done_callback(self._handle_exception)

    async def getResources(self, sess):
        """
        :param sess: Session object for request
        :type sess: :class:`aiohttp.ClientSession`
        :return: List of dictionaries containing the resource endpoints available at this data store.
        :rtype: coroutine
        """
        url, hdr = self._get_conn_args("")
        return await self._do(sess.get, self._url, headers=hdr)
    
    async def getStubs(self, col, sess):
        """
        :param str col: Name of the collection to retrieve stubs from
        :param sess: Session object for request
        :type sess: :class:`aiohttp.ClientSession`
        :return: List of dictionaries containing the selfRefs for resources in a given collection.
        :rtype: coroutine
        """
        url, hdr = self._get_conn_args(col, fields="selfRef", unique="true")
        return await self._do(sess.get, url, headers=hdr)

    async def get(self, col, sess, **kwargs):
        """
        :param str col: Name of the collection to get data from
        :param sess: Session object for request
        :param \*\*kwargs: Keyword arguments to the request
        :type sess: :class:`aiohttp.ClientSession`
        :return: List of dictionaries containing the resources matching the conditions in \*\*kwargs.
        :rtype: coroutine
        """
        url, hdr = self._get_conn_args(col, **kwargs)
        return await self._do(sess.get, url, headers=hdr)

    async def post(self, col, data, sess):
        """
        :param str col: Name of the collection to post data
        :param dict[str,str] data: Dictionary containing the data to send to store
        :param sess: Session object for request
        :type sess: :class:`aiohttp.ClientSession`
        :return: List of dictionaries containing the resources posted to the store.
        :rtype: coroutine
        """
        url, hdr = self._get_conn_args(col)
        return await self._do(sess.post, url, data=json.dumps(data), headers=hdr)

    def synchronous_post(self, col, data):
        """
        :param str col: Name of the collection to post data
        :param dict[str, str] data: Dictionary containing the data to send to the store
        :return: List of dictionaries containing the resources posted to the store.
        """
        url, hdr = self._get_conn_args(col)
        return requests.post(url, data=json.dumps(data), headers=hdr)
    
    async def put(self, col, data, sess):
        """
        :param str col: Name of the collection to put data into
        :param dict[str,str] data: Dictionary containing the data to send to store
        :param sess: Session object for request
        :type sess: :class:`aiohttp.ClientSession`
        :return: List of dictionaries containing the resources posted to the store.
        :rtype: coroutine
        """
        url, hdr = self._get_conn_args(col)
        try:
            async with sess.put(url, data=json.dumps(data), headers=hdr, ssl=self._sslcontext, timeout=1) as resp:
                await self._check_response(resp)
                return True
        except (asyncio.TimeoutError, ClientConnectionError):
            getLogger("unisrt").warn( f"[{col}] Timeout on request to instance '{self._url}', deferring PUT")
            getLogger("unisrt").debug(f"   + Data | {data}")
            return False

    async def delete(self, col, sess):
        """
        :param str col: Name of the collection to delete resource from
        :param sess: Session object for request
        :type sess: :class:`aiohttp.ClientSession`
        :return: List of dictionaries containing the resources removed from the store.
        :rtype: coroutine
        """
        url, hdr = self._get_conn_args(col)
        return await self._do(sess.delete, url, headers=hdr)
    
    async def subscribe(self, col, cb):
        """
        :param str col: Name of the collection to subscribe to
        :param callable cb: Callback function for messages
        :rtype: coroutine
        """
        async def _add_channel():
            await self._socket.send(json.dumps({'query':{}, 'resourceType': col}))

        while self._lock: await asyncio.sleep(0)
        if col not in self._channels:
            asyncio.run_coroutine_threadsafe(_add_channel(), self.loop)
        self._channels[col].append(cb)
        return []
    
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

    async def _check_response(self, r):
        """
        :param r: Resonse object from the data store
        
        :type ref: ClientResponse
        :rtype: List[Dict[str, Any]]
        """
        if 200 <= r.status <= 299:
            try: resp = json.loads(str(await r.read(), 'utf-8'))
            except json.decoder.JSONDecodeError: resp = []
            return resp if isinstance(resp, list) else [resp]
        else:
            raise ConnectionError("Error from unis - [{}] {}".format(r.status, await r.text()), r.status)

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
        _SingletonOnUID.instances = {}
        _SingletonOnUID.virtuals = {}
    
    def _shutdown(self):
        async def close(loop):
            [t.cancel() for t in asyncio.Task.all_tasks(loop) if t != asyncio.Task.current_task(loop)]
            if self._socket:
                await self._socket.close()
            else:
                await asyncio.sleep(0)
            loop.stop()
        """
        :rtype: None
        """
        self._open = False
        asyncio.run_coroutine_threadsafe(close(self.loop), self.loop)
