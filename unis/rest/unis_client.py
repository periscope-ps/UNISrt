import json
import bson
import requests
import websocket
import concurrent.futures
import uritools

from concurrent.futures import ThreadPoolExecutor

from unis.runtime.settings import MIME
from unis import logging

class UnisError(Exception):
    pass

class UnisReferenceError(UnisError):
    def __init__(self, msg, href):
        super(UnisReferenceError, self).__init__(msg)
        self.href = href

class UnisProxy(object):
    @logging.debug("UnisProxy")
    def __init__(self, conns, inline=False):
        self.clients = {}
        self._default_source = "http://localhost:8888"
        for conn in conns:
            if conn.get("enabled", True):
                if not uritools.urisplit(conn['url']).authority:
                    raise ValueError("unis url is malformed - {}".format(conn["url"]))
                if "default" in conn and conn["default"]:
                    self._default_source = conn["url"]
                self.clients[conn['url']] = UnisClient(conn, inline)
    
    @logging.info("UnisProxy")
    def shutdown(self):
        for client in self.clients.values():
            client.shutdown()
    
    @logging.info("UnisProxy")
    def getResources(self, source=None):
        if source:
            if source in self.clients:
                return self.clients[source].getResources()
            else:
                raise ValueError("No unis instance at requested location - {}".format(source))
        else:
            return self._query_all([client.getResources for client in self.clients.values()])
    
    @logging.info("UnisProxy")
    def get(self, href, source=None, limit=None, kwargs={}):
        if source:
            if source in self.clients:
                result = self.clients[source].get(href, limit, kwargs)
                return result if isinstance(result, list) else [result]
            else:
                raise ValueError("No unis instance at requested location - {}".format(source))
        else:
            try:
                result = self.clients[self._source_from_ref(href)].get(href, limit, kwargs)
                return result if isinstance(result, list) else [result]
            except ValueError:
                return self._query_all([client.get for client in self.clients.values()], href, limit, kwargs)
    
    @logging.info("UnisProxy")
    def post(self, resources):
        msgs = {}
        resources = resources if isinstance(resources, list) else [resources]
        for resource in resources:
            source = resource.getSource() or self._default_source
            col = resource.getCollection()
            k = "{}.{}".format(source, col)
            if k not in msgs:
                msgs[k] = (source, col, [])
            msgs[k][2].append(resource.to_JSON())
        with ThreadPoolExecutor(max_workers=12) as executor:
            futures = []
            results = []
            for data in msgs.values():
                futures.append(executor.submit(self.clients[data[0]].post, "{}".format(data[1]), json.dumps(data[2])))
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if isinstance(result, list):
                    results.extend(result)
                else:
                    results.append(result)
        return results
    
    @logging.info("UnisProxy")
    def put(self, href, data):
        source = self._source_from_ref(href)
        if source in self.clients:
            return self.clients[source].put(href, data)
        else:
            raise ValueError("No unis instance at requested location - {}".format(source))
    
    @logging.info("UnisProxy")
    def delete(self, data):
        source = data.getSource() or self._default_source
        if source in self.clients:
            return self.clients[source].delete(data.to_JSON())
        else:
            raise ValueError("No unis client at requested location - {}".format(source))
    
    @logging.info("UnisProxy")
    def subscribe(self, collection, callback, source=None):
        if source:
            return self.clients[source].subscribe(collection, callback)
        return self._query_all([client.subscribe for client in self.clients.values()], collection, callback)
    
    @logging.debug("UnisProxy")
    def _query_all(self, funcs, *args):
        results = []
        with ThreadPoolExecutor(max_workers=12) as executor:
            futures = [executor.submit(func, *args) for func in funcs]
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if isinstance(result, list):
                    results.extend(result)
                else:
                    results.append(result)
        return results
    
    @logging.debug("UnisProxy")
    def _source_from_ref(self, href):
        if uritools.urijoin(href, '/') == '/':
            raise ValueError('href must contain a fully qualified domain name')
        return uritools.urijoin(href, '/').strip('/')
        
class UnisClient(object):
    @logging.debug("UnisClient")
    def __init__(self, kwargs, inline=False):
        self._url = kwargs["url"]
        self._verify = kwargs.get("verify", False)
        self._ssl = kwargs.get("cert", None)
        self._executor = ThreadPoolExecutor(max_workers=12)
        self._socket = None
        self._inline = inline
        self._shutdown = False
        self._channels = {}
    
    @logging.info("UnisClient")
    def shutdown(self):
        if self._socket and self._shutdown:
            self._socket.close()
            self._socket = None
        else:
            self._shutdown = True
        self._executor.shutdown()
    
    @logging.info("UnisClient")
    def getResources(self):
        headers = { 'Content-Type': 'application/perfsonar+json',
                    'Accept': MIME['PSJSON'] }
        return self._check_response(requests.get(self._url, verify = self._verify, cert = self._ssl, headers=headers), False)
    
    @logging.info("UnisClient")
    def get(self, url, limit = None, kwargs={}):
        args = self._get_conn_args(url)
        args["url"] = self._build_query(args, inline=self._inline, **kwargs)
        return self._check_response(requests.get(args["url"], verify = self._verify, cert = self._ssl, headers=args["headers"]), False)
    
    @logging.info("UnisClient")
    def post(self, url, data):
        args = self._get_conn_args(url)
        if isinstance(data, dict):
            data = json.dumps(data)
        
        return self._check_response(requests.post(args["url"], data = data, 
                                                  verify = self._verify, cert = self._ssl), False)
    
    @logging.info("UnisClient")
    def put(self, url, data):
        args = self._get_conn_args(url)
        if isinstance(data, dict):
            data = json.dumps(data)
            
        return self._check_response(requests.put(args["url"], data=data,
                                                 verify=self._verify, cert=self._ssl), False)
    
    @logging.info("UnisClient")
    def delete(self, url):
        args = self._get_conn_args(url)
        return self._check_response(requests.delete(args["url"], verify=self._verify, cert=self._ssl), False)
    
    @logging.info("UnisClient")
    def subscribe(self, collection, callback):
        if collection not in self._channels:
            self._channels[collection] = []
        self._channels[collection].append(callback)
        
        if self._socket:
            while not self._shutdown:
                pass
            self._socket.send(json.dumps({ 'query': {}, 'resourceType': collection}))
        else:
            self._subscribe(collection)
    @logging.debug("UnisClient")
    def _subscribe(self, collection):
        kwargs = {}
        if self._ssl:
            kwargs["ca_certs"] = self._ssl[0]
            
        uri = uritools.urisplit(self._url)
        url = "ws{s}://{h}/subscribe/{c}".format(s = "s" if "ca_certs" in kwargs else "", 
                                                 h = uri.authority,
                                                 c = collection)
        def on_message(ws, message):
            message = json.loads(message)
            if "headers" not in message or "collection" not in message["headers"]:
                raise UnisError("Depreciated header in message, client UNIS incompatable")
            callbacks = self._channels[message["headers"]["collection"]]
            for callback in callbacks:
                callback(message)
        def on_open(ws):
            if self._shutdown:
                ws.close()
            else:
                self._shutdown = True
            
        self._socket = websocket.WebSocketApp(url, 
                                              on_message = on_message,
                                              on_open  = on_open, 
                                              on_error = lambda ws, error: None,
                                              on_close = lambda ws: None)

        self._executor.submit(self._socket.run_forever, sslopt=kwargs)
        
    @logging.debug("UnisClient")
    def _build_query(self, args, inline=False, **kwargs):
        q = ""
        for k,v in kwargs.items():
            if v:
                q += "{k}={v}&".format(k = k, v = v)
                
        if inline:
            q += "inline"
        else:
            q = q[0:-1]
        return "{b}?{q}".format(b=args["url"], q=q)
    
    @logging.debug("UnisClient")
    def _get_conn_args(self, uri):
        path = uritools.urisplit(uri).getpath().split('/')
        collection, uid = path[-2:] if len(path) > 1 else (path[0], '')
        path = "/".join([collection, uid])
        uri = uritools.urijoin(self._url, path.strip('/'))
        return { "collection": collection, 
                 "url": uritools.urijoin(self._url, "/".join([collection, uid])).strip('/'),
                 "headers": { 'Content-Type': 'application/perfsonar+json', 'Accept': MIME['PSJSON'] } }
    
    @logging.debug("UnisClient")
    def _check_response(self, r, read_as_bson=True):
        if 200 <= r.status_code <= 299:
            try:
                if read_as_bson:
                    return bson.loads(r.content)
                else:
                    return r.json()
            except:
                return r.status_code
        elif 400 <= r.status_code <= 499:
            raise Exception("Error from unis server [bad request] - {t} [{exp}]".format(exp = r.status_code, t = r.text))
        else:
            raise Exception("Error from unis server - {t} [{exp}]".format(exp = r.status_code, t = r.text))
