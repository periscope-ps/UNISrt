import json
import bson
import re
import requests
import websocket

from concurrent.futures import ThreadPoolExecutor

from unis.runtime.settings import MIME
from unis import logging

class UnisError(Exception):
    pass

class UnisReferenceError(UnisError):
    def __init__(self, msg, href):
        super(UnisReferenceError, self).__init__(msg)
        self.href = href

class UnisClient(object):
    @logging.debug("UnisClient")
    def __init__(self, url, inline=False, **kwargs):
        re_str = 'http[s]?://(?P<host>[^:/]+)(?::(?P<port>[0-9]{1,5}))?$'
        if not re.compile(re_str).match(url):
            raise ValueError("unis url is malformed")
        
        self._url = url
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
    def get(self, url, limit = None, **kwargs):
        args = self._get_conn_args(url)
        args["url"] = self._build_query(args, inline=self._inline, limit=limit, **kwargs)
        return self._check_response(requests.get(args["url"], verify = self._verify, cert = self._ssl, headers=args["headers"]), False)
    
    @logging.info("UnisClient")
    def post(self, url, data):
        args = self._get_conn_args(url)
        if isinstance(data, dict):
            data = json.dumps(data)
        
        return self._check_response(requests.post(args["url"], data = data, 
                                                  verify = self._verify, cert = self._ssl), False)
    
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
    @loging.debug("UnisClient")
    def _subscribe(self, collection):
        kwargs = {}
        if self._ssl:
            kwargs["ca_certs"] = self._ssl[0]
            
        re_str = 'http[s]?://(?P<host>[^:/]+)(?::(?P<port>[0-9]{1,4}))$'
        matches = re.match(re_str, self._url)
        url = "ws{s}://{h}:{p}/subscribe/{c}".format(s = "s" if "ca_certs" in kwargs else "", 
                                                     h = matches.group("host"), 
                                                     p = matches.group("port"), 
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
                                              on_error = lambda ws, error: raise UnisError("Error from websocket")
                                              on_close = lambda ws: None

        self._executor.submit(self._socket.run_forever, sslopt=kwargs)
        
    @logging.debug("UnisClient")
    def _build_query(self, args, inline=False, **kwargs):
        if kwargs:
            q = ""
            for k,v in kwargs.items():
                if v:
                    q += "{k}={v}&".format(k = k, v = v)
                
            if inline:
                q += "inline"
            else:
                q = q[0:-1]
            return "{b}?{q}".format(b=args["url"], q=q)
        return args["url"]
    
    @logging.debug("UnisClient")
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
