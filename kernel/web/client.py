import asyncio
import json
import re
import requests
import ssl
import websockets

from netlogger import nllog

from ..psobject import schemas
from ..settings import MIME

class UnisClient(object, nllog.DoesLogging):
    def __init__(self, url, kwargs**):
        re_str = 'http[s]?://(?P<host>[^:/]+)(?::(?P<port>[0-9]{1,4}))$'
        if not re.compile(re_str).match(resource["href"]):
            raise ValueError("unis url is malformed")
        
        self._url = url
        self._verify = kwargs.get("verify", False)
        self._ssl = kwargs.get("cert", None)
        nllog.DoesLogging.__init__(self)
    
    def getResources(self):
        headers = { 'content-type': 'application/perfsonar+json',
                    'accept': MIME['PSJSON'] }
        return self._check_response(requests.get(self._url, verify = self._verify, cert = self._ssl))
        
    def get(self, url):
        args = self._get_conn_args(url)
        self.log.debug("unis-get <url={u} headers={h}>".format(u = args["url"], h = args["headers"]))
        return self._check_response(requests.get(args["url"], verify = self._verify, cert = self._ssl))
    
    def post(self, url, data):
        args = self._get_conn_args(url)
        if isinstance(data, dict):
            data = json.dumps(data)
        
        self.log.debug("unis-post <url={u} headers={h}  data={d}>".format(u = args["url"], 
                                                                          h = args["headers"], 
                                                                          data = data))
        return self._check_response(requests.post(args["url"], data = data, 
                                                  verify = self._verify, cert = self._ssl))
    
    def subscribe(self, collection, callback):
        asyncio.get_event_loop().run_until_complete(self._subscribe(collection, callback))
    @asyncio.coroutine
    def _subscribe(self, collection, callback):
        ctx = None
        if self._verify:
            ctx = ssl.create_default_context(cafile = self._verify)
        elif self._ssl:
            ctx = ssl.create_default_context(cafile = self._ssl[0])
        url = "ws{s}://{u}/subscribe/{c}".format(s = "s" if ctx else "", u = self._url, c = collection)
        ws = yield from websockets.client.connect(url, ssl = ctx)
        
        while True:
            resource = yield from ws.recv()
            try:
                resource = json.loads(resource)
            except Exception as exp:
                self.log.error("Could not load json - {e} - {v}".format(e = exp, v = resource))
                
            callback(resource)
        
        
    def _get_conn_args(self, url):
        re_str = "{full}|{rel}|{name}".format(full = 'http[s]?://(?P<host>[^:/]+)(?::(?P<port>[0-9]{1,4}))?/(?P<col1>[a-zA-Z]+)$',
                                              rel  = '#/(?P<col2>[a-zA-Z]+)$',
                                              name = '(?P<col3>[a-zA-Z]+)$')
        matches = re.compile(re_str).match(resource["href"])
        collection = matches.group("col1") or matches.group("col2") or matches.group("col3")
        return { "collection": collection,
                 "url": "{u}/{c}".format(u = self._url, c = args["collection"]),
                 "headers": { 'content-type': 'application/perfsonar+json profile=' + schemas.getRef(collection),
                              'accept': MIME['PSJSON'] } }
    
    def _check_response(self, r):
        self.log.debug("unis-response <code={c}>".format(c = r.status_code))
        if 200 <= r.status_code <= 299:
            try:
                return r.json()
            except:
                return r.status_code
        elif 400 <= r.status_code <= 499:
            raise Exception("Error from unis server [bad request] - {t} [{exp}]".format(exp = r.status_code, t = r.text))
        else:
            raise Exception("Error from unis server - {t} [{exp}]".format(exp = r.status_code, t = r.text))
