import json
import re
import sys

from unis.models import schemaLoader
from unis.models.lists import UnisCollection
from unis.rest import UnisClient, UnisError

# The ObjectLayer converts json objects from UNIS into python objects and stores
# them in query-able collections.  Clients have access to find and update, but
# should only need to make use of the ObjectLayer.x interface where x is a
# resource.  For instance, appending new objects to the object collections has
# the expected effect of creating the related object in UNIS. Similarly, 
# objects update as needed when modified.
class ObjectLayer(object):
    class iCollection(object):
        def __init__(self, name, schema, model):
            re_str = "http[s]?://(?:[^:/]+)(?::[0-9]{1-4})?/(?:[^/]+/)*(?P<sname>[^/]+)#$"
            matches = re.compile(re_str).match(schema)
            assert(matches.group("sname"))
            self.name = name
            self.sname = matches.group("sname")
            self.uri = schema
            self.model = model
            
    def __init__(self, url, runtime=None, **kwargs):
        self.defer_update = False
        self.__cache__ = {}
        self.__models__ = {}
        self._unis = UnisClient(url, **kwargs)
        for resource in self._unis.getResources():
            re_str = "{full}|{rel}".format(full = 'http[s]?://(?P<host>[^:/]+)(?::(?P<port>[0-9]{1,4}))?/(?P<col1>[a-zA-Z]+)$',
                                           rel  = '#/(?P<col2>[a-zA-Z]+)$')
            matches = re.compile(re_str).match(resource["href"])
            collection = matches.group("col1") or matches.group("col2")
            
            schema = resource["targetschema"]["items"]["href"]
            model = schemaLoader.get_class(schema)
            self.__models__[collection] = self.iCollection(collection, schema, model)
            if collection not in ["events", "data"]:
                self.__cache__[collection] = UnisCollection(resource["href"], collection, model, self)
        
        if runtime:
            self.__subscriber__ = runtime
            self.defer_update = runtime.settings["defer_update"]
    
    def __getattr__(self, n):
        if n in self.__cache__:
            return self.__cache__[n]
        else:
            raise AttributeError("{n} not found in ObjectLayer".format(n = n))
    
    # Returns weakref to cache object
    def find(self, href):
        re_str = "{full}|{rel}".format(full = 'http[s]?://(?P<host>[^:/]+)(?::(?P<port>[0-9]{1,4}))?/(?P<col1>[a-zA-Z]+)/(?P<uid1>\S+)$',
                                       rel  = '#/(?P<col2>[a-zA-Z]+)/(?P<uid2>[a-zA-Z0-9]+)$')
        matches = re.compile(re_str).match(href)
        
        if matches:
            tmpCollection = matches.group("col1") or matches.group("col2")
            tmpUid = matches.group("uid1") or matches.group("uid2")
            if tmpCollection not in self.__cache__:
                raise ValueError("unknown collection {c} in href".format(c = tmpCollection))
            
            if tmpUid not in self.__cache__[tmpCollection]:
                tmpResource = self._unis.get(href)
                if tmpResource:
                    model = self.__models__[tmpCollection].model
                    tmpObject = model(tmpResource, self, local_only=False)
                    self.__cache__[tmpCollection][tmpUid] = tmpObject
                else:
                    raise UnisError("href does not reference resource in unis.")
            
            return self.__cache__[tmpCollection][tmpUid]
        else:
            raise ValueError("href must be a direct uri to a unis resource.")
    
    def update(self, resource):
        if getattr(resource, "selfRef", None):
            tmpResponse = self._unis.post(resource.selfRef, json.dumps(resource.to_JSON()))
        else:
            ref = None
            for k, item_meta in self.__models__.items():
                if isinstance(resource, item_meta.model):
                    ref = "#/{c}".format(c = k)
            if ref:
                tmpResponse = self._unis.post(ref, json.dumps(resource.to_JSON()))
        if tmpResponse:
            resource._pending = False
    
    def insert(self, resource, uid=None):
        if isinstance(resource, dict):
            if "$schema" in resource:
                for k, item_meta in self.__models__.items():
                    if item_meta.uri == resource["$schema"]:
                        resource = item_meta.model(resource, self, local_only=False)
                        self.__cache__[item_meta.name][resource.id] = resource
                        return self.__cache__[item_meta.name][resource.id]
        else:
            for k, item_meta in self.__models__.items():
                if isinstance(resource, item_meta.model):
                    resource.id = uid or resource.id
                    if not resource.id:
                        raise ValueError("Resource does not have a valid id attribute")
                    self.__cache__[item_meta.name][resource.id] = resource
                    return self.__cache__[item_meta.name][resource.id]
            raise ValueError("Resource type not found in ObjectLayer")
    
    def subscribe(self, runtime):
        self.__subscriber__ = runtime
        self.defer_update = runtime.settings["defer_update"]
    
    def _publish(self, ty, resource):
        if self.__subscriber__:
            self.__subscriber__._publish(ty, resource)
    def about(self):
        return [v.uri for k,v in self.__models__.items()]
        
    def shutdown(self):
        self._unis.shutdown()
    def __enter__(self):
        return self
    def __exit__(self, type, value, traceback):
        self.shutdown()
