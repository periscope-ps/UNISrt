import json
import re
import sys

from unis.models import schemaLoader
from unis.models.lists import UnisCollection
from unis.rest import UnisClient, UnisError, UnisReferenceError

# The ObjectLayer converts json objects from UNIS into python objects and stores
# them in query-able collections.  Clients have access to find and update, but
# should only need to make use of the ObjectLayer.x interface where x is a
# resource.  For instance, appending new objects to the object collections has
# the expected effect of creating the related object in UNIS. Similarly, 
# objects update as needed when modified.
class ObjectLayer(object):
    class iCollection(object):
        def __init__(self, name, schema, model):
            re_str = "http[s]?://(?:[^:/]+)(?::[0-9]{1-5})?/(?:[^/]+/)*(?P<sname>[^/]+)#$"
            matches = re.compile(re_str).match(schema)
            assert(matches.group("sname"))
            self.name = name
            self.sname = matches.group("sname")
            self.uri = schema
            self.model = model
            
    def __init__(self, url, runtime=None, **kwargs):
        self.defer_update = False
        self._cache = {}
        self._models = {}
        self._addr = url
        self._unis = UnisClient(url, **kwargs)
        for resource in self._unis.getResources():
            re_str = "{full}|{rel}".format(full = 'http[s]?://(?P<host>[^:/]+)(?::(?P<port>[0-9]{1,5}))?/(?P<col1>[a-zA-Z]+)$',
                                           rel  = '#/(?P<col2>[a-zA-Z]+)$')
            matches = re.compile(re_str).match(resource["href"])
            collection = matches.group("col1") or matches.group("col2")
            
            schema = resource["targetschema"]["items"]["href"]
            model = schemaLoader.get_class(schema)
            self._models[collection] = self.iCollection(collection, schema, model)
            if collection not in ["events", "data"]:
                sync = runtime.settings["auto_sync"] if runtime else True
                self._cache[collection] = UnisCollection(resource["href"], collection, model, self, sync)
        
        if runtime:
            self._subscriber = runtime
            self.defer_update = runtime.settings["defer_update"]
    
    def __getattr__(self, n):
        if n in self._cache:
            return self._cache[n]
        else:
            raise AttributeError("{n} not found in ObjectLayer".format(n = n))
    
    # Returns weakref to cache object
    def find(self, href):
        re_str = "{full}|{rel}".format(full = '(?P<domain>http[s]?://[^:/]+(?::[0-9]{1,5}))/(?P<col1>[a-zA-Z]+)/(?P<uid1>\S+)$',
                                       rel  = '#/(?P<col2>[a-zA-Z]+)/(?P<uid2>[a-zA-Z0-9]+)$')
        matches = re.compile(re_str).match(href)
        
        if matches:
            domain = matches.group("domain")
            if domain and domain != self._addr:
                raise UnisReferenceError("Resource does not belong to the registered instance of UNIS", href)
                
            tmpCollection = matches.group("col1") or matches.group("col2")
            tmpUid = matches.group("uid1") or matches.group("uid2")
            if tmpCollection not in self._cache:
                raise ValueError("unknown collection {c} in href".format(c = tmpCollection))
            
            try:
                return self._cache[tmpCollection].where({"id": tmpUid})[0]
            except IndexError:
                tmpResource = self._unis.get(href)
                if tmpResource:
                    model = self._models[tmpCollection].model
                    tmpObject = model(tmpResource, self, local_only=False)
                    self._cache[tmpCollection].append(tmpObject)
                    return tmpObject
                else:
                    raise UnisError("href does not reference resource in unis.")
        else:
            raise ValueError("href must be a direct uri to a unis resource.")
    
    def update(self, resource):
        ref = "#/{c}".format(c = getattr(resource, "_collection", ""))
        try:
            tmpResponse = self._unis.post(ref, json.dumps(resource.to_JSON()))
        except:
            resource._pending = False
            raise
        
        self._cache[resource._collection].updateIndex(resource)
        resource._pending = False
    
    def insert(self, resource, uid=None):
        if isinstance(resource, dict):
            if "$schema" in resource:
                for k, item_meta in self._models.items():
                    if item_meta.uri == resource["$schema"]:
                        resource["id"] = resource.get("id", uid)
                        if not resource["id"]:
                            raise ValueError("Resource does not have a valid id attribute")
                        resource = item_meta.model(resource, self, local_only=False)
                        self._cache[item_meta.name].append(resource)
                        return resource
                raise ValueError("Unknown schema - {s}".format(s = resource["$schema"]))
            else:
                raise ValueError("No schema in dict, cannot continue")
        else:
            for k, item_meta in self._models.items():
                if isinstance(resource, item_meta.model):
                    resource.id = uid or resource.id
                    if not resource.id:
                        raise ValueError("Resource does not have a valid id attribute")
                    self._cache[item_meta.name].append(resource)
                    return resource
            raise ValueError("Resource type not found in ObjectLayer")
    
    def subscribe(self, runtime):
        self._subscriber = runtime
        self.defer_update = runtime.settings["defer_update"]
    
    def _publish(self, ty, resource):
        if self._subscriber:
            self._subscriber._publish(ty, resource)
    def about(self):
        return [v.uri for k,v in self._models.items()]
        
    def shutdown(self):
        self._unis.shutdown()
    def __enter__(self):
        return self
    def __exit__(self, type, value, traceback):
        self.shutdown()
