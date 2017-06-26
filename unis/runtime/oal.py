import json
import re
import sys
import time
import uuid

from unis.models import schemaLoader
from unis.models.lists import UnisCollection
from unis.rest import UnisClient, UnisError, UnisReferenceError
from unis import logging

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
            
    @logging.debug("OAL")
    def __init__(self, url, runtime, **kwargs):
        self.defer_update = False
        self._cache = {}
        self._models = {}
        self._addr = url
        self._unis = UnisClient(url, inline=runtime.settings["inline"], **kwargs)
        self._pending = set()
        self.defer_update = runtime.settings["defer_update"]
        
        for resource in self._unis.getResources():
            re_str = "{full}|{rel}".format(full = 'http[s]?://(?P<host>[^:/]+)(?::(?P<port>[0-9]{1,5}))?/(?P<col1>[a-zA-Z]+)$',
                                           rel  = '#/(?P<col2>[a-zA-Z]+)$')
            matches = re.compile(re_str).match(resource["href"])
            collection = matches.group("col1") or matches.group("col2")
            
            schema = resource["targetschema"]["items"]["href"]
            model = schemaLoader.get_class(schema)
            self._models[collection] = self.iCollection(collection, schema, model)
            if collection not in ["events", "data"]:
                self._cache[collection] = UnisCollection(resource["href"], collection, model, self, runtime.settings["auto_sync"], runtime.settings["subscribe"])
        
    
    def __getattr__(self, n):
        if n in self._cache:
            return self._cache[n]
        else:
            raise AttributeError("{n} not found in ObjectLayer".format(n = n))
    
    @logging.debug("OAL")
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
            
            if self._cache[tmpCollection].hasValue("id", tmpUid):
                return list(self._cache[tmpCollection].where({"id": tmpUid}))[0]
            else:
                tmpResource = self._unis.get(href)
                if tmpResource:
                    model = schemaLoader.get_class(tmpResource["$schema"])
                    tmpObject = model(tmpResource, self, local_only=False)
                    tmpObject = self._cache[tmpCollection].append(tmpObject)
                    return tmpObject
                else:
                    raise UnisError("href does not reference resource in unis.")
        else:
            raise ValueError("href must be a direct uri to a unis resource - Got", href)
    
    @logging.info("OAL")
    def flush(self):
        if not self._pending:
            return
        
        requests = {}
        cols = {}
        roots = []
        
        # Generate dependency tree
        for obj in self._pending:
            collectionName = obj._collection
            if collectionName not in cols:
                roots.append(collectionName)
                cols[collectionName] = set()
                
            pending = obj._waiting_on & self._pending
            if pending:
                if collectionName in roots:
                    roots.remove(collectionName)
                for dependent in pending:
                    dependentCol = dependent._collection
                    if dependentCol not in cols:
                        roots.append(dependentCol)
                        cols[dependentCol] = set()
                    cols[collectionName].add(dependentCol)
            
            if collectionName in requests:
                requests[collectionName].append(obj)
            else:
                requests[collectionName] = [obj]
            
        for col in roots:
            cols[col] = None
            
        # Order requests
        do_once = True
        while do_once or roots:
            do_once = False
            for col in roots:
                self._do_update(requests[col], col)
                for dependentCol, s in cols.items():
                    if isinstance(s, set):
                        s = s - set([col])
                        if not s:
                            roots.append(dependentCol)
                            cols[dependentCol] = None
                        else:
                            cols[dependentCol] = s
            roots = []
            shortest = None
            for col, s in cols.items():
                if isinstance(s, set):
                    if shortest is None or len(shortest) > len(s):
                        shortest = s
            
            if shortest:
                roots = list(shortest)
        
        self._pending = set()
        
    @logging.info("OAL")
    def update(self, resource):
        if resource.isDeferred() or self.defer_update:
            self._pending.add(resource)
        if resource not in self._pending:
            self._pending.add(resource)
            self._do_update([resource], resource._collection)
            self._pending.remove(resource)
    @logging.debug("OAL")
    def remove(self, resource):
        self._cache[resource._collection].remove(resource)
    @logging.debug("OAL")
    def _do_update(self, resources, collection):
        ref = "#/{c}".format(c=collection)
        self._cache[collection].locked = True
        msg = []
        response = []
        try:
            for resource in resources:
                if not getattr(resource, "id", None):
                    resource.setWithoutUpdate("id", str(uuid.uuid4()))
                resource.validate()
                resource.setWithoutUpdate("ts", int(time.time() * 1000000))
                msg.append(resource.to_JSON())
            response = self._unis.post(ref, json.dumps(msg))
        except:
            raise
        finally:
            for resource in resources:
                response = response if isinstance(response, list) else [response]
                for resp in response:
                    if resp["id"] == resource.id:
                        if resp["selfRef"] != getattr(resource, "selfRef", None):
                            resource.selfRef = resp["selfRef"]
                            resource.commit("selfRef")
                resource._pending = False
                self._cache[collection].updateIndex(resource)
            self._cache[collection].locked = False
    
    @logging.info("OAL")
    def insert(self, resource, uid=None):
        if isinstance(resource, dict):
            if "$schema" in resource:
                model = schemaLoader.get_class(resource["$schema"])
                resource = model(resource)
            else:
                raise ValueError("No schema in dict, cannot continue")
            
        resource.id = uid or getattr(resource, "id", None)
        col = self.getModel(resource.names)
        resource = col.append(resource)
        return resource
        
    @logging.info("OAL")
    def getModel(self, names):
        if not isinstance(names, list):
            names = [names]
        for name in names:
            for k, item_meta in self._models.items():
                if item_meta.model._schema["name"] == name:
                    return self._cache[item_meta.name]
        
        raise ValueError("Resource type {n} not found in ObjectLayer".format(n=names))
    
    def about(self):
        return [v.uri for k,v in self._models.items()]
        
    def shutdown(self):
        if self._pending:
            self.flush()
        
        self._unis.shutdown()
    def __enter__(self):
        return self
    def __exit__(self, type, value, traceback):
        self.shutdown()
    def __contains__(self, resource):
        for k, item_meta in self._models.items():
            if item_meta.model._schema["name"] in resource.names:
                return resource in self._cache[item_meta.name]
