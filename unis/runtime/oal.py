import json
import sys
import time
import uritools
import uuid

from unis.models import schemaLoader
from unis.models.lists import UnisCollection
from unis.rest import UnisProxy, UnisError, UnisReferenceError
from unis import logging

# The ObjectLayer converts json objects from UNIS into python objects and stores
# them in query-able collections.  Clients have access to find and update, but
# should only need to make use of the ObjectLayer.x interface where x is a
# resource.  For instance, appending new objects to the object collections has
# the expected effect of creating the related object in UNIS. Similarly, 
# objects update as needed when modified.
class ObjectLayer(object):
    @logging.debug("OAL")
    def __init__(self, runtime):
        self.defer_update = False
        self._cache = {}
        self._models = []
        self._unis = UnisProxy(runtime.settings["unis"], inline=runtime.settings["inline"])
        self._pending = set()
        self.defer_update = runtime.settings["defer_update"]
        for resource in self._unis.getResources():
            collection = uritools.urisplit(resource['href']).getpath().split('/')[-1]
            if collection not in self._cache:
                schema = resource["targetschema"]["items"]["href"]
                model = schemaLoader.get_class(schema)
                self._models.append({ "sname": uritools.urisplit(schema).getpath().split('/')[-1], "name": collection, "uri": schema, "model": model })
                if collection not in ["events", "data"]:
                    self._cache[collection] = UnisCollection(resource["href"], collection, model, self, runtime.settings["auto_sync"], runtime.settings["subscribe"])
                    if model in runtime.settings["preload"]:
                        self._cache[collection].sync()
        
    
    def __getattr__(self, n):
        if n in self._cache:
            return self._cache[n]
        else:
            raise AttributeError("{n} not found in ObjectLayer".format(n = n))
    
    @logging.debug("OAL")
    def find(self, href):
        parts = uritools.urisplit(href)
        if len(parts.path.split('/')) > 1:
            collection, uid = parts.path.split('/')[-2:]
            if collection not in self._cache:
                raise ValueError("unknown collection {c} in href".format(c = collection))
            
            if self._cache[collection].hasValue("id", uid):
                return list(self._cache[collection].where({"id": uid}))[0]
            else:
                tmpResource = self._unis.get(href)
                if tmpResource:
                    model = schemaLoader.get_class(tmpResource[0]["$schema"])
                    tmpObject = model(tmpResource[0], self, local_only=False)
                    self._cache[collection].append(tmpObject)
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
            collectionName = obj.getCollection()
            if collectionName not in cols:
                roots.append(collectionName)
                cols[collectionName] = set()
                
            pending = obj._waiting_on & self._pending
            if pending:
                if collectionName in roots:
                    roots.remove(collectionName)
                for dependent in pending:
                    dependentCol = dependent.getCollection()
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
            self._do_update([resource], resource.getCollection())
            self._pending.remove(resource)
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
                msg.append(resource)
            response = self._unis.post(msg)
        except:
            raise
        finally:
            if response:
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
        col.append(resource)
        return resource
        
    @logging.info("OAL")
    def getModel(self, names):
        for record in self._models:
            if record['model']._schema['name'] in list(names):
                return self._cache[record['name']]
        
        raise ValueError("Resource type {n} not found in ObjectLayer".format(n=names))
    
    def about(self):
        return [v['uri'] for v in self._models]
        
    def shutdown(self):
        if self._pending:
            self.flush()
        
        self._unis.shutdown()
    def __enter__(self):
        return self
    def __exit__(self, type, value, traceback):
        self.shutdown()
    def __contains__(self, resource):
        for record in self._models:
            if record['model']._schema['name'] in resource.names:
                return True if isinstance(resource, type) else resource in self._cache[record['name']]
        return False
