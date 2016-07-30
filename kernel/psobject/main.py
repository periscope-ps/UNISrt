import json
import re
import sys

from kernel.psobject import factory
from kernel.psobject import schemas
from kernel.psobject.objects import UnisObject
from kernel.psobject.lists import UnisCollection
from kernel.web import UnisClient, UnisError

# The ObjectLayer converts json objects from UNIS into python objects and stores
# them in query-able collections.  Clients have access to find and update, but
# should only need to make use of the ObjectLayer.x interface where x is a
# resource.  For instance, appending new objects to the object collections has
# the expected effect of creating the related object in UNIS. Similarly, 
# objects update as needed when modified.
class ObjectLayer(object):
    class iCollection(object):
        def __init__(self, name, schema):
            re_str = "http[s]?://(?:[^:/]+)(?::[0-9]{1-4})?/(?:[^/]+/)*(?P<sname>[^/]+)#$"
            matches = re.compile(re_str).match(schema)
            assert(matches.group("sname"))
            self.name = name
            self.sname = matches.group("sname")
            self.schema = schema
            
    def __init__(self, url, **kwargs):
        self.__cache__ = {}
        self.__schema__ = {}
        self._unis = UnisClient(url, **kwargs)
        for resource in self._unis.getResources():
            re_str = "{full}|{rel}".format(full = 'http[s]?://(?P<host>[^:/]+)(?::(?P<port>[0-9]{1,4}))?/(?P<col1>[a-zA-Z]+)$',
                                           rel  = '#/(?P<col2>[a-zA-Z]+)$')
            matches = re.compile(re_str).match(resource["href"])
            collection = matches.group("col1") or matches.group("col2")
            
            schema = resource["targetschema"]["items"]["href"]
            self.__schema__[collection] = self.iCollection(collection, schema)
            schemas.get(schema)
            if collection not in ["events", "data"]:
                setattr(self, self.__schema__[collection].sname.title(), lambda c=collection: "This is a " + c + "...")
                self.__cache__[collection] = UnisCollection(resource["href"], collection, schema, self)
    
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
                    tmpObject = UnisObject(tmpResource, self, False, False)
                    self.__cache__[tmpCollection][tmpUid] = tmpObject
                else:
                    raise UnisError("href does not reference resource in unis.")
            else:
                tmpObject = self.__cache__[tmpCollection][tmpUid]
            
            return factory.reference(tmpObject)
        else:
            raise ValueError("href must be a direct uri to a unis resource.")
    
    def update(self, resource):
        resource.validate()
        tmpResponse = self._unis.post(resource.selfRef, json.dumps(resource.to_JSON()))
        if tmpResponse:
            resource._pending = False
    
    def about(self):
        return self.__schema__.values()

    def shutdown(self):
        self._unis.shutdown()
    def __enter__(self):
        return self
    def __exit__(self, type, value, traceback):
        self.shutdown()
