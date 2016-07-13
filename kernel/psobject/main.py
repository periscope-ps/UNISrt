import json
import re

import .schemas

from .objects import UnisObject
from .lists import UnisList
from ..web import UnisClient

# The ObjectLayer converts json objects from UNIS into python objects and stores
# them in query-able collections.  Clients have access to find and update, but
# should only need to make use of the ObjectLayer.x interface where x is a
# resource.  For instance, appending new objects to the object collections has
# the expected effect of creating the related object in UNIS. Similarly, 
# objects update as needed when modified.
class ObjectLayer(object):
    def __init__(self, kwargs**):
        self._unis = UnisClient(kwargs**)
        for resource in self._unis.getResources():
            re_str = "{full}|{rel}".format(full = 'http[s]?://(?P<host>[^:/]+)(?::(?P<port>[0-9]{1,4}))?/(?P<col1>[a-zA-Z]+)$',
                                           rel  = '#/(?P<col2>[a-zA-Z]+)$')
            matches = re.compile(re_str).match(resource["href"])
            collection = matches.group("col1") or matches.group("col2")
            
            schema = resource["targetschema"]["items"]["href"]
            schemas.get(schema)
            if collection not in ["events", "data"]:
                self.__cache__ = UnisList(resource["href"], schema, collection, self)
    
    def __getattribute__(self, n):
        if n in __cache__:
            self.__cache__[n]
        else:
            super(UnisRuntime, self).__getattribute__(n)
    
    # Returns weakref to cache object
    def find(self, href):
        re_str = "{full}|{rel}".format(full = 'http[s]?://(?P<host>[^:/]+)(?::(?P<port>[0-9]{1,4}))?/(?P<col1>[a-zA-Z]+)/(?P<uid1>\S+)$',
                                       rel  = '#/(?P<col2>[a-zA-Z]+)/(?P<uid2>\S+)$')
        matches = re.compile(re_str).match(href)
        
        if matches:
            tmpCollection = matches.group("col1") or matches.group("col2")
            tmpUid = matches.group("uid1") or matches.group("uid2")
            if tmpCollection not in self.__cache__:
                raise ValueError("unknown collection {c} in href".format(c = tmpCollection))
                
            if tmpUid not in self.__cache__[tmpCollection]:
                tmpResource = self._unis.get(href)
                if tmpResource:
                    tmpObject = UnisObject(tmpResource, self)
                    self.__cache__[matches.group("collection")][matches.group("uid")] = tmpObject
                else:
                    raise ValueError("href does not reference resource in unis.")
            else:
                tmpObject = self.__cache__[tmpCollection][tmpUid]
            
            return unisobject.reference(tmpObject)
        else:
            raise ValueError("href must be a direct uri to a unis resource.")
    
    def update(self, resource):
        resource.validate()
        tmpResponse = self._unis.post(resource.selfRef, json.dumps(resource))
        if tmpResponse:
            resource.Pending = False
