import json
import re
import requests

def get(schema):
    if schema not in CACHE:
        try:
            tmpResponse = requests.get(schema)
            CACHE[schema] = tmpResonse.json()
            return CACHE[schema]
        except Exception as exp:
            raise Exception("Failed to load remote schema - {s}".format(s = exp))

def fromName(name):
    for schema in CACHE.keys():
        re_str = 'http[s]?://(?P<host>[^:/]+)(?::(?P<port>[0-9]{1,4}))?/{s}$'.format(s = n)
        exists = re.search(re_str, schema)
        if exists:
            return CACHE[schema]
    return None

def getRef(name):
    for schema in CACHE.keys():
        re_str = 'http[s]?://(?P<host>[^:/]+)(?::(?P<port>[0-9]{1,4}))?/{s}$'.format(s = n)
        exists = re.search(re_str, schema)
        if exists:
            return schema
    return None


CACHE = {}
try:
    SCHEMAS = [ "http://json-schema.org/draft-03/schema#",
            "http://json-schema.org/draft-04/hyper-schema#",
            "http://json-schema.org/draft-04/links#" ]
    for schema in SCHEMAS:
        tmpResponse = requests.get(schema)
        CACHE[schema] = tmpResponse.json()
except Exception as exp:
    print("Failed to load remote schema - {s}".format(s = exp))
