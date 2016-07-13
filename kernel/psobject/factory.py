import weakref

import .schemas

from .objects import UnisObject, EmbeddedObject

def newObject(src, runtime, defer_update=False, virtual=False):
    return UnisObject(src, runtime, defer_update, virtual, embedded)
def newEmbeddedObject(src, runtime):
    return EmbeddedObject(src, runtime)

def validate(obj, schema):
    obj.__dict__["_schema"] = schemas.get(schema)
    obj.validate()

def remoteObject(obj):
    return isinstance(obj, UnisObject) and \
        not obj._virtual and \
        getattr(obj, "_schema", None) != None

def addLocalProperty(obj, n, v = None):
    reserved = ["pending", "defer", "virtual", 
                "dirty", "runtime", "embedded", "lasttouched"]
    if n in reserved:
        error = "Cannot assign to {n}, {n} is a keyword".format(n = n)
        raise AttributeError(error)
        
    obj.__dict__["_{n}".format(n = n)] = v

def setDeferred(obj, v):
    obj.__dict__["_defer"] = v
def isDeferred(obj):
    return obj.__dict__["_defer"]

def isVirtual(obj):
    return obj.__dict__["_virtual"]

def reference(obj):
    r = weakref.ref(obj)
    return r()
def refcount(obj):
    return weakref.getweakrefcount(obj)

def lastTouched(obj):
    return obj.__dict__["_lasttouched"]
