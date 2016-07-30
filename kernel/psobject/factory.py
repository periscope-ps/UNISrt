import weakref

from kernel.psobject import schemas
from kernel.psobject.objects import UnisObject

def newObject(src, runtime, defer_update=False, virtual=False):
    return UnisObject(src, runtime, defer_update, virtual)

def validate(obj, schema):
    obj._schema = schemas.get(schema)
    obj.validate()

def remoteObject(obj):
    return isinstance(obj, UnisObject) and \
        not obj._local

def setDeferred(obj, v):
    obj._defer = v
def isDeferred(obj):
    return obj._defer

def isPending(obj, v):
    return obj._pending

def isVirtual(obj):
    return obj._local

def reference(obj):
    r = weakref.ref(obj)
    return r()
def refcount(obj):
    return weakref.getweakrefcount(obj)

def lastTouched(obj):
    return obj._lasttouched
