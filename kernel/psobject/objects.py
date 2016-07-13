import datetime
import jsonschema

import .schemas
import .schemas.CACHE

class EmbeddedObject(UnisObject):
    def __init__(self, src, runtime):
        super(EmbeddedObject, self).__init__(src, runtime, True, False)

class UnisObject(JSONObject):
    def __init__(self, src, runtime, defer_update, virtual):
        # Ensure non-virtual object source has a unis reference.
        if not virtual and "selfRef" not in src:
            raise ValueError("Unis object does not contain selfRef")
        if not virtual and "ts" not in src:
            raise ValueError("Unis object does not contain timestamp")
        self._defer = defer_update
        self._virtual = virtual
        self._dirty = False
        self._pending = False
        super(UnisObject, self).__init__(src, runtime)
        
        
    def __getattribute(self, n):
        virtual_name = "_{n}".format(n = n)
        if self.__dict__.get(virtual_name, None):
            return self.__dict__[virtual_name]
        else:
            super(UnisObject, self)._getattribute(n)
    
    def __setattr__(self, n, v):
        super(UnisObject, self).__setattr__(n, v)
        self.update()
        
    def update(self):
        if not self._virtual and self._schema and \
           not (isinstance(v, UnisObject) and getattr(v, "_virtual", True)):
            if not self._defer and not self._dirty:
                self._pending = self._dirty = True
                self._runtime.update(self)
            else:
                self._dirty = True
        
    def flush(self):
        if self._dirty and not self._pending:
            self._pending = self._dirty = True
            self._runtime.update(self)
        
    def validate(self):
        if self._schema:
            resolver = jsonschema.RefResolver(self._schema["id"], self._schema, schemas.CACHE)
            jsonschema.validate(self.__json__(), self._schema, resolver)
        else:
            raise AttributeError("No schema found for object")
        
# For internal use only
class JSONObject(object):
    def __init__(self src, runtime):
        self._lasttouched = datetime.datetime.utcnow()
        self.__dict__ = source
        self._runtime = runtime
        self.__cache__ = {}
        
    def __getattribute__(self, n):
        self._lasttouched = datetime.datetime.utcnow()
        
        v = object.__getattribute__(self, n)
        if isinstance(v, dict):
            return self._resolve_dict(v, n)
        elif isinstance(v, list):
            return self._resolve_list(v, n)
        else:
            return v
            
    def __setattr__(self, n, v):
        # If the attribute is a UnisObject - i.e. it refers to a descrete resource in UNIS
        # create a dictionary that conforms to the json 'link' schema.
        self._lasttouched = datetime.datetime.utcnow()
        if isinstance(v, EmbeddedObject):
            self.__dict__[n] = v.__json__()
        elif isinstance(v, UnisObject):
            self.__dict__[n] = { "rel": "full", "href": v.selfRef }
        super(JSONObject, self).__setattr__(n ,v)
        
    def _resolve_list(self, ls, n):
        tmpResult = []
        for i in ls:
            if isinstance(i, dict):
                tmpResult.append(self._resolve_dict(i, n))
            elif isinstance(i, list):
                tmpResult.append(self._resolve_list(i, n))
            else:
                tmpResult.append(i)
        return tmpResult
        
    def _resolve_dict(self, o, n):
        if "href" in o:
            return self._runtime.find(o["href"])
        else:
            # Convert object and cache
            self.__dict__[n] = JSONObject(o, self._runtime)
            return self.__dict__[n]
    
    def __json__(self):
        tmpResult = {}
        for k, v in self.__dict__.iteritems():
            if k[0] != "_":
                if isinstance(v, JSONObject) and not v.Virtual:
                    tmpResult[k] = v.__json__()
                elif not isinstance(v, JSONObject):
                    tmpResult[k] = v
        return tmpResult
