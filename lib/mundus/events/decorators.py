import logging

from collections import namedtuple

from mundus.events.ty import types
from mundus.events.vm import FunctionAnalyzer as Parser

__all__ = ("new_event", "update_event", "delete_event", "publish_event",
           "prepush_event", "postpush_event", "data_event", "depends_on")

_access = namedtuple('access', ('reads', 'writes', 'deletes'))

"""
class _rt_type(object):
    def __init__(self, tys): self._rt_types = tys

_base_types = ['null', 'string', 'boolean', 'number', 'integer']
_type = namedtuple('path_type', ('path', 'ty'))
def _lift(cls):
    if hasattr(cls, '_rt_types'): return cls
    elif '$ref' in cls:
        return schemaLoader.get_class(cls['$ref'])
    elif 'type' in cls:
        if cls['type'] in _base_types:
            return cls['type']
        elif cls['type'] == 'object':
            return _rt_type({k:v for k,v in cls.get('properties', {}).items()})
        else: # cls['type'] == 'list'
            return [_lift(p) for p in cls.get('items', {}).get('anyOf', [])]
    else:
        return 'null'

def _get_type(full_path, ty):
    paths = full_path.split('.')
    
    h_ty = ty
    for i,head in enumerate(paths[1:]):
        if not hasattr(h_ty, '_rt_types') or head not in h_ty._rt_types: break
        h_ty = _lift(h_ty._rt_types[head])
        if isinstance(h_ty, list):
            return _type(full_path, [(ty, paths[:i+1])] + [_get_type(".".join(paths[i+1:]), ty).ty for ty in h_ty])
            
        else:
            if h_ty in _base_types: break
            if hasattr(h_ty, '_rt_defaults'):
                return _type(full_path, [(ty, paths[:i+1])] + _get_type(".".join(paths[i+1:]), h_ty).ty)
    return _type(full_path, (ty, paths))
"""

def _get_type_info(fn, ty):
    (reads, writes, deletes) = fn.access
    fn.access = _access(['.'.join(p.split('.')[1:]) for p in reads],
                        ['.'.join(p.split('.')[1:]) for p in writes],
                        ['.'.join(p.split('.')[1:]) for p in deletes])

def depends_on(other):
    def _f(fn):
        fn.depends_on = [other]
        if hasattr(fn, 'depends_on'):
            fn.depends_on += fn.depends_on
        return fn

def event(ty, *channels):
    def _f(fn):
        if hasattr(fn, 'ty') and issubclass(ty, fn.ty):
            ty = fn.ty
        if hasattr(fn, 'ty') and not issubclass(fn.ty, ty):
            logging('unis.events').error(f"Listeners may only register a single type [{ty} != {fn.ty}]")
        else:
            fn.ty, fn.ch = ty, channels
            if hasattr(fn, 'ch'):
                fn.ch += fn.ch
            if not hasattr(fn, 'access'):
                fn.access = _access(*Parser(fn).find_dependencies())
                _get_type_info(fn, ty)
        return fn
    return _f

def create_event(ty):
    return event(ty, types.CREATE)

def new_event(ty):
    return event(ty, types.NEW)

def update_event(ty):
    return event(ty, types.UPDATE)

def delete_event(ty):
    return event(ty, types.DELETE)

def publish_event(ty):
    return event(ty, types.PUBLISH)

def prepush_event(ty):
    return event(ty, types.PREPUSH)

def postpush_event(ty):
    return event(ty, types.POSTPUSH)

def data_event(ty):
    return event(ty, types.DATA)
