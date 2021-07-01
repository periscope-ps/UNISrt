from functools import wraps

class _AlwaysFalse(object):
    def __eq__(a,b): return False
    def __ne__(a,b): return False
    def __gt__(a,b): return False
    def __ge__(a,b): return False
    def __lt__(a,b): return False
    def __le__(a,b): return False
    def __contains__(a,b): return False
    def __regex__(a,b): return False
    def __bool__(self): return False
_false = _AlwaysFalse()

def decent_prop(v, n):
    if not n:
        msg = "Dotted name may not be empty"
        logging.getLogger("unis.utils").warn(msg)
        raise ValueError(msg)
    try:
        for p in n.split("."):
            v = getattr(v, p)
    except AttributeError: return _false
    return v

def ident(a, b=None):
    self, fn = a, (a if not b else b)
    if self is not fn:
        @wraps(fn)
        def _f(*args, **kwargs):
            fn(*args, **kwargs)
            return self
    else:
        @wraps(fn)
        def _f(self, *args, **kwargs):
            fn(self, *args, **kwargs)
            return self
    return _f
