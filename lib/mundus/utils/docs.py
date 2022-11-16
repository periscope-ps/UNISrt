from functools import wraps

def annotate(v):
    def _inner(fn):
        fn.__doc__ += v
        return fn
    return _inner
