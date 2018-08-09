from collections import namedtuple

Event = namedtuple('Event', ('col', 'ty'))
def _reg(events):
    def _wrapper(f):
        f.rt_events = getattr(f, 'rt_events', [])
        f.rt_events.extend(events)
        return f
    return _wrapper

def new_event(cols):
    """
    :param cols: Name of the :class:`UnisCollection <unis.models.lists.UnisCollection>` associated with the event.
    :type cols: str or list[str]

    Decorator that associates a :class:`RuntimeService <unis.services.abstract.RuntimeService>` function with a 
    collection.  The decorated function will be registered as a callback with the collection.
    
    Decorated function invoked on when **new** resources are created. Decorated function should follow:
    
        **Parameters:**
        
        * **resource:** :class:`UnisObject <unis.models.models.UnisObject>` invoking the event.
    """
    cols = cols if isinstance(cols, list) else [cols]
    return _reg([Event(col, 'new') for col in cols])

def update_event(cols):
    """
    :param cols: Name of the :class:`UnisCollection <unis.models.lists.UnisCollection>` associated with the event.
    :type cols: str or list[str]

    Decorator that associates a :class:`RuntimeService <unis.services.abstract.RuntimeService>` function with a 
    collection.  The decorated function will be registered as a callback with the collection.
    
    Decorated function invoked on when resources are **updated**. Decorated function should follow:
    
        **Parameters:**
        
        * **resource:** :class:`UnisObject <unis.models.models.UnisObject>` invoking the event.
    """
    cols = cols if isinstance(cols, list) else [cols]
    return _reg([Event(col, 'update') for col in cols])

def delete_event(cols):
    """
    :param str cols: Name of the :class:`UnisCollection <unis.models.lists.UnisCollection>` associated with the event.
    :type cols: str or list[str]

    Decorator that associates a :class:`RuntimeService <unis.services.abstract.RuntimeService>` function with a 
    collection.  The decorated function will be registered as a callback with the collection.
    
    Decorated function invoked on when resources are **deleted**. Decorated function should follow:
    
        **Parameters:**
        
        * **resource:** :class:`UnisObject <unis.models.models.UnisObject>` invoking the event.
    """
    cols = cols if isinstance(cols, list) else [cols]
    return _reg([Event(col, 'delete') for col in cols])


