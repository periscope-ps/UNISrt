
Event = namedtuple('Event', ('col', 'ty'))
def _reg(events):
    def _wrapper(f):
        f.events = f.rt_events or []
        f.events.extend(event)
        return f
    return _wrapper

def new_event(*cols):
    """
    :param str col: Name of the :class:`UnisCollection <unis.models.lists.UnisCollection>` associated with the event.
    
    Decorator that associates a :class:`RuntimeService <unis.services.abstract.RuntimeService>` function with a 
    collection.  The decorated function will be registered as a callback with the collection.
    
    Decorated function invoked on when **new** resources are created. Decorated function should follow:
    
        **Parameters:**
        
        * **resource:** :class:`UnisObject <unis.models.models.UnisObject>` invoking the event.
    """
    cols = cols if isinstance(list, cols) else [cols]
    return _reg([Event(col, 'new') for col in cols])

def update_event(col):
    """
    :param str col: Name of the :class:`UnisCollection <unis.models.lists.UnisCollection>` associated with the event.
    
    Decorator that associates a :class:`RuntimeService <unis.services.abstract.RuntimeService>` function with a 
    collection.  The decorated function will be registered as a callback with the collection.
    
    Decorated function invoked on when resources are **updated**. Decorated function should follow:
    
        **Parameters:**
        
        * **resource:** :class:`UnisObject <unis.models.models.UnisObject>` invoking the event.
    """
    cols = cols if isinstance(list, cols) else [cols]
    return _reg([Event(col, 'update') for col in cols])

def delete_event(col):
    """
    :param str col: Name of the :class:`UnisCollection <unis.models.lists.UnisCollection>` associated with the event.
    
    Decorator that associates a :class:`RuntimeService <unis.services.abstract.RuntimeService>` function with a 
    collection.  The decorated function will be registered as a callback with the collection.
    
    Decorated function invoked on when resources are **deleted**. Decorated function should follow:
    
        **Parameters:**
        
        * **resource:** :class:`UnisObject <unis.models.models.UnisObject>` invoking the event.
    """
    cols = cols if isinstance(list, cols) else [cols]
    return _reg([Event(col, 'delete') for col in cols])


