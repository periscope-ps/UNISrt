from collections import namedtuple

Event = namedtuple('Event', ('col', 'ty'))
def _reg(events):
    def _wrapper(f):
        f.rt_events = getattr(f, 'rt_events', [])
        f.rt_events.extend(events)
        return f
    return _wrapper


def postflush_event(cols):
    """
    :param cols: Name of the :class:`UnisCollection <unis.models.lists.UnisCollection>` associated with the event.
    :type cols: str or list[str]

    Decorator that associates a :class:`RuntimeService <unis.services.abstract.RuntimeService>` function with a 
    collection.  The decorated function will be registered as a callback with the collection.
    
    Decorated function invoked on when resources are **flushed** to the back end datastore. Decorated function should follow:
    
        **Parameters:**
        
        * **resource:** :class:`UnisObject <unis.models.models.UnisObject>` invoking the event.
    """
    cols = cols if isinstance(cols, list) else [cols]
    return _reg([Event(col, 'postflush') for col in cols])
    
def preflush_event(cols):
    """
    :param cols: Name of the :class:`UnisCollection <unis.models.lists.UnisCollection>` associated with the event.
    :type cols: str or list[str]

    Decorator that associates a :class:`RuntimeService <unis.services.abstract.RuntimeService>` function with a 
    collection.  The decorated function will be registered as a callback with the collection.
    
    Decorated function invoked on when resources are **flushed** to the back end datastore. Decorated function should follow:
    
        **Parameters:**
        
        * **resource:** :class:`UnisObject <unis.models.models.UnisObject>` invoking the event.
    """
    cols = cols if isinstance(cols, list) else [cols]
    return _reg([Event(col, 'preflush') for col in cols])
    
def commit_event(cols):
    """
    :param cols: Name of the :class:`UnisCollection <unis.models.lists.UnisCollection>` associated with the event.
    :type cols: str or list[str]

    Decorator that associates a :class:`RuntimeService <unis.services.abstract.RuntimeService>` function with a 
    collection.  The decorated function will be registered as a callback with the collection.
    
    Decorated function invoked on when resources are **commited** to the back end datastore. Decorated function should follow:
    
        **Parameters:**
        
        * **resource:** :class:`UnisObject <unis.models.models.UnisObject>` invoking the event.
    """
    cols = cols if isinstance(cols, list) else [cols]
    return _reg([Event(col, 'commit') for col in cols])
    
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


def new_update_event(cols):
    """
    :param str cols: Name of the :class:`UnisCollection <unis.models.lists.UnisCollection>` associated with the event.
    :type cols: str or list[str]

    Shortcut event subscribes to both new and update events.
    """
    cols = cols if isinstance(cols, list) else [cols]
    events = []
    for col in cols:
        events.extend([Event(col, 'new'), Event(col, 'update')])
    return _reg(events)

def new_delete_event(cols):
    """
    :param str cols: Name of the :class:`UnisCollection <unis.models.lists.UnisCollection>` associated with the event.
    :type cols: str or list[str]

    Shortcut event subscribes to both new and delete events.
    """
    cols = cols if isinstance(cols, list) else [cols]
    events = []
    for col in cols:
        events.extend([Event(col, 'new'), Event(col, 'delete')])
    return _reg(events)

def update_delete_event(cols):
    """
    :param str cols: Name of the :class:`UnisCollection <unis.models.lists.UnisCollection>` associated with the event.
    :type cols: str or list[str]

    Shortcut event subscribes to both update and delete events.
    """
    cols = cols if isinstance(cols, list) else [cols]
    events = []
    for col in cols:
        events.extend([Event(col, 'update'), Event(col, 'delete')])
    return _reg(events)

def all_events(cols):
    """
    :param str cols: Name of the :class:`UnisCollection <unis.models.lists.UnisCollection>` associated with the event.
    :type cols: str or list[str]

    Shortcut event subscribes to both new, update and delete events.
    """
    cols = cols if isinstance(cols, list) else [cols]
    events = []
    for c in cols:
        events.extend([Event(c, 'new'), Event(c, 'update'), Event(c, 'delete')])
    return _reg(events)
