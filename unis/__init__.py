from unis import events
from unis.settings import OPTIONS
from unis import config
from unis.containers import container, watchdog

def connect(url, ws=False, allowvirtual=False):
    """
    Connect to a remote data store and return a handle for the requested store.  This function
    can also return a virtual container if ``allowvirtual`` is set to True.  Virtual containers
    behave as normal containers with the exception of pending all requests until a connection
    to the remote data store can be made.  This feature provides delay tolerant functionality
    to containers but should be enabled with care, as it can potentially hide network errors.

    :param href: URL of the data store represented by the container
    :type href: str
    :param ws: Establish a persistent connection to the data store, defaults to False
    :type ws: bool, optional
    :param allowvirtual: Generates a pending container instead of raising ``ConnectionError``, defaults to False
    :type allowvirtual: bool, optional
    :raises mundus.exceptions.ConnectionError: Exception raised if ``href`` finds no route to host
    :rtype RemoteContainer:  :mundus.containers.RemoteContainer:`RemoteContainer`
    """
    conn = container.get_container(url, allowvirtual):
    if ws: conn.connect()
    return conn

def push():
    [c.push() for c in container.instances.values()]

def delete_record(v):
    v.get_container().delete(v)

def types():
    return list(set(sum([c.records.keys() for c in container.instances.values()])))

def Q():
    q = None
    for c in container.instances.values():
        if not q: q = c.Q()
        else: q |= c.Q()
    return q

def set_option(n, v):
    f"""
    Modify the behavior of mundus.  Options are as follows.
    
    {config.print_config()}
    """
    setattr(config.Configuration(), n, v)

_sentinal = object()
def get_option(n, default=_sentinal):
    if default == _sentinal: return getattr(config.Configuration(), n)
    return getattr(config.Configuration(), n, default)

def register_listener(fn):
    events.manager.register(fn)

watchdog.run()
