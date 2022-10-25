from unis import watchdog
from unis.version import VERSION
from unis.containers import container
from logging import getLogger

_log = getLogger("mundus")
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
    _log.info(f"Establishing connecting to {url}")
    conn = container.get_container(url, allowvirtual)
    if ws:
        _log.debug(f"- Creating websocket socket for {url}")
        conn.connect()
    return conn

def push():
    _log.debug("Pushing records to remote")
    [c.push() for c in container.instances.values()]

def delete_record(v):
    _log.debug(f"Deleting {v.selfRef} from system")
    container.from_instance(v).delete(v)

def types():
    return list(set(sum([c.records.keys() for c in container.instances.values()])))

#def Q():
#    q = None
#    for c in container.instances.values():
#        if not q: q = c.Q()
#        else: q |= c.Q()
#    return q

#def register_listener(fn):
#    events.manager.register(fn)

watchdog.run()
