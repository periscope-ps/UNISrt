import asyncio

from logging import getLogger
from urllib.parse import urlparse

from mundus import containers, watchdog

_log = getLogger("mundus")
watchdog.run()

def connect(url, ws=False):
    """
    :param href: URL of the data store represented by the container
    :type href: str
    :param ws: Establish a persistent connection to the data store, defaults to False
    :type ws: bool, optional
    :raises mundus.exceptions.ConnectionError: Exception raised if ``href`` finds no route to host
    :rtype RemoteContainer:  `Container <mundus.containers.Container>`

    Connect to a remote data store and return a handle to the requested store.
    """
    _log.info(f"Establishing connecting to {url}")
    conn = containers.get_container(url)
    return conn

def push():
    """
    Push all pending record updates to corresponding remote stores.  This will invoke
    the minimum number of requests necessary to updated the corresponding collections
    on each remote.  In the worst case, this will require NxM connections, where N
    is the number of remotes and M is the number of collections per remote.  In
    practice, this is extremely uncommon, with the average push case being O(M).

    The specific behavior of `push` depends on the `conn.auto_push` option.  If set
    to True, all entities will be pushed by the runtime when their values are changed
    and the `push` function will be a NOOP.
    """
    _log.debug("Pushing records to remote")
    calls = [c.push() for c in containers.container.remote_map.values()]
    asyncio.get_event_loop().run_until_complete(asyncio.gather(*calls))

def delete_entity(v):
    """
    Remove an entity from the remote and evict it from the mundus cache.
    """
    _log.debug(f"Deleting {v.selfRef} from system")
    v.container().delete(v)

def types():
    """
    :return: A list of `types` associated with remotes.
    :rtype: List[`Entity <mundus.models.models.Entity>`]
    
    List all types current recognized by remote stores.  Note that this does not
    include subclasses and only returns explicitly listed by the remote as validated
    types.  This function can be used as an alternative to an explicit call to
    `get_class <mundus.models.get_class>` or an archive yaml file.
    """
    tys = set()
    for remote in containers.container.remote_map.values():
        tys |= set(remote.cols.keys())
    return list(tys)

#def register_listener(fn):
#    events.manager.register(fn)
