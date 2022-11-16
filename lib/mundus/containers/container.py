import enum, asyncio, logging
from typing import NoReturn

from urllib.parse import urlparse, urlunparse
from threading import Lock
from collections import defaultdict

from mundus import models, options
from mundus.containers import client as unis
from mundus.exceptions import ConnectionError, MundusMergeError, RemovedEntityError

class Action(enum.Enum):
    PUSH = enum.auto()
    DELETE = enum.auto()

_lock = Lock()
log = logging.getLogger("mundus.containers")
netloc_map = {}
listdict = lambda: defaultdict(list)
remote_map = {}
reverse_map = {}
class _AbstractRemote(object):
    def __init__(self):
        self._reset()

    def _reset(self):
        self.id_index, self.cols = {}, defaultdict(dict)
        self.links_forward, self.links_reverse = defaultdict(listdict), defaultdict(listdict)
        self._ty_children  = defaultdict(list)
        self._col_ts = defaultdict(lambda: 0)
        self._pending = defaultdict(set)

    def add(self, entity):
        if entity.container():
            entity.container().remove(entity)
        with _lock:
            if entity.id in self.cols[type(entity)]:
                self.cols[type(entity)][entity.id]._merge(entity)
            else:
                self.cols[type(entity)][entity.id] = entity
            entity = self.cols[type(entity)][entity.id]
            self.id_index[entity.id] = entity
        return self.id_index[entity.id]

    def add_rel(self, rel):
        self.links_forward[rel.colRef][rel.subject.id].append(rel.target)
        self.links_reverse[rel.colRef][rel.target.id].append(rel.subject)

    def remove(self, entity):
        with _lock:
            if (Action.PUSH, entity) in self._pending[entity.colRef]:
                self._pending[entity.colRef].remove((Action.PUSH, entity))
            self._pending[entity.colRef].add((Action.DELETE, entity))
            if entity.id in self.cols[type(entity)]:
                del self.cols[type(entity)][entity.id]
            if entity.id in self.id_index:
                del self.id_index[entity.id]
            entity._set_container(None)

    def _commit_any(self, record):
        if (Action.DELETE, record) in self._pending[record.colRef]:
            self._pending[record.colRef].remove((Action.DELETE, record))
        self._pending[record.colRef].add((Action.PUSH, record))

    def commit(self, entity):
        if entity.id not in self.id_index:
            self.add(entity)
        elif self.id_index[entity.id] != entity:
            self.id_index[entity.id]._merge(entity)
        self._commit_any(entity)

    def subtypes(self, ty):
        return set(self._ty_children[ty] + [ty])

    async def push(self):
        self._pending = defaultdict(set)
    async def pull(self, ty): pass
    async def pull_rels(self, path): pass

    def __str__(self):
        return f"@{netloc_map[self]}"

class _NullRemote(_AbstractRemote): pass
class _Remote(_AbstractRemote):
    alive = True
    def __init__(self, client, classes, links):
        self.client = client
        self.links, self.classes = links, classes
        super().__init__()

    def _reset(self):
        super()._reset()
        self.links_forward, self.links_reverse = {}, {}
        for p in self.links:
            self.links_forward[p.split('/')[0]] = defaultdict(list)
            self.links_reverse[p.split('/')[0]] = defaultdict(list)
        for cls in self.classes:
            self._register_type(cls)

    def _register_type(self, ty):
        for sub in ty.__mmro__:
            if issubclass(sub, models.Entity):
                self._ty_children[sub].append(ty)

    def _import(self, other):
        for col, entities in other.cols.items():
            for uid,e in entities.items():
                try:
                    self.cols[col][uid]._merge(e)
                except KeyError:
                    self.cols[col][uid] = e
                self._pending[e.colRef].add(self.cols[col][uid])
        for ty, actions in other._pending.items():
            self._pending[ty] | actions
        if options.get("conn.auto_push"):
            asyncio.get_event_loop().run_until_complete(self.push())

    def commit(self, entity):
        super().commit(entity)
        if options.get("conn.auto_push"):
            asyncio.get_event_loop().run_until_complete(self.push())

    def add(self, entity):
        entity = super().add(entity)
        entity._set_container(self)
        return entity

    def add_rel(self, rel):
        super().add_rel(rel)
        self._commit_any(rel)
        if options.get("conn.auto_push"):
            asyncio.get_event_loop().run_until_complete(self.push())

    def remove(self, entity):
        super().remove(entity)
        if options.get("conn.auto_push"):
            asyncio.get_event_loop().run_until_complete(self.push())

    async def push(self) -> NoReturn:
        calls, conns = [], defaultdict(list)
        with _lock:
            for ty,actions in self._pending.items():
                for a,e in actions:
                    if options.get("conn.auto_validate"):
                        e.validate()
                    if a == Action.DELETE:
                        calls.append(self.client.delete(e.selfRef))
                    else:
                        conns[e.colRef].append(dict(e))
            for href,data in conns.items():
                calls.append(self.client.post(href, data))
            try: responses = await asyncio.gather(*calls)
            except ConnectionError as e:
                log.warning(f"Failed to commit changes to remote - '{netloc_map[self]}'")
                return
            self._pending = defaultdict(set)

    async def pull(self, path):
        ts = self._col_ts[path]
        results = await self.client.get(path, **{':ts': f"gt={ts}"})
        for e in results:
            entity = models.get_class(e[":type"])(e)
            if ts < entity.ts:
                ts = entity.ts
            self.add(entity)
        self._col_ts[path] = ts

    async def pull_rels(self, path):
        def _col(v):
            return v.split('/')[0]
        def _id(v):
            return v.split('/')[1]

        ts = self._col_ts[path]
        results = await self.client.get(path, ts=f"gt={ts}")
        paths = {}
        for rel in results:
            paths[_col(rel["subject"])] = paths[_col(rel["target"])] = 1
        await asyncio.gather(*[self.pull(p) for p in paths])
        for rel in results:
            if ts < rel[":ts"]:
                ts = rel[":ts"]
            self.links_forward[path][_id(rel["subject"])].append(self.id_index[_id(rel["target"])])
            self.links_reverse[path][_id(rel["target"])].append(self.id_index[_id(rel["subject"])])
        self._col_ts[path] = ts

    def close(self):
        asyncio.get_event_loop().run_until_complete(self.push())
        asyncio.get_event_loop().run_until_complete(self.client.close())
        self._reset()

class Container(object):
    """
    The Container class operates as a proxy for a single remote data store.  Communication to the
    remote is handled by a wrapped Remote proxy class that is hotswapped in automatically when a live
    connection is established.  This ensures that only one connection is made to each remote regardless
    of how many Container instances are created for the remote.  I.e.: Two Containers created for a
    remote in which one Container references the IP of the remote while the other references a
    CNAME record will both contain the same proxy object, avoiding duplication.

    Entities and Relationships are cached locally as they are read in.  Only new records are
    queried when new requests are made to the remote.
    """
    def __init__(self, netloc, scheme, connect=True):
        self._remote, self._netloc, self._scheme = _NullRemote(), netloc, scheme
        if netloc is not None and connect:
            self._connect()

    def _get_desc(self, client):
        home = asyncio.get_event_loop().run_until_complete(client.get(""))
        clss, links = [], []
        for v in home:
            if v['rel'] == "collection":
                for x in v["targetschema"]["items"]["oneOf"]:
                    clss.append(models.get_class(x["$ref"]))
            elif v['rel'] == "link":
                links.append(v["href"].split('/')[1])
        return clss, links

    def _connect(self):
        if self._netloc is None:
            return
        if not isinstance(self._remote, _NullRemote):
            return self._remote.client.check_ws()
        if self._netloc in reverse_map:
            self._remote = reverse_map[self._netloc]
            return

        client = unis.Client(urlunparse((self._scheme, self._netloc, '', '', '', '')))
        try:
            ident = asyncio.get_event_loop().run_until_complete(client.get("about"))["ident"]
            if ident not in remote_map:
                desc = self._get_desc(client)
                with _lock:
                    remote_map[ident] = _Remote(client, *desc)
            remote_map[ident]._import(self._remote)
            reverse_map[self._netloc] = self._remote = remote_map[ident]
            netloc_map[self._remote] = self._netloc
        except ConnectionError:
            return

    def add(self, entity):
        """
        :param entity: Entity to add to this container
        :type entity: `Entity <mundus.models.models.Entity>`
        
        Add an Entity to the Container for tracking.  Adding Entities in this will automatically 
        commit the new Entity to the backend based on mundus' current `conn.auto_push` option.
        If `auto_push` is enabled, the Entity is immediately pushed to the remote, otherwise
        the push is deferred until the next manual call to 
        `push <mundus.containers.container.Container.push>`
        """
        self._connect()
        result = self._remote.add(entity)
        result._set_container(self._remote)
        entity._set_container(self._remote)
        self._remote.commit(result)
        return result

    def push(self):
        """
        Manually push all local changes to the remote data store.  This function does nothing
        if the `conn.auto_push` option is enabled.
        """
        self._connect()
        return asyncio.get_event_loop().run_until_complete(self._remote.push())
    def remove(self, entity):
        """
        :param entity: Entity to be removed from the container
        :type entity: `Entity <mundus.models.models.Entity>`

        Remove an `Entity <mundus.models.models.Entity>` from the container.
        """
        self._connect()
        return self._remote.remove(entity)

    def find_entities(self, hrefs):
        """
        :param hrefs: A list of urls to entities to pull from the remote.
        :type hrefs: list[str]
        :return: A list of `Entities <mundus.models.models.Entity>`
        :rtype: list[`Entity <mundus.models.models.Entity>`]
        
        This function collects the entities referenced in the `hrefs`.  This results in
        a single call to the remote, and will use any cached results if available.
        """
        self._connect()
        calls, paths = [], [urlparse(v).path.split('/') for v in hrefs]
        for p in set([p[0] for p in paths]):
            calls.append(self._remote.pull(p))
        asyncio.get_event_loop().run_until_complete(asyncio.gather(*calls))
        result = []
        for p in paths:
            result.append(self._remote.id_index[p[1]])
            result[-1]._set_container(self._remote)
        return result

    def find_relationship(self, href):
        """
        :param href: A url to a collection of relationships
        :type hrefs: str
        :return: A list of `Entities <mundus.models.models.Entity>`
        :rtype: list[`Entity <mundus.models.models.Entity>`]

        This function collects the entities referenced in the relationship described by the `href`.
        This results in a single call to the remote per entity collection, and will use any cached 
        results if available.
        """
        self._connect()
        href = urlparse(href).path.split('/')
        asyncio.get_event_loop().run_until_complete(self._remote.pull_rels(href[2]))
        if href[3] == "target":
            rels = self._remote.links_forward
        else:
            rels = self._remote.links_reverse
        return [v for v in rels[href[2]][href[1]]]

    def _add_relationship(self, rel):
        self._connect()
        self._remote.add_rel(rel)

    def close(self):
        """
        Closes the connection for this remote.  The main effect of this closure is clearning the 
        internal cache for the remote.  Unlock conventional sockets, a closed remote connection
        is automatically restarted in the event an operation is executed on the contaienr.
        """
        self._remote.close()

_nullcontainer = Container(None, None)
def get_container(url):
    """
    :param url: Address of the remote entity store.
    :type schema_url: str
    :return: Returns a container that manages the connection and data at a url.
    :rtype: `Container <mundus.containers.container.Container>`

    Generates or returns a container that maintains the connection with the remote entities and
    relationships.  If `url` is **None**, the *NullContainer* is returned.  This is a special container
    that has no backend and is used as temporary storage for new entities that are not associated
    with a remote.
    """
    if url is None:
        return _nullcontainer
    url = urlparse(url)
    return Container(url.netloc, url.scheme)
