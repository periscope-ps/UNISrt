from mundus import containers, options, models
from tests import remote_mock
from mundus.settings import ID_FIELD, TS_FIELD
from mundus.exceptions import RemovedEntityError, MundusMergeError, ConnectionError

import importlib, uuid, warnings, copy
from pytest import fixture
from jsonschema.exceptions import ValidationError

Node = models.get_class("http://unis.open.sice.indiana.edu/schema/2.0.0/entities/topology/node")
Link = models.get_class("http://unis.open.sice.indiana.edu/schema/2.0.0/entities/topology/link")
class MockClient(object):
    get_calls = []
    post_calls = []
    put_calls = []
    delete_calls = []
    check_ws_calls = []
    close_calls = []
    disabled = True
    @classmethod
    def reset(cls):
        cls.get_calls = []
        cls.post_calls = []
        cls.put_calls = []
        cls.delete_calls = []
        cls.check_ws_calls = []
        cls.close_calls = []
        cls.disabled = True

    def __init__(self, uid):
        self.uid = uid

    def check_ws(self, *args, **kwargs):
        self.check_ws_calls.append((args, kwargs))

    async def close(self, *args, **kwargs):
        self.close_calls.append((args, kwargs))

    async def get(self, *args, **kwargs):
        if self.disabled: raise ConnectionError()
        ts = int(kwargs.get("ts", "gt=0").split("=")[1])
        self.get_calls.append((args, kwargs))
        if args[0] == "":
            return copy.deepcopy(remote_mock.home)
        elif args[0] == "about":
            return {**copy.deepcopy(remote_mock.about), **{"ident": self.uid}}
        elif args[0] == "nodes":
            result = copy.deepcopy(remote_mock.nodes)
            return [v for v in result if v[":ts"] > ts]
        elif args[0] == "links":
            return copy.deepcopy(remote_mock.links)
        elif args[0] == "owns":
            result = copy.deepcopy(remote_mock.owns)
            return [v for v in result if v[":ts"] > ts]
        
    async def post(self, *args, **kwargs):
        if self.disabled: raise ConnectionError()
        self.post_calls.append((args, kwargs))

    async def put(self, *args, **kwargs):
        if self.disabled: raise ConnectionError()
        self.put_calls.append((args, kwargs))

    async def delete(self, *args, **kwargs):
        if self.disabled: raise ConnectionError()
        self.delete_calls.append((args, kwargs))

@fixture
def mock_client(monkeypatch):
    def mock_client(x):
        client = MockClient(x)
        MockClient.disabled = False
        return client
    monkeypatch.setattr(containers.client, "Client", mock_client)
    MockClient.reset()
    return MockClient

@fixture
def bad_client(monkeypatch):
    def mock_client(x):
        return MockClient(x) 
    monkeypatch.setattr(containers.client, "Client", mock_client)
    MockClient.reset()
    return MockClient   

@fixture
def reload_module():
    options.set("conn.auto_push", False)
    options.set("conn.auto_validate", False)
    importlib.reload(containers.container)


def test_factory_null(reload_module):
    c1 = containers.get_container(None)
    c2 = containers.get_container(None)

    assert c1 is c2

def test_factory_other(mock_client, reload_module):
    null = containers.get_container(None)
    c1 = containers.get_container("http://test1")
    c2 = containers.get_container("http://test1")

    assert c1._remote is c2._remote
    assert c1 is not null and c2 is not null

def test_factory_different(mock_client, reload_module):
    c1 = containers.get_container("http://test1")
    c2 = containers.get_container("http://test2")
    c3 = containers.get_container("http://test1")

    assert c1._remote is not c2._remote
    assert c1._remote is c3._remote

def test_subtypes(mock_client, reload_module): 
    ComputeNode = models.get_class("http://unis.open.sice.indiana.edu/schema/2.0.0/entities/topology/computenode")
    PhysNode = models.get_class("http://unis.open.sice.indiana.edu/schema/2.0.0/entities/topology/physicalnode")
    Switch = models.get_class("http://unis.open.sice.indiana.edu/schema/2.0.0/entities/topology/switchnode")
    c1 = containers.get_container("http://test1")

    tys = c1._remote.subtypes(Node)

    assert len(tys) == 4
    assert all([n in tys for n in [Node, ComputeNode, PhysNode, Switch]])

def test_container_str(mock_client, reload_module):
    c1 = containers.get_container("http://test1:8888")

    assert str(c1._remote) == "@test1:8888"

def test_merge_containers(mock_client, reload_module):
    c1 = containers.get_container("http://test1")
    c2 = containers.get_container("http://test2")

    try:
        c1._remote._import(c1._remote)
        assert False
        return
    except MundusMergeError:
        assert True

def test_add_null(reload_module, mock_client):
    c = containers.get_container(None)
    n = Node()
    c.add(n)

    assert Node in c._remote.cols
    assert n.id in c._remote.id_index

def test_add_other(reload_module, mock_client):
    c1 = containers.get_container("http://test1")
    n = Node()
    c1.add(n)

    assert Node in c1._remote.cols
    assert n.id in c1._remote.id_index
    assert len(c1._remote._pending) == 1

    c2 = containers.get_container("http://test1")
    assert Node in c2._remote.cols
    assert n.id in c1._remote.id_index
    assert n.id in c2._remote.id_index
    assert n == c1._remote.id_index[n.id]
    assert n == c2._remote.id_index[n.id]

def test_auto_move(reload_module, mock_client):
    cn = containers.get_container(None)
    c1 = containers.get_container("http://test1")
    n = Node()

    assert Node in cn._remote.cols
    assert n.id in cn._remote.id_index
    assert cn._remote.id_index[n.id] == n

    c1.add(n)

    assert n.id not in cn._remote.id_index

def test_add_different(reload_module, mock_client):
    c1 = containers.get_container("http://test1")
    n = Node()
    c1.add(n)

    assert Node in c1._remote.cols
    assert n.id in c1._remote.id_index
    assert len(c1._remote._pending) == 1

    c2 = containers.get_container("http://test2")
    assert Node not in c2._remote.cols
    assert n.id in c1._remote.id_index
    assert n.id not in c2._remote.id_index
    assert n == c1._remote.id_index[n.id]

def test_push_null(reload_module, mock_client):
    c1 = containers.get_container(None)
    n = Node()
    c1.add(n)
    c1.push()

    assert len(mock_client.post_calls) == 0

def test_push_other(reload_module, mock_client):
    c1 = containers.get_container("http://test2")
    n = Node()
    c1.add(n)

    assert len(mock_client.post_calls) == 0
    assert len(c1._remote._pending[n.colRef]) == 1
    
    c1.push()

    assert len(mock_client.post_calls) == 1
    assert len(c1._remote._pending[n.colRef]) == 0

def test_push_multi(reload_module, mock_client):
    c1 = containers.get_container("http://test2")
    n, l = Node(), Link()
    c1.add(n)
    c1.add(l)
    
    assert len(mock_client.post_calls) == 0
    assert len(c1._remote._pending[n.colRef]) == 1
    assert len(c1._remote._pending[l.colRef]) == 1
    c1.push()
    assert len(mock_client.post_calls) == 2
    assert len(c1._remote._pending[n.colRef]) == 0
    assert len(c1._remote._pending[l.colRef]) == 0

def test_push_auto(reload_module, mock_client):
    options.set("conn.auto_push", True)
    c1 = containers.get_container("http://test2")
    n = Node()
    c1.add(n)

    assert len(mock_client.post_calls) == 1
    assert len(c1._remote._pending[n.colRef]) == 0

def test_remove_null(reload_module, mock_client):
    c1 = containers.get_container(None)
    n = Node()
    c1.remove(n)

    assert len(mock_client.delete_calls) == 0
    assert n.id not in c1._remote.id_index
    
def test_remove_other(reload_module, mock_client):
    c1 = containers.get_container("http://test1")
    n = Node()
    c1.add(n)
    c1.push()

    c1.remove(n)
    assert len(c1._remote._pending[n.colRef]) == 1
    c1.push()
    assert len(mock_client.delete_calls) == 1
    assert len(c1._remote._pending[n.colRef]) == 0

def test_remove_auto(reload_module, mock_client):
    options.set("conn.auto_push", True)
    c1 = containers.get_container("http://test1")
    n = Node()
    c1.add(n)
    c1.push()

    c1.remove(n)
    assert len(mock_client.delete_calls) == 1

def test_remove_push(reload_module, mock_client):
    c1 = containers.get_container("http://test1")
    n = c1.add(Node())
    c1.remove(n)

    assert len(c1._remote._pending[n.colRef]) == 1
    assert list(c1._remote._pending[n.colRef])[0][0] == containers.container.Action.DELETE

def test_push_remove(reload_module, mock_client):
    c1 = containers.get_container("http://test1")
    n = c1.add(Node())
    c1.push()
    c1.remove(n)
    c1.add(n)

    assert len(c1._remote._pending[n.colRef]) == 1
    assert list(c1._remote._pending[n.colRef])[0][0] == containers.container.Action.PUSH

def test_remove_error(reload_module, mock_client):
    c1 = containers.get_container("http://test1")
    n = c1.add(Node())
    c1.remove(n)

    try:
        n.name = "test"
        assert False
    except RemovedEntityError:
        assert True

def test_find_entities(reload_module, mock_client):
    Node = models.get_class("http://unis.open.sice.indiana.edu/schema/2.0.0/entities/topology/node")
    c1 = containers.get_container("http://test1")
    n = c1.find_entities(["nodes/1"])

    assert len(n) == 1
    assert isinstance(n[0], Node)
    assert len(mock_client.get_calls) == 3
    assert c1._remote._col_ts["nodes"] == 5

def test_validate_good(reload_module, mock_client):
    options.set("conn.auto_validate", True)
    c1 = containers.get_container("http://test1")
    n = c1.add(Node())
    n.name = "test"
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        c1.push()

def test_validate_bad(reload_module, mock_client):
    options.set("conn.auto_validate", True)
    c1 = containers.get_container("http://test1")
    n = c1.add(Node())
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        try:
            c1.push()
            assert False
        except ValidationError:
            assert True

def test_find_relationship(reload_module, mock_client):
    c1 = containers.get_container("http://test1")
    rel = c1.find_relationship("nodes/1/owns/subject")
    rel2 = c1.find_relationship("nodes/2/owns/subject")

    assert len(rel) == 1
    assert isinstance(rel[0], Node)
    assert len(rel2) == 1
    assert isinstance(rel2[0], Node)
    assert rel[0] != rel2[0]

def test_find_target_relationship(reload_module, mock_client):
    c1 = containers.get_container("http://test1")
    rel1 = c1.find_relationship("nodes/1/owns/target")
    rel2 = c1.find_relationship("nodes/2/owns/subject")

    assert len(rel1) == 1
    assert isinstance(rel1[0], Node)
    assert len(rel2) == 1
    assert isinstance(rel2[0], Node)
    assert rel1[0] != rel2[0]

def test_add_relationship(reload_module, mock_client):
    c1 = containers.get_container(None)
    n1, n2 = Node(), Node()
    rel = models.relationship.Relationship("owns", n1, n2)
    c1._add_relationship(rel)

    assert n2 in c1._remote.links_forward["owns"][n1.id]
    assert n1 in c1._remote.links_reverse["owns"][n2.id]
    assert n1 not in c1._remote.links_forward["owns"][n1.id]
    assert n2 not in c1._remote.links_reverse["owns"][n2.id]

    assert len(c1._remote._pending) == 0

def test_add_relationship(reload_module, mock_client):
    c1 = containers.get_container("http://test1")
    n1, n2 = Node(), Node()
    rel = models.relationship.Relationship("owns", n1, n2)
    c1._add_relationship(rel)

    assert n2 in c1._remote.links_forward["owns"][n1.id]
    assert n1 in c1._remote.links_reverse["owns"][n2.id]
    assert n1 not in c1._remote.links_forward["owns"][n1.id]
    assert n2 not in c1._remote.links_reverse["owns"][n2.id]

    assert len(c1._remote._pending) == 1

def test_add_relationhip_auto(reload_module, mock_client):
    options.set("conn.auto_push", True)
    c1 = containers.get_container("http://test1")
    n1, n2 = Node(), Node()
    rel = models.relationship.Relationship("owns", n1, n2)
    c1._add_relationship(rel)

    assert len(mock_client.post_calls) == 1

def test_merge_add(reload_module, mock_client):
    c1 = containers.get_container("http://test1")
    n1 = c1.add(Node({ID_FIELD: "5"}))
    n1_2 = c1.add(Node({ID_FIELD: "5", TS_FIELD: 2, "name": "hello"}))

    assert n1.name == "hello"
    assert len(c1._remote.id_index) == 1

def test_bad_client(reload_module, bad_client):
    c1 = containers.get_container("http://test1")

    assert isinstance(c1._remote, containers.container._NullRemote)

def test_disconnect_push(reload_module, mock_client):
    c1 = containers.get_container("http://test1")
    n1 = c1.add(Node())
    mock_client.disabled = True
    c1.push()

    assert len(c1._remote._pending) == 1

def test_merge_containers(reload_module, bad_client):
    c1 = containers.get_container("http://test1")
    c2 = containers.get_container("http://test1")

    assert c1._remote is not c2._remote
    
    n1 = c1.add(Node())
    n2 = c2.add(Node())

    bad_client.disabled = False
    n3 = c2.add(Node())
    n4 = c1.add(Node())

    assert c1._remote is c2._remote

def test_close_containers(reload_module, mock_client):
    c1 = containers.get_container("http://test1")
    n = c1.find_entities(["nodes/1"])

    assert len(mock_client.close_calls) == 0
    assert len(c1._remote.id_index) != 0

    c1.close()

    assert len(mock_client.close_calls) == 1
    assert len(c1._remote.id_index) == 0

def test_reload_containers(reload_module, mock_client):
    c1 = containers.get_container("http://test1")
    n = c1.find_entities(["nodes/1"])[0]

    c1.close()

    n.name = "changed"
    assert len(c1._remote.id_index) != 0

def test_model_merge(reload_module, mock_client):
    c1 = containers.get_container("http://test1")
    n1, n2 = Node({ID_FIELD: "5", "name": "bar", TS_FIELD: 0}), Node({ID_FIELD: "5", TS_FIELD: 1})

    c1.add(n1)
    c1.add(n2)

    n2.name = "foo"

    assert c1._remote.id_index[n1.id] == n1
    assert n1.name == "foo"
