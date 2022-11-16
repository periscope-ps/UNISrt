import mundus
from mundus.models import get_class

from pytest import fixture

class MockContainer(object):
    init_calls = []
    push_calls = []
    delete_calls = []
    remote_map = {}
    @classmethod
    def reset(cls):
        cls.init_calls = []
        cls.push_calls = []
        cls.delete_calls = []
        cls.remote_map = {}

    def __init__(self, url, *args, **kwargs):
        self.init_calls.append((url, args, kwargs))
        self.remote_map[url] = self
        self.cols = {str: [], int: []}

    async def push(self, *args, **kwargs):
        self.push_calls.append((args, kwargs))

    def delete(self, *args, **kwargs):
        self.delete_calls.append((args, kwargs))

    def add(self, v, *args, **kwargs):
        v._set_container(self)

@fixture
def mock_remotemap(monkeypatch):
    monkeypatch.setattr(mundus.containers.container, "remote_map", MockContainer.remote_map)
    return MockContainer.remote_map
    
@fixture
def mock_container(monkeypatch):
    def mock_container(url):
        container = MockContainer(url)
        return container
    monkeypatch.setattr(mundus.containers, "get_container", mock_container)
    MockContainer.reset()
    return MockContainer

def test_connect(mock_container):
    conn = mundus.connect("http://test1")

    assert conn
    assert isinstance(conn, MockContainer)

def test_push(mock_container, mock_remotemap):
    mundus.connect("http://test1")
    mundus.connect("http://test2")

    mundus.push()

    assert len(mock_container.push_calls) == 2

def test_delete_entity(mock_container, mock_remotemap):
    cls = get_class("http://unis.open.sice.indiana.edu/schema/2.0.0/entities/topology/computenode")
    n = cls()

    mundus.delete_entity(n)

    assert len(mock_container.delete_calls) == 1

def test_types(mock_container, mock_remotemap):
    mundus.connect("http://test1")
    mundus.connect("http://test2")

    assert len(mundus.types()) == 2
    assert int in mundus.types()
    assert str in mundus.types()
