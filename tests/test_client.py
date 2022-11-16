from mundus.containers import client
from mundus.exceptions import ConnectionError
from tests import remote_mock
from urllib.parse import urlparse

import aiohttp, importlib, asyncio, bson

from pytest import fixture

class MockSession(object):
    class manager(object):
        def __init__(self, url):
            self.url = urlparse(url).path

        def dumps(self, dat):
            if isinstance(dat, list):
                return bson.dumps({str(i): v for i,v in enumerate(dat)})
            else: return bson.dumps(dat)
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args, **kwargs): pass
        async def read(self):
            print(self.url)
            if self.url.split("/")[1:][0] == "": return self.dumps(remote_mock.home)
            elif self.url.split("/")[1:][0] == "about": return self.dumps(remote_mock.about)
            elif self.url.split("/")[1:][0] == "nodes": return self.dumps(remote_mock.nodes)
            elif self.url.split("/")[1:][0] == "about": return self.dumps(remote_mock.about)
            elif self.url.split("/")[1:][0] == "links": return self.dumps(remote_mock.links)
            elif self.url.split("/")[1:][0] == "owns": return self.dumps(remote_mock.owns)

    init_calls = []
    get_calls = []
    post_calls = []
    put_calls = []
    delete_calls = []
    close_calls = []
    @classmethod
    def reset(cls):
        cls.init_calls = []
        cls.get_calls = []
        cls.post_calls = []
        cls.put_calls = []
        cls.delete_calls = []
        cls.close_calls = []
    
    def __init__(self, *args, **kwargs):
        self.init_calls.append((args, kwargs))

    def get(self, *args, **kwargs):
        self.get_calls.append((args, kwargs))
        return self.manager(args[0])

    def post(self, *args, **kwargs):
        self.post_calls.append((args, kwargs))
        return self.manager(args[0])

    def put(self, *args, **kwargs):
        self.put_calls.append((args, kwargs))
        return self.manager(args[0])

    def delete(self, *args, **kwargs):
        self.delete_calls.append((args, kwargs))
        return self.manager(args[0])

    async def close(self, *args, **kwargs):
        self.close_calls.append((args, kwargs))

@fixture
def mock_session(monkeypatch):
    def mock_session(*args, **kwargs):
        session = MockSession(*args, **kwargs)
        return session
    monkeypatch.setattr(aiohttp, "ClientSession", mock_session)
    MockSession.reset()
    return MockSession

@fixture
def reload_module():
    importlib.reload(client)

def test_build_client(reload_module):
    conn = client.Client("http://test1")

    assert conn is not None
    assert conn.session is not None
    
def test_check_ws(reload_module):
    conn = client.Client("http://test1")

    conn.check_ws()
    # TODO: Implement WS

def test_get(reload_module, mock_session):
    conn = client.Client("http://test1")

    result = asyncio.get_event_loop().run_until_complete(conn.get("/nodes", ts="gt=5"))

    assert len(mock_session.get_calls) == 1
    assert result == remote_mock.nodes
    assert mock_session.get_calls[0][0][0] == "http://test1/nodes?ts=gt%3D5"

def test_get_dict(reload_module, mock_session):
    conn = client.Client("http://test1")

    result = asyncio.get_event_loop().run_until_complete(conn.get("about"))

    assert len(mock_session.get_calls) == 1
    assert result == remote_mock.about
    assert mock_session.get_calls[0][0][0] == "http://test1/about"

def test_post(reload_module, mock_session):
    conn = client.Client("http://test1")

    result = asyncio.get_event_loop().run_until_complete(conn.post("/links", {"a": 10}))

    assert len(mock_session.post_calls) == 1
    assert result == remote_mock.links
    assert mock_session.post_calls[0][0][0] == "http://test1/links"
    assert mock_session.post_calls[0][1]["data"] == bson.dumps({"a": 10})

def test_post_list(reload_module, mock_session):
    conn = client.Client("http://test1")

    result = asyncio.get_event_loop().run_until_complete(conn.post("/links", [{"a": 10}]))

    assert len(mock_session.post_calls) == 1
    assert mock_session.post_calls[0][0][0] == "http://test1/links"
    assert mock_session.post_calls[0][1]["data"] == bson.dumps({"0": {"a": 10}})

def test_put(reload_module, mock_session):
    conn = client.Client("http://test1")

    result = asyncio.get_event_loop().run_until_complete(conn.put("/owns", {"a": 10}))

    assert len(mock_session.put_calls) == 1
    assert result == remote_mock.owns
    assert mock_session.put_calls[0][0][0] == "http://test1/owns"
    assert mock_session.put_calls[0][1]["data"] == bson.dumps({"a": 10})

def test_delete(reload_module, mock_session):
    conn = client.Client("http://test1")

    result = asyncio.get_event_loop().run_until_complete(conn.delete("/owns/1"))

    assert len(mock_session.delete_calls) == 1
    assert result == remote_mock.owns
    assert mock_session.delete_calls[0][0][0] == "http://test1/owns/1"

def test_close(reload_module, mock_session):
    conn = client.Client("http://test1")

    asyncio.get_event_loop().run_until_complete(conn.close())

    assert len(mock_session.close_calls) == 0

def test_close_session(reload_module, mock_session):
    conn = client.Client("http://test1")

    assert client.Client.session

    asyncio.get_event_loop().run_until_complete(client.Client.close_session())

    assert len(mock_session.close_calls) == 1
    assert client.Client.session is None

def test_session_raises(reload_module, mock_session):
    conn = client.Client("http://test1")

    asyncio.get_event_loop().run_until_complete(client.Client.close_session())

    try:
        conn.get("/nodes")
        assert False
    except ConnectionError:
        assert True
