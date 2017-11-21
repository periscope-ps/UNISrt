import json
import unittest

from unittest.mock import MagicMock, patch

from unis.rest import UnisProxy
from unis.rest.unis_client import UnisClient

class ProxyTest(unittest.TestCase):
    def _make_n(self, n=1):
        clients = []
        mocks = {}
        for i in range(n):
            url = "http://localhost:{}".format(8888 + i)
            clients.append({ 'url': url, 'verify': False, 'cert': None})
            mocks[url] = MagicMock()
            
        proxy = UnisProxy(clients)
        proxy.clients = mocks
        return proxy, mocks
        
    def test_single_unis_init(self):
        # Arrange
        proxy = UnisProxy([{'url': 'http://localhost:8888', 'verify': False, 'cert': None}])
        
        # Assert
        self.assertTrue(len(proxy.clients) == 1)
        self.assertTrue(isinstance(proxy.clients['http://localhost:8888'], UnisClient))
    
    def test_multi_unis_init(self):
        # Arrange
        proxy = UnisProxy([{'url': 'http://localhost:8888', 'cert': None, 'verify': False},
                           {'url': 'http://localhost:8890', 'cert': None, 'verify': False}])
        
        # Assert
        self.assertTrue(len(proxy.clients) == 2)
        self.assertTrue(isinstance(proxy.clients['http://localhost:8888'], UnisClient))
        self.assertTrue(isinstance(proxy.clients['http://localhost:8890'], UnisClient))
        
    @patch('unis.rest.unis_client.UnisClient')
    def test_multi_unis_init_ssl(self, client):
        # Arrange
        proxy = UnisProxy([{'url': 'http://localhost:8888', 'cert': None, 'verify': False},
                           {'url': 'https://localhost:8889', 'cert': 'good_cert', 'verify': True}])
        
        # Assert
        self.assertEqual(client.call_count, 2)
        client.assert_any_call({'url': 'http://localhost:8888', 'cert': None, 'verify': False}, False)
        client.assert_any_call({'url': 'https://localhost:8889', 'cert': 'good_cert', 'verify': True}, False)
        
    def test_bad_url_init(self):
        # Assert
        with self.assertRaises(ValueError):
            UnisProxy([{'url': 'http:/localhost;8888', 'ssl': False, 'verify': None}])
    
    def test_single_unis_shutdown(self):
        # Arrange
        proxy, clients = self._make_n(1)
        
        # Act
        proxy.shutdown()
        
        # Assert
        clients['http://localhost:8888'].shutdown.assert_called_once_with()
        
    def test_multi_unis_shutdown(self):
        # Arrange
        proxy, clients = self._make_n(2)
        
        # Act
        proxy.shutdown()
        
        # Assert
        clients['http://localhost:8888'].shutdown.assert_called_once_with()
        clients['http://localhost:8889'].shutdown.assert_called_once_with()
    
    def test_single_unis_getResources(self):
        # Arrange
        proxy, clients = self._make_n(1)
        clients['http://localhost:8888'].getResources.return_value = [10]
        
        # Act
        v = proxy.getResources()
        
        # Assert
        clients['http://localhost:8888'].getResources.assert_called_once_with()
        self.assertEqual(v, [10])
        
    def test_multi_unis_getResources(self):
        # Arrange
        proxy, clients = self._make_n(2)
        clients['http://localhost:8888'].getResources.return_value = [10]
        clients['http://localhost:8889'].getResources.return_value = [15]
        
        # Act
        v = proxy.getResources()
        
        # Assert
        clients['http://localhost:8888'].getResources.assert_called_once_with()
        clients['http://localhost:8889'].getResources.assert_called_once_with()
        self.assertEqual(len(v), 2)
        self.assertIn(10, v)
        self.assertIn(15, v)
        
    def test_multi_unis_getResource_with_source(self):
        # Arrange
        proxy, clients = self._make_n(2)
        clients['http://localhost:8888'].getResources.return_value = "s1"
        clients['http://localhost:8889'].getResources.return_value = "s2"
        
        # Act
        v = proxy.getResources('http://localhost:8889')
        
        # Assert
        self.assertEqual(clients['http://localhost:8888'].getResources.call_count, 0)
        clients['http://localhost:8889'].getResources.assert_called_once_with()
        self.assertEqual(v, "s2")
        
    def test_single_unis_get_generic(self):
        # Arrange
        proxy, clients = self._make_n(1)
        clients['http://localhost:8888'].get.return_value = "success"
        
        # Act
        v = proxy.get('nodes')
        
        # Assert
        clients['http://localhost:8888'].get.assert_called_once_with("nodes", None, {})
        self.assertEqual(v, ["success"])
        
    def test_multi_unis_get_generic(self):
        # Arrange
        proxy, clients = self._make_n(2)
        clients['http://localhost:8888'].get.return_value = "success1"
        clients['http://localhost:8889'].get.return_value = "success2"
        
        # Act
        v = proxy.get('nodes')
        
        # Assert
        clients['http://localhost:8888'].get.assert_called_once_with("nodes", None, {})
        clients['http://localhost:8889'].get.assert_called_once_with("nodes", None, {})
        self.assertEqual(len(v), 2)
        self.assertIn("success1", v)
        self.assertIn("success2", v)
        
    def test_multi_unis_get_list_generic(self):
        # Arrange
        proxy, clients = self._make_n(2)
        clients['http://localhost:8888'].get.return_value = ["success1"]
        clients['http://localhost:8889'].get.return_value = ["success2"]
        
        # Act
        v = proxy.get('nodes')
        
        # Assert
        clients['http://localhost:8888'].get.assert_called_once_with("nodes", None, {})
        clients['http://localhost:8889'].get.assert_called_once_with("nodes", None, {})
        self.assertEqual(len(v), 2)
        self.assertIn("success1", v)
        self.assertIn("success2", v)
        
    def test_single_unis_get_specific(self):
        # Arrange
        proxy, clients = self._make_n(1)
        clients['http://localhost:8888'].get.return_value = "success"
        
        # Act
        v = proxy.get('http://localhost:8888/nodes')
        
        # Assert
        clients['http://localhost:8888'].get.assert_called_once_with("http://localhost:8888/nodes", None, {})
        self.assertEqual(v, ["success"])

    def test_multi_unis_get_specific(self):
        # Arrange
        proxy, clients = self._make_n(2)
        clients['http://localhost:8888'].get.return_value = "success1"
        clients['http://localhost:8889'].get.return_value = "success2"
        
        # Act
        v = proxy.get("http://localhost:8889/nodes")
        
        # Assert
        self.assertEqual(clients['http://localhost:8888'].get.call_count, 0)
        clients['http://localhost:8889'].get.assert_called_once_with("http://localhost:8889/nodes", None, {})
        self.assertEqual(v, ["success2"])
    
    def test_multi_unis_get_source(self):
        # Arrange
        proxy, clients = self._make_n(2)
        clients['http://localhost:8888'].get.return_value = "success1"
        clients['http://localhost:8889'].get.return_value = "success2"
        
        # Act
        v = proxy.get("nodes", "http://localhost:8889")
        
        # Assert
        self.assertEqual(clients['http://localhost:8888'].get.call_count, 0)
        clients['http://localhost:8889'].get.assert_called_once_with("nodes", None, {})
        self.assertEqual(v, ["success2"])
    
    def test_single_unis_post_one(self):
        # Arrange
        proxy, clients = self._make_n(1)
        clients['http://localhost:8888'].post.return_value = "success"
        n = MagicMock()
        n.getSource.return_value = "http://localhost:8888"
        n.getCollection.return_value = "nodes"
        n.to_JSON.return_value = {}
        
        # Act
        v = proxy.post(n)
        
        # Assert
        clients['http://localhost:8888'].post.assert_called_once_with("nodes", "[{}]")
        self.assertEqual(v, ["success"])

    def test_multi_unis_post_one(self):
        # Arrange
        proxy, clients = self._make_n(2)
        clients['http://localhost:8888'].post.return_value = "success1"
        clients['http://localhost:8889'].post.return_value = "success2"
        n = MagicMock()
        n.getSource.return_value = "http://localhost:8889"
        n.getCollection.return_value = "nodes"
        n.to_JSON.return_value = {}
        
        # Act
        v = proxy.post(n)
        
        # Assert
        self.assertEqual(clients['http://localhost:8888'].post.call_count, 0)
        clients['http://localhost:8889'].post.assert_called_once_with("nodes", "[{}]")
        self.assertEqual(v, ["success2"])

    def test_single_unis_post_list(self):
        # Arrange
        proxy, clients = self._make_n(1)
        clients['http://localhost:8888'].post.return_value = "success"
        n1 = MagicMock()
        n1.getSource.return_value = "http://localhost:8888"
        n1.getCollection.return_value = "nodes"
        n1.to_JSON.return_value = {}
        n2 = MagicMock()
        n2.getSource.return_value = "http://localhost:8888"
        n2.getCollection.return_value = "nodes"
        n2.to_JSON.return_value = {}
        
        # Act
        v = proxy.post([n1, n2])
        
        # Assert
        clients['http://localhost:8888'].post.assert_called_once_with("nodes", "[{}, {}]")
        self.assertEqual(v, ["success"])
    
    def test_multi_unis_post_list(self):
        # Arrange
        proxy, clients = self._make_n(2)
        clients['http://localhost:8888'].post.return_value = "success1"
        clients['http://localhost:8889'].post.return_value = "success2"
        n1 = MagicMock()
        n1.getSource.return_value = "http://localhost:8888"
        n1.getCollection.return_value = "nodes"
        n1.to_JSON.return_value = {}
        n2 = MagicMock()
        n2.getSource.return_value = "http://localhost:8889"
        n2.getCollection.return_value = "nodes"
        n2.to_JSON.return_value = {}
        
        # Act
        v = proxy.post([n1, n2])
        
        # Assert
        clients['http://localhost:8888'].post.assert_called_once_with("nodes", "[{}]")
        clients['http://localhost:8889'].post.assert_called_once_with("nodes", "[{}]")
        self.assertEqual(len(v), 2)
        self.assertIn("success1", v)
        self.assertIn("success2", v)

    def test_single_unis_post_multilist(self):
        # Arrange
        proxy, clients = self._make_n(1)
        clients['http://localhost:8888'].post.return_value = "success1"
        n1 = MagicMock()
        n1.getSource.return_value = "http://localhost:8888"
        n1.getCollection.return_value = "nodes"
        n1.to_JSON.return_value = {"v": 10 }
        n2 = MagicMock()
        n2.getSource.return_value = "http://localhost:8888"
        n2.getCollection.return_value = "links"
        n2.to_JSON.return_value = { "v": 20 }
        n3 = MagicMock()
        n3.getSource.return_value = "http://localhost:8888"
        n3.getCollection.return_value = "links"
        n3.to_JSON.return_value = { "v": 20 }
        
        # Act
        v = proxy.post([n1, n2, n3])
        
        # Assert
        self.assertTrue(clients['http://localhost:8888'].post.call_count == 2)
        clients['http://localhost:8888'].post.assert_any_call("nodes", '[{"v": 10}]')
        clients['http://localhost:8888'].post.assert_any_call("links", '[{"v": 20}, {"v": 20}]')
        

    def test_multi_unis_post_multilist(self):
        # Arrange
        proxy, clients = self._make_n(2)
        clients['http://localhost:8888'].post.return_value = "success1"
        clients['http://localhost:8889'].post.return_value = "success2"
        n1 = MagicMock()
        n1.getSource.return_value = "http://localhost:8888"
        n1.getCollection.return_value = "nodes"
        n1.to_JSON.return_value = {"v": 10 }
        n2 = MagicMock()
        n2.getSource.return_value = "http://localhost:8889"
        n2.getCollection.return_value = "links"
        n2.to_JSON.return_value = { "v": 20 }
        n3 = MagicMock()
        n3.getSource.return_value = "http://localhost:8889"
        n3.getCollection.return_value = "ports"
        n3.to_JSON.return_value = { "v": 30 }
        
        # Act
        v = proxy.post([n1, n2, n3])
        
        # Assert
        self.assertTrue(clients['http://localhost:8888'].post.call_count == 1)
        self.assertTrue(clients['http://localhost:8889'].post.call_count == 2)
        clients['http://localhost:8888'].post.assert_called_once_with("nodes", '[{"v": 10}]')
        clients['http://localhost:8889'].post.assert_any_call("links", '[{"v": 20}]')
        clients['http://localhost:8889'].post.assert_any_call("ports", '[{"v": 30}]')
        
    def test_single_unis_put(self):
        # Arrange
        proxy, clients = self._make_n(1)
        clients['http://localhost:8888'].put.return_value = "success"
        
        # Act
        v = proxy.put('http://localhost:8888', { 'v': 10 })
        
        # Assert
        self.assertEqual(v, "success")
        clients['http://localhost:8888'].put.assert_called_once_with('http://localhost:8888', { 'v': 10 })
        
    def test_single_unis_put_bad_source(self):
        # Arrange
        proxy, clients = self._make_n(1)
        
        # Act
        with self.assertRaises(ValueError):
            v = proxy.put('bad_ref', {'v': 10})
            
    def test_multi_unis_put(self):
        # Arrange
        proxy, clients = self._make_n(2)
        clients['http://localhost:8888'].put.return_value = "success1"
        clients['http://localhost:8889'].put.return_value = "success2"
        
        # Act
        v = proxy.put('http://localhost:8889', {'v': 10})
        
        # Assert
        self.assertEqual(v, "success2")
        self.assertEqual(clients['http://localhost:8888'].put.call_count, 0)
        clients['http://localhost:8889'].put.assert_called_once_with('http://localhost:8889', {'v': 10})
    
    def test_single_unis_delete(self):
        # Arrange
        proxy, clients = self._make_n(1)
        clients['http://localhost:8888'].delete.return_value = "success"
        n = MagicMock()
        n.getSource.return_value = 'http://localhost:8888'
        n.to_JSON.return_value = {'v': 10}
        
        # Act
        v = proxy.delete(n)
        
        # Assert
        n.getSource.assert_called_once_with()
        self.assertEqual(v, "success")
        clients['http://localhost:8888'].delete.assert_called_once_with({'v': 10})
        
    def test_multi_unis_delete(self):
        # Arrange
        proxy, clients = self._make_n(2)
        clients['http://localhost:8888'].delete.return_value = "success1"
        clients['http://localhost:8889'].delete.return_value = "success2"
        n = MagicMock()
        n.getSource.return_value = 'http://localhost:8888'
        n.to_JSON.return_value = {'v': 10}
        
        # Act
        v = proxy.delete(n)
        
        # Assert
        n.getSource.assert_called_once_with()
        self.assertEqual(v, "success1")
        clients['http://localhost:8888'].delete.assert_called_once_with({'v': 10})
        self.assertEqual(clients['http://localhost:8889'].delete.call_count, 0)
        
    def test_single_unis_subscribe(self):
        # Arrange
        proxy, clients = self._make_n(1)
        clients['http://localhost:8888'].subscribe.return_value = "success"
        
        # Act
        v = proxy.subscribe('nodes', 'cb')
        
        # Assert
        clients['http://localhost:8888'].subscribe.assert_called_once_with('nodes', 'cb')
        self.assertEqual(v, ['success'])
        
    def test_multi_unis_subscribe(self):
        # Arrange
        proxy, clients = self._make_n(2)
        clients['http://localhost:8888'].subscribe.return_value = "success1"
        clients['http://localhost:8889'].subscribe.return_value = "success2"
        
        # Act
        v = proxy.subscribe('nodes', 'cb')
        
        # Assert
        clients['http://localhost:8888'].subscribe.assert_called_once_with('nodes', 'cb')
        clients['http://localhost:8889'].subscribe.assert_called_once_with('nodes', 'cb')
        self.assertEqual(len(v), 2)
        self.assertIn("success1", v)
        self.assertIn("success2", v)
        
class ClientTest(unittest.TestCase):
    class _response(object):
        def __init__(self, code=200):
            self.status_code = code
            
        def json(self):
            return { "v": 10 }
    
    def _headers(self):
        return {'Content-Type': 'application/perfsonar+json', 'Accept': 'application/perfsonar+json'}
    
    @patch('unis.rest.unis_client.requests')
    def test_getResources(self, requests):
        # Arrange
        client = UnisClient({'url': 'http://localhost:8888'})
        requests.get.return_value = ClientTest._response()
        
        # Act
        v = client.getResources()
        
        # Assert
        requests.get.assert_called_once_with('http://localhost:8888', verify=False, cert=None, headers=self._headers())
        self.assertEqual(v, { 'v': 10 })
    
    @patch('unis.rest.unis_client.requests')
    def test_bad_getResources(self, requests):
        # Arrange
        client = UnisClient({'url': 'http://localhost:8888'})
        requests.get.return_value = ClientTest._response(404)
        
        # Act
        with self.assertRaises(Exception):
            client.getResources()

    @patch('unis.rest.unis_client.requests')
    def test_get(self, requests):
        # Arrange
        client = UnisClient({'url': 'http://localhost:8888'})
        requests.get.return_value = ClientTest._response()
        
        # Act
        v = client.get('nodes')
        
        # Assert
        requests.get.assert_called_once_with('http://localhost:8888/nodes?', verify=False, cert=None, headers=self._headers())
        self.assertEqual(v, { 'v': 10 })
    
    @patch('unis.rest.unis_client.requests')
    def test_get_specific(self, requests):
        # Arrange
        client = UnisClient({'url': 'http://localhost:8888'})
        requests.get.return_value = ClientTest._response()
        
        # Act
        v = client.get('nodes/test')
        
        # Assert
        requests.get.assert_called_once_with('http://localhost:8888/nodes/test?', verify=False, cert=None, headers=self._headers())
        self.assertEqual(v, { 'v': 10 })
        
    @patch('unis.rest.unis_client.requests')
    def test_bad_get(self, requests):
        # Arrange
        client = UnisClient({'url': 'http://localhost:8888'})
        requests.get.return_value = ClientTest._response(404)
        
        # Act
        with self.assertRaises(Exception):
            client.get('nodes')

    @patch('unis.rest.unis_client.requests')
    def test_post(self, requests):
        # Arrange
        client = UnisClient({'url': 'http://localhost:8888'})
        requests.post.return_value = ClientTest._response()
        
        # Act
        v = client.post('nodes', { 'v': 10 })
        
        # Assert
        requests.post.assert_called_once_with('http://localhost:8888/nodes', data='{"v": 10}', verify=False, cert=None)
        self.assertEqual(v, {'v': 10})
        
    @patch('unis.rest.unis_client.requests')
    def test_post_fullurl(self, requests):
        # Arrange
        client = UnisClient({'url': 'http://localhost:8888'})
        requests.post.return_value = ClientTest._response()
        
        # Act
        v = client.post('http://localhost:8888/nodes', { 'v': 10 })
        
        # Assert
        requests.post.assert_called_once_with('http://localhost:8888/nodes', data='{"v": 10}', verify=False, cert=None)
        self.assertEqual(v, {'v': 10})
    
    @patch('unis.rest.unis_client.requests')
    def test_bad_post(self, requests):
        # Arrange
        client = UnisClient({'url': 'http://localhost:8888'})
        requests.post.return_value = ClientTest._response(404)
        
        # Act
        with self.assertRaises(Exception):
            client.post('nodes', {'v': 10})
    
    @patch('unis.rest.unis_client.requests')
    def test_put(self, requests):
        # Arrange
        client = UnisClient({'url': 'http://localhost:8888'})
        requests.put.return_value = ClientTest._response()
        
        # Act
        v = client.put('nodes/test', { 'v': 10 })
        
        # Assert
        requests.put.assert_called_once_with('http://localhost:8888/nodes/test', data='{"v": 10}', verify=False, cert=None)
        self.assertEqual(v, {'v': 10})
        
    @patch('unis.rest.unis_client.requests')
    def test_put_fullurl(self, requests):
        # Arrange
        client = UnisClient({'url': 'http://localhost:8888'})
        requests.put.return_value = ClientTest._response()
        
        # Act
        v = client.put('http://localhost:8888/nodes/test', { 'v': 10 })
        
        # Assert
        requests.put.assert_called_once_with('http://localhost:8888/nodes/test', data='{"v": 10}', verify=False, cert=None)
        self.assertEqual(v, {'v': 10})

    @patch('unis.rest.unis_client.requests')
    def test_bad_put(self, requests):
        # Arrange
        client = UnisClient({'url': 'http://localhost:8888'})
        requests.put.return_value = ClientTest._response(404)
        
        # Act
        with self.assertRaises(Exception):
            client.put('nodes', {'v': 10})
    
    @patch('unis.rest.unis_client.requests')
    def test_delete(self, requests):
        # Arrange
        client = UnisClient({'url': 'http://localhost:8888'})
        requests.delete.return_value = ClientTest._response()
        
        # Act
        v = client.delete('nodes/test')
        
        # Assert
        requests.delete.assert_called_once_with('http://localhost:8888/nodes/test', verify=False, cert=None)
        self.assertEqual(v, {'v': 10})
        
    @patch('unis.rest.unis_client.requests')
    def test_delete_fullurl(self, requests):
        # Arrange
        client = UnisClient({'url': 'http://localhost:8888'})
        requests.delete.return_value = ClientTest._response()
        
        # Act
        v = client.delete('http://localhost:8888/nodes/test')
        
        # Assert
        requests.delete.assert_called_once_with('http://localhost:8888/nodes/test', verify=False, cert=None)
        self.assertEqual(v, {'v': 10})
        
    @patch('unis.rest.unis_client.requests')
    def test_bad_delete(self, requests):
        # Arrange
        client = UnisClient({'url': 'http://localhost:8888'})
        requests.delete.return_value = ClientTest._response(404)
        
        # Act
        with self.assertRaises(Exception):
            client.delete('nodes', {'v': 10})
    
