import unittest

from unis.services import RuntimeService
from unis.services.event import new_event,update_event,delete_event

class _testservice1(RuntimeService):
    def initialize(self):
        self._initialized = True

class _testservice2(RuntimeService):
    @new_event("nodes")
    def single_new(self, res):
        pass

    @update_event("nodes")
    def single_update(self, res):
        pass

    @delete_event("nodes")
    def single_delete(self, res):
        pass

class _testservice3(RuntimeService):
    @new_event(["nodes", "links"])
    def multi_new(self, res):
        pass

    @update_event(["nodes", "links"])
    def multi_update(self, res):
        pass

    @delete_event(["nodes", "links"])
    def multi_delete(self, res):
        pass

class _testservice4(RuntimeService):
    @new_event("nodes")
    def first_new(self, res):
        pass

    @new_event("nodes")
    def second_new(self, res):
        pass

class RuntimeServiceTest(unittest.TestCase):
    def test_contruct_service(self):
        service = RuntimeService()

    def test_single_service(self):
        service = _testservice2()
        
        self.assertIn('nodes', service.rt_listeners)
        for n in ['new', 'update', 'delete']:
            with self.subTest(name=n):
                self.assertIn(n, service.rt_listeners['nodes'])
                self.assertIn(getattr(_testservice2, 'single_' + n), service.rt_listeners['nodes'][n])
        
    def test_multi_service(self):
        service = _testservice3()
        
        self.assertIn('nodes', service.rt_listeners)
        self.assertIn('links', service.rt_listeners)
        self.assertIn('new', service.rt_listeners['nodes'])
        self.assertIn('update', service.rt_listeners['nodes'])
        self.assertIn('delete', service.rt_listeners['nodes'])
        self.assertIn(_testservice3.multi_new, service.rt_listeners['nodes']['new'])
        self.assertIn(_testservice3.multi_update, service.rt_listeners['nodes']['update'])
        self.assertIn(_testservice3.multi_delete, service.rt_listeners['nodes']['delete'])

        self.assertIn('new', service.rt_listeners['links'])
        self.assertIn('update', service.rt_listeners['links'])
        self.assertIn('delete', service.rt_listeners['links'])
        self.assertIn(_testservice3.multi_new, service.rt_listeners['links']['new'])
        self.assertIn(_testservice3.multi_update, service.rt_listeners['links']['update'])
        self.assertIn(_testservice3.multi_delete, service.rt_listeners['links']['delete'])

    def test_concurrent_service(self):
        service = _testservice4()
        
        self.assertIn('nodes', service.rt_listeners)
        self.assertIn('new', service.rt_listeners['nodes'])
        self.assertNotIn('update', service.rt_listeners['nodes'])
        self.assertNotIn('delete', service.rt_listeners['nodes'])
        self.assertIn(_testservice4.first_new, service.rt_listeners['nodes']['new'])
        self.assertIn(_testservice4.second_new, service.rt_listeners['nodes']['new'])
