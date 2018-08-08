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
    def mutli_delete(self, res):
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
        
        self.assertIn(service.rt_listeners, 'nodes')
        for n in ['new', 'update', 'delete']:
            with self.subTest(name=n):
                self.assertIn(service.rt_listeners['nodes'], n)
                self.assertIn(service.rt_listeners['nodes'][n], getattr(service, 'single_' + n))
        
    def test_multi_service(self):
        service = _testservice3()
        
        self.assertIn(service.rt_listeners, 'nodes')
        self.assertIn(service.rt_listeners, 'links')
        self.assertIn(service.rt_listeners['nodes'], 'new')
        self.assertIn(service.rt_listeners['nodes'], 'update')
        self.assertIn(service.rt_listeners['nodes'], 'delete')
        self.assertIn(service.rt_listeners['nodes']['new'], service.multi_new)
        self.assertIn(service.rt_listeners['nodes']['update'], service.multi_update)
        self.assertIn(service.rt_listeners['nodes']['delete'], service.multi_delete)

        self.assertIn(service.rt_listeners['links'], 'new')
        self.assertIn(service.rt_listeners['links'], 'update')
        self.assertIn(service.rt_listeners['links'], 'delete')
        self.assertIn(service.rt_listeners['links']['new'], service.multi_new)
        self.assertIn(service.rt_listeners['links']['update'], service.multi_update)
        self.assertIn(service.rt_listeners['links']['delete'], service.multi_delete)

    def test_concurrent_service(self):
        service = _testservice4()
        
        self.assertIn(service.rt_listeners, 'nodes')
        self.assertIn(service.rt_listeners['nodes'], 'new')
        self.assertNotIn(service.rt_listeners['nodes'], 'update')
        self.assertNotIn(service.rt_listeners['nodes'], 'delete')
        self.assertIn(service.rt_listeners['nodes']['new'], service.first_new)
        self.assertIn(service.rt_listeners['nodes']['new'], service.second_new)
