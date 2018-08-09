from collections import namedtuple, defaultdict
from functools import wraps

class ServiceMetaclass(type):
    """
    The :class:`ServiceMetaclass <unis.services.abstract.ServiceMetaclass>` handles registration of
    decorated functions for event driven access to runtime resources.
    
    Decorators also ensure that the :class:`Context <unis.models.models.Context>` is properly
    configured and resources are set to not update from changes within the service; a precaution
    to prevent event cascades.
    """
    def __init__(cls, name, bases, kwargs):
        def decoratorFactory(fn):
            @wraps(fn)
            def nf(self, resource):
                tmpState = resource._rt_live
                resource.setRuntime(self.runtime)
                resource._rt_live = False
                fn(self, resource)
                resource._rt_live = tmpState
            return nf

        cls.rt_listeners = defaultdict(lambda: defaultdict(list))
        for n,op in kwargs.items():
            if hasattr(op, 'rt_events'):
                op = decoratorFactory(op)
                setattr(cls, n, op)
                for event in op.rt_events:
                    cls.rt_listeners[event.col][event.ty].append(op)

class RuntimeService(metaclass=ServiceMetaclass):
    @property
    def targets(self):
        """
        Automatically generated list of collections that the service targets.
        
        :rtype: list[str]
        """
        return list(self.rt_listeners.keys())

    def setRuntime(self, runtime):
        """
        :param runtime: The owner of the service.
        :type runtime: :class:`Runtime <unis.runtime.runtime.Runtime>`
        
        :meth:`RuntimeService.setRuntime <unis.services.RuntimeService.setRuntime>` is called by the 
        :class:`Runtime <unis.runtime.runtime.Runtime>` when a class or instance is passed to 
        :meth:`ObjectLayer.addService <unis.runtime.oal.ObjectLayer.addService>`
        to register to service with the runtime's constituent collections.
        """
        self.runtime = runtime

    def initialize(self):
        """
        Can be overridden by inheriting classes to perform one time operations after service construction
        is complete but before events are handled.
        """
        pass
    def attach(self, col):
        """
        :param col: The collection to attach this service to.
        :type col: :class:`UnisCollection <unis.models.lists.UnisCollection>`
        
        Attaches the :class:`RuntimeService <unis.services.abstract.RuntimeService>` to a 
        :class:`UnisCollection <unis.models.lists.UnisCollection>`.  ``attach`` is called by the
        runtime when a new collection is generated.
        """
        if col.name in self.rt_listeners:
            col.addCallback(lambda res, ty: [op(self, res) for op in self.rt_listeners[col.name][ty]])
    
