
class ServiceMetaclass(type):
    """
    The :class:`ServiceMetaclass <unis.services.abstract.ServiceMetaclass>` handles registration of
    the overloaded :meth:`RuntimeService.new <unis.services.abstract.RuntimeService.new>`,
    :meth:`RuntimeService.update <unis.service.abstract.RuntimeService.update>`, and 
    :meth:`RuntimeService.delete <unis.service.abstract.RuntimeService.delete>` functions.
    
    The decorators also ensure that the :class:`Context <unis.models.models.Context>` is properly
    configured and resources are set to not update from changes within the service; a precaution
    to prevent event cascades.
    """
    def __init__(cls, name, bases, kwargs):
        def decoratorFactory(fn):
            def nf(self, resource):
                tmpState = resource._rt_live
                resource.setRuntime(self.runtime)
                resource._rt_live = False
                kwargs[fn](self, resource)
                resource._rt_live = tmpState
            return nf
        
        for op in ['new', 'update', 'delete']:
            if op in kwargs:
                setattr(cls, op, decoratorFactory(op))

class RuntimeService(metaclass=ServiceMetaclass):
    """
    :param list[str] targets: (optional) Set the collections to target for the service.

    Base abstract class for defining new services in a runtime application.
    """
    
    targets = []
    """
    ``targets`` is a list of strings containing collection names which should invoke the service.
    """
    
    def __init__(self, targets=None):
        if targets:
            for target in targets:
                self.targets.append(target)
        super(RuntimeService, self).__init__()

    def attach(self, runtime):
        """
        :param runtime: The owner of the service.
        :type runtime: :class:`Runtime <unis.runtime.runtime.Runtime>`
        
        ``attach`` is called by the :class:`Runtime <unis.runtime.runtime.Runtime>` when a class or
        instance is passed to :meth:`ObjectLayer.addService <unis.runtime.oal.ObjectLayer.addService>`
        to register to service with the runtime's constituent collections.
        """
        self.runtime = runtime
        if self.targets:
            for target in self.targets:
                if target in self.runtime:
                    collection = self.runtime.getModel(getattr(target, "names", []))
                    getattr(self.runtime, collection).addService(self)
        else:
            for collection in self.runtime.collections:
                collection.addService(self)
    
    def new(self, resource):
        pass
    
    def update(self, resource):
        pass
    
    def delete(self, resource):
        """
        """
        
        pass
    
