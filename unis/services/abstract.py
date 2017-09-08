

class ServiceMetaclass(type):
    def __init__(cls, name, bases, kwargs):
        def decoratorFactory(fn):
            def nf(self, resource):
                tmpState = resource.isDeferred()
                resource.setDeferred(True)
                kwargs[fn](self, resource)
                resource.setDeferred(tmpState)
            return nf
        
        for op in ['new', 'update', 'delete']:
            if op in kwargs:
                setattr(cls, op, decoratorFactory(op))

class RuntimeService(metaclass=ServiceMetaclass):
    targets = []
    
    def __init__(self, targets=[]):
        if targets:
            for target in targets:
                self.targets.append(target)
        super(RuntimeService, self).__init__()
    def attach(self, runtime):
        self._runtime = runtime
        if self.targets:
            for target in self.targets:
                if target in self._runtime:
                    collection = self._runtime.getModel(getattr(target, "names", []))
                    collection.addService(self)
        else:
            for collection in self._runtime.collections:
                collection.addService(self)
    
    def new(self, resource):
        pass
    def update(self, resource):
        pass
    def delete(self, resource):
        pass
