

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
    def __init__(self, runtime=None):
        self.runtime = runtime
        super(RuntimeService, self).__init__()
    def attach(self, runtime):
        self.runtime = runtime
    
    def new(self, resource):
        pass
    def update(self, resource):
        pass
    def delete(self, resource):
        pass
