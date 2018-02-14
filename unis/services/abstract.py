
class ServiceMetaclass(type):
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
    targets = []
    
    def __init__(self, targets=[]):
        if targets:
            for target in targets:
                self.targets.append(target)
        super(RuntimeService, self).__init__()
    def attach(self, runtime):
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
        pass
