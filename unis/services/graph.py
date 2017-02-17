
from unis.services.abstract import RuntimeService
from unis.models import Node, Link


# UnisGrapher is a service that connects newly discovered nodes, ports, and links
# as they are discovered
class UnisGrapher(RuntimeService):
    def new(self, resource):
        if isinstance(resource, Node):
            for port in resource.ports:
                port = self.runtime.find(port)
                if port.node and port.node != resource:
                    raise AttributeError("Port object referenced by two or more Nodes")
                
                port.node = resource
        elif isinstance(resource, Link):
            if resource.directed:
                port1 = self.runtime.find(resource.endpoints.source)
                port2 = self.runtime.find(resource.endpoints.sync)
            else:
                port1 = self.runtime.find(resource.endpoints[0])
                port2 = self.runtime.find(resource.endpoints[1])
                
            if not port1 or port2:
                raise AttributeError("Link object does not reference both endpoints")
            
            port1.link = resource
            port2.link = resource
