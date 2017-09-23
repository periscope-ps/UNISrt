
from unis.services.abstract import RuntimeService
from unis.models import Node, Link, Port


# UnisGrapher is a service that connects newly discovered nodes, ports, and links
# as they are discovered
class UnisGrapher(RuntimeService):
    targets = [ Node, Link ]
    def new(self, resource):
        if isinstance(resource, Node):
            for port in resource.ports:
                if hasattr(port, "node") and port.node != resource:
                    raise AttributeError("Port object referenced by two or more Nodes")
                port.node = resource
        elif isinstance(resource, Link):
            if resource.directed:
                if isinstance(resource.endpoints.source, Port):
                    resource.endpoints.source.link = resource
                if isinstance(resource.endpoints.sink, Port):
                    resource.endpoints.source.link = resource
            else:
                if isinstance(resource.endpoints[0], Port):
                    resource.endpoints[0].link = resource
                if isinstance(resource.endpoints[1], Port):
                    resource.endpoints[1].link = resource
