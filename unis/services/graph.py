
from unis.services.abstract import RuntimeService
from unis.services.graphbuilder import Graph
from unis.models import Node, Link, Port


# UnisGrapher is a service that connects newly discovered nodes, ports, and links
# as they are discovered
class UnisGrapher(RuntimeService):
    targets = [ Node, Link ]
    def update(self, resource):
        if not hasattr(self.runtime, "graph"):
            self.runtime.graph = Graph(db=self.runtime)
        if isinstance(resource, Node):
            if not hasattr(resource, "svg"):
                resource.svg = { "x": 0, "y": 0, "active": False }
            if resource not in self.runtime.graph.vertices:
                self.runtime.graph.vertices.append(resource)
            l4_addr = []
            for port in resource.ports:
                if hasattr(port, "address") and getattr(port.address, "type", None) == "ipv4":
                    l4_addr.append(port.address.address)
                if hasattr(port, "node") and port.node != resource:
                    raise AttributeError("Port object referenced by two or more Nodes {} and {}".format(port.node.id, resource.id))
                port.node = resource
            resource.l4_addr = l4_addr
        elif isinstance(resource, Link):
            if resource.directed:
                if isinstance(resource.endpoints.source, Port):
                    resource.endpoints.source.link = resource
                if isinstance(resource.endpoints.sink, Port):
                    resource.endpoints.sink.link = resource
                    if resource.endpoints.source and resource.endpoints.sink:
                        edge = (resource.endpoints.source.node, resource.endpoints.sink.node)
                        if not self.runtime.graph.hasEdge(*edge):
                            self.runtime.graph.edges.append(edge)
            else:
                if isinstance(resource.endpoints[0], Port):
                    resource.endpoints[0].link = resource
                if isinstance(resource.endpoints[1], Port):
                    resource.endpoints[1].link = resource
                if len(resource.endpoints) == 2 and all(map(lambda x: hasattr(x, "node"), resource.endpoints)):
                    if not self.runtime.graph.hasEdge(resource.endpoints[0].node, resource.endpoints[1]):
                        self.runtime.graph.edges.append((resource.endpoints[0].node, resource.endpoints[1].node))
                    if not self.runtime.graph.hasEdge(resource.endpoints[1].node, resource.endpoints[0]):
                        self.runtime.graph.edges.append((resource.endpoints[1].node, resource.endpoints[0].node))
        
    
    def new(self, resource):
        if not hasattr(self.runtime, "graph"):
            self.runtime.graph = Graph(db=self.runtime)
        if isinstance(resource, Node):
            if not hasattr(resource, "svg"):
                resource.svg = { "x": 0, "y": 0, "active": False }
            if resource not in self.runtime.graph.vertices:
                self.runtime.graph.vertices.append(resource)
            l4_addr = []
            for port in resource.ports:
                if hasattr(port, "address") and getattr(port.address, "type", None) == "ipv4":
                    l4_addr.append(port.address.address)
                if hasattr(port, "node") and port.node != resource:
                    raise AttributeError("Port object referenced by two or more Nodes")
                port.node = resource
            resource.l4_addr = l4_addr
        elif isinstance(resource, Link):
            if resource.directed:
                if isinstance(resource.endpoints.source, Port):
                    resource.endpoints.source.link = resource
                if isinstance(resource.endpoints.sink, Port):
                    resource.endpoints.sink.link = resource
                    if resource.endpoints.source and resource.endpoints.sink:
                        self.runtime.graph.edges.append((resource.endpoints.source.node, resource.endpoints.sink.node))
            else:
                if isinstance(resource.endpoints[0], Port):
                    resource.endpoints[0].link = resource
                if isinstance(resource.endpoints[1], Port):
                    resource.endpoints[1].link = resource
                if len(resource.endpoints) == 2 and all(map(lambda x: hasattr(x, "node"), resource.endpoints)):
                    self.runtime.graph.edges.append((resource.endpoints[0].node, resource.endpoints[1].node))
                    self.runtime.graph.edges.append((resource.endpoints[1].node, resource.endpoints[0].node))
