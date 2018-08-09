from unis.services.abstract import RuntimeService
from unis.services.event import new_update_event
from unis.services.graphbuilder import Graph

class UnisGrapher(RuntimeService):
    """
    Automatically generates a :class:`Graph <unis.services.graphbuilder.Graph>`
    and attachs it to the runtime as a new **graph** attribute.

    `Node` and `Link` objects added to the runtime are automatically added to 
    the graph as appropriate.  When a new complete edge is detected between
    two `Nodes`, an edge is added to the graph.
    """
    
    def initialize(self):
        self.runtime.graph = Graph(db=self.runtime)

    def _get_full_edge(self, start):
        if hasattr(start, '_graph_tag') or not (hasattr(start, 'link') and hasattr(start, 'node')):
            raise ValueError("Incomplete start port")

        link, eps = start.link, start.link.endpoints
        end = eps.sink if link.directed else eps[0] if eps[1] == start else [1]
        
        if (link.directed and eps.sink == start) or \
           (not (hasattr(end, 'link') and hasattr(end, 'node'))):
            raise ValueError("Incomplete end port")
        return (start.node, end.node)

    def _try_add_edge(self, port):
        try:
            edge = self._get_full_edge(port)
        except ValueError:
            return False
        if not self.runtime.graph.hasEdge(*edge):
            self.runtime.graph.edges.append(edge)
        return True
    
    @new_update_event('links')
    def new_links(self, link):
        """
        :param link: The link resource added or modified in the runtime.
        
        Adds a backreference to the Port objects registered to the link
        and - if a full edge is available - adds an edge to the graph.
        """
        ends = link.endpoints
        a,b = (ends.source, ends.sink) if link.directed else (ends[0], ends[1])
        a.link = b.link = link

        if self._try_add_edge(a):
            a._graph_tag = True
        if not link.directed:
            if self._try_add_edge(b):
                b._graph_tag = True
    
    @new_update_event('nodes')
    def new_nodes(self, node):
        """
        :param node: The node resource added or modified in the runtime.
        
        Adds a backreference to the Port objects registered to the node,
        registers the node as a vertex and - if a full edge is available - adds
        an edge to the graph.
        """
        if node not in self.runtime.graph.vertices:
            self.runtime.graph.vertices.append(node)

        for p in node.ports:
            p.node = node
            if self._try_add_edge(p):
                p._graph_tag = True
