import itertools
import math
import random
import re

from collections import defaultdict
from string import ascii_uppercase
from copy import copy

from unis.models import Node, Port, Link

from pprint import pprint

class Graph(object):
    """
    :param vertices: A list of :class:`Nodes <unis.models.models.UnisObject>` to add to the graph.
    :param edges: A list of pairs of :class:`Nodes <unis.models.models.UnisObject>` representing adjacency.
    :param db: The :class:`Runtime <unis.runtime.runtime.Runtime>` to associate with the graph.
    :param str subnet: A subnet to assign to the nodes.
    :param str prefix: A prefix to add to each vertex's name.
    :type vertices: list[:class:`Node <unis.models.models.UnisObject>`]
    :type edges: list[tuple(:class:`Node <unis.models.models.UnisObject>`, :class:`Node <unis.models.models.UnisObject>`)]
    :type db: :class:`Runtime <unis.runtime.runtime.Runtime>`
    
    :class:`Graph <unis.services.graphbuilder.Graph>` serves as a catch-all graph
    handling solution in the mode of networkx.  It interfaces directly with a 
    ;class:`Runtime <unis.runtime.runtime.Runtime>` and provides more streamlined
    tools for graph generation, display, and manipulation.

    The `subnet` and `prefix` parameters are used for vertex generation, when new vertices are added,
    they are given an interface layer 4 IP address and name if not present.
    """
    def __init__(self, vertices=[], edges=[], db=None, subnet='10.0.0.0/8', prefix=''):
        address, mask = subnet.split("/")
        subnet_size = 4 - int(mask) // 8
        self.nextip = [0 for _ in range(subnet_size)]
        self.subnet = ".".join(address.split(".")[:(4 - subnet_size)])
        self.subnet += ".{}".format(".".join(["{}" for x in range(subnet_size)]))
        self.prefix = prefix
        self.vertices = list(vertices)
        self.edges = list(edges)
        self._edgeref = set()
        self.height = 0
        self.width  = 0
        if db:
            self._rt = db
        else:
            from unis import Runtime
            self._rt = Runtime(proxy={'defer_update':True, 'subscribe':False})
        self.processing_level = 0
    
    def _nextaddr(self):
        result = list(self.nextip)
        newip = []
        add = 1
        for v in reversed(self.nextip):
            if v < 255:
                newip.insert(0, v + add)
                add = 0
            else:
                newip.insert(0, 0)
        self.nextip = newip
        return result
    
    def createVertex(self):
        """
        Generate a :class:`Node <unis.models.models.UnisObject>` and add it as 
        a graph vertex.
        """
        n = Node({ "name": "{}{}".format(self.prefix, len(self.vertices)) })
        n.svg = {}
        self._rt.insert(n)
        self.vertices.append(n)
        return n
        
    def hasEdge(self, src, dst):
        """
        :param src: Vertex on the ingress side of the edge.
        :param dst: Vertex on the egress side of the edge.
        :type src: :class:`Node <unis.models.models.UnisObject>`
        :type dst: :class:`Node <unis.models.models.UnisObject>`
        :rtype: boolean
        
        Checks for the existance of an edge between two :class:`Nodes <unis.models.models.UnisObject>`.
        """
        return set([str(hash(src)) + str(hash(dst))]) & self._edgeref
    def createEdge(self, src, dst):
        """
        :param src: Vertex on the ingress side of the edge.
        :param dst: Vertex on the egress side of the edge.
        :type src: :class:`Node <unis.models.models.UnisObject>`
        :type dst: :class:`Node <unis.models.models.UnisObject>`
        
        Adds an edge between two :class:`Nodes <unis.models.models.UnisObject>`.
        """
        p_src = Port({"index": str(len(src.ports)) })
        p_dst = Port({"index": str(len(dst.ports)) })
        p_src.address.address = self.subnet.format(*self._nextaddr())
        p_src.address.type = "ipv4"
        p_dst.address.address = self.subnet.format(*self._nextaddr())
        p_dst.address.type = "ipv4"
        self._rt.insert(p_src)
        self._rt.insert(p_dst)
        self._edgeref.add(str(hash(src)) + str(hash(dst)))
        self._edgeref.add(str(hash(dst)) + str(hash(src)))
        src.ports.append(p_src)
        dst.ports.append(p_dst)
        self.edges.append((src, dst))
        self.edges.append((dst, src))
        l = Link({ "directed": False, "endpoints": [p_src, p_dst] })
        self._rt.insert(l)
    
    def finalize(self, include_svg=False):
        """
        :param bool include_svg: Push visual metadata to the data store.
        
        Inserts the current graph into a backend store.  This should be used
        for inter-session reuse of the same graph.
        """
        for n in self.vertices:
            if include_svg:
                n.extendSchema('svg')
            for p in n.ports:
                p.commit()
                p.link.commit()
            n.commit()
        self._rt.flush()
    
    def spring(self, unitsize=10, repeat=50):
        """
        :param int unitsize: Spring force factor (recommended between 5-20).
        :param int repeat: Number of interations to run simulation.
        
        Runs a spring simulation over the graph and places planer 
        positional metadata onto each vertex.
        """
        # initialize
        for n in self.vertices:
            if self.processing_level == 0 or not hasattr(n, 'svg'):
                n.svg = { "x": 0, "y": 0 }
        sidelen = int(math.sqrt(len(self.vertices)))
        for i, n in enumerate(self.vertices):
            x, y = (20 * (i % sidelen), 20 * (i // sidelen))
            n.svg.x, n.svg.y = (x, y)
        
        # spring
        for x in range(repeat):
            bar = "#" * math.ceil(30 * float(x + 1) / repeat)
            bar += "-" * 30
            print("\rRunning spring simulation [{}]".format(bar[:30]), end='\r')
            forces = {}
            
            # Repulsive
            for n in self.vertices:
                forces[n.name] = {}
                for adj in self.vertices:
                    if n == adj:
                        continue
                    d = math.sqrt(pow(adj.svg.x - n.svg.x, 2) + pow(adj.svg.y - n.svg.y, 2)) / unitsize
                    mag = -1 * (1 / pow(d, 2)) * unitsize
                    angle = math.atan2((adj.svg.y - n.svg.y), (adj.svg.x - n.svg.x))
                    forces[n.name][adj.name] = (mag * math.cos(angle), mag * math.sin(angle), 'repulsive', mag, angle)
            
            # Attractive
            for a,b in self.edges:
                if a != b:
                    d = math.sqrt(pow(b.svg.x - a.svg.x, 2) + pow(b.svg.y - a.svg.y, 2)) / unitsize
                    mag = 2 * math.log(d / 1) * unitsize
                    angle = math.atan2((b.svg.y - a.svg.y), (b.svg.x - a.svg.x))
                    forces[a.name][b.name] = (mag * math.cos(angle), mag * math.sin(angle), 'attractive', mag, angle, a.name, b.name)
                    angle += 180
                    forces[b.name][a.name] = (mag * math.cos(angle), mag * math.sin(angle), 'attractive', mag, angle, a.name, b.name)
            
            # Apply Forces
            #pprint(forces)
            for n in self.vertices:
                x = sum([x[0] for x in forces[n.name].values()]) * 0.1
                n.svg.x += x
                y = sum([x[1] for x in forces[n.name].values()]) * 0.1
                n.svg.y += y
        print()
        
        # Recenter
        x, y = (0, 0)
        self.height = 0
        self.width = 0
        for n in self.vertices:
            x = min(n.svg.x, x)
            y = min(n.svg.y, y)
            self.height = max(n.svg.y + 20, self.height)
            self.width  = max(n.svg.x + 20, self.width)
        
        for n in self.vertices:
            n.svg.x += abs(min(x - 20, 0)) + 400
            n.svg.y += abs(min(y - 20, 0)) + 400
        self.processing_level += repeat
    
    def dump(self, filename):
        """
        :param str filename: Location to save data.
        
        Writes positional information for the graph for future reuse.
        
        .. warning:: The graph **must** be the same graph to reuse positional metadata.
        """
        result = { "nodes": {}, "_processing_level": self.processing_level }
        if self.processing_level:
            import json
            for n in self.vertices:
                result["nodes"][n.name] = [n.svg.x, n.svg.y]
            with open(filename, 'w') as f:
                f.write(json.dumps(result))
    
    def load(self, filename):
        """
        :param str filename: Filename for the layout.
        
        Reads positional information for the graph from a file.
        
        .. warning:: The graph **must** be the same graph to reuse positional metadata.
        """
        import json
        with open(filename) as f:
            layout = json.load(f)
        for node in self.vertices:
            try:
                node.svg = { "active": False, "x": layout["nodes"][node.name][0], "y": layout["nodes"][node.name][1] }
            except KeyError:
                return False
        self.processing_level = layout["_processing_level"]
        return True
    
    def svg(self, rules=[], output=None):
        """
        :param list rules: List of annotations to add to the graph.
        :param str output: Filename to save the result
        :returns: String containing an `svg` visual representation of the graph.
        
        Generates a visual representation of the graph in `svg` format for web/visualization use.
        """
        def _addcircle(a, complete, cls):
            circle = "  <circle id='node-{}' class='{}' data-rules='{}' transform='matrix(1 0 0 1 {} {})' r='{}' stroke='black' stroke-width='2'><title>{}</title></circle>"
            if a not in complete:
                name = re.sub('[.<>:]', '', a.name)
                return circle.format(name, cls, name, a.svg.x, a.svg.y, 6, a.name), complete | set([a])
            return "", complete
        
        # Init chart
        palette = itertools.cycle([[0xb5, 0x89, 0x00], [0xcb, 0x4b, 0x16], [0xd3, 0x36, 0x82], [0x6c, 0x71, 0xc4],
                                   [0x26, 0x8b, 0xd2], [0x2a, 0xa1, 0x98], [0x85, 0x99, 0x00]])
        width  = max(1000, max([n.svg.x + 400 for n in self.vertices]))
        height = max(1000, max([n.svg.y + 400 for n in self.vertices]))
        result =  "<svg id='f-svg' width='{}' height='{}' transform='matrix(1 0 0 1 -300 -300)'>".format(width, height)
        result += "  <defs><mask id='clipper' maskUnits='userSpaceOnUse'><rect height='100%' width='100%' fill='white'></rect>{}</mask></defs>"
        result += "  <rect height='100%' width='100%' fill='rgb(253,246,227)'/>"
        line   =  "  <line {{}} x1='{}' y1='{}' x2='{}' y2='{}' style='stroke:rgb({{}});stroke-width:{{}}'/>"
        
        # Draw lines
        complete = set()
        for p, path in enumerate(rules):
            color = ",".join(map(str, next(palette)));
            for i in range(len(path)):
                if i < len(path) - 1:
                    a, b = (path[i][0], path[i + 1][0])
                    if (a, b) not in complete:
                        l  = line.format(a.svg.x, a.svg.y, b.svg.x, b.svg.y)
                        result += l.format("", "0,0,0", 3) + l.format("", color, 1)
                    complete |= set([(a, b), (b, a)])
                
                if path[i][1]:
                    a = path[i][0]
                    x,y = (a.svg.x + (36 * ((i % 2 * 2) - 1)), a.svg.y + (10 * ((i % 2 * 2) - 1)))
                    l = line.format(a.svg.x, a.svg.y, x, y)
                    result += l.format("id='rule-{}-{}-line' class='rules {}' opacity='0.6' mask='url(#clipper)'".format(p, i, a.name.replace('.', "")), "0,0,0", 1)
        
        for a, b in self.edges:
            result += "" if (a,b) in complete else line.format(a.svg.x, a.svg.y, b.svg.x, b.svg.y).format("", "0,0,0", 3)
        
        # Draw circles
        complete = set()
        for path in rules:
            for i in range(len(path)):
                r, complete = _addcircle(path[i][0], complete, "active")
                result += r
        for a in self.vertices:
            r, complete = _addcircle(a, complete, '')
            result += r
        
        # Draw ruleboxes
        group = '''
        <g class='rules {}' id='rule-{}-{}' x='1' y='1' transform='matrix(1 0 0 1 {} {})' opacity='0.6'>
          <use href='#clipping'/>
          <rect width='73' height='34' style='stroke-width:1;stroke:rgb(0,0,0);fill:rgb(238, 232, 213)' rx='4' ry='4'/>
          <text font-size='5' fill='rgb(88,110,117)' y='4'>{}</text>
        </g>'''
        
        masks = ""
        for p, path in enumerate(rules):
            for i in range(len(path)):
                if path[i][1]:
                    a = path[i][0]
                    x,y = (a.svg.x - 26 + (36 * ((i % 2 * 2) - 1)), (a.svg.y - 17 + (27 * ((i % 2 * 2) - 1))))
                    text = "".join(["<tspan dy='1.2em' x='8'>" + line + "</tspan>" for line in path[i][1].split("\n")])
                    result += group.format(re.sub('[.<>:]', '', a.name), p, i, x, y, text)
                    masks  += "<rect width='73' height='34' id='mask-rule-{}-{}' transform='matrix(1 0 0 1 {} {})'></rect>".format(p, i, x, y)
        
        result += "</svg>"
        result = result.format(masks)
        if output:
            with open(output) as f:
                f.write(result)
        
        return result

    @classmethod
    def power_graph(cls, size, gamma=2.5, db=None, subnet='10.0.0.0/8'):
        """
        :param int size: Number of vertices in the graph.
        :param int gamma: Intensity of the connectivity of the graph.
        :param db: :class:`Runtime <unis.runtime.runtime.Runtime>` to store the graph.
        :param str subnet: IP range to use for the vertices.
        
        Generate a graph containing vertices and edges such that the number of egress edges follows
        a power distribution over all vertices.  
        """
        def get_degrees(d=None):
            while not d:
                while not (d and all([sum(d[:k]) <= (k * (k - 1)) + sum(map(lambda x: min(k, x), d[k + 1:])) for k in range(1, size)]) and sum(d) >= 2*(size-1)):
                    d = list(sorted([min(size - 1, math.floor(pow(random.random(), 1.0/(-1 * gamma)))) for _ in range(size)], reverse=True))
                    if sum(d) < 2*(size-1):
                        while sum(d) < 2*(size-1):
                            for i in range(1, max(d)):
                                if len(list(filter(lambda x: x == i, d))) > len(list(filter(lambda x: x == i + 1, d))) * gamma:
                                    d[d.index(i)] += 1
                            d[0] += 1
                    td = copy(d)
                while td:
                    v = td.pop()
                    for j in range(v):
                        if j < len(td):
                            td[j] -= 1
                        else:
                            d = None
                    td = list(sorted(filter(lambda x: x, td), reverse=True))
                
            return d
        
        g = Graph(db=db, subnet=subnet)
        nodes = [[g.createVertex(), d] for d in get_degrees()]
        while nodes:
            n, d = nodes.pop()
            for j in range(d):
                nodes[j][1] -= 1
                g.createEdge(n, nodes[j][0])
            nodes = list(sorted(filter(lambda x: x[1], nodes), key=lambda x: x[1], reverse=True))
        return g
            
            
    @classmethod
    def build_graph(cls, size, degree, db=None, subnet='10.0.0.0/8', prefix=''):
        """
        :param int size: Number of vertices in the graph.
        :param int degree: Intensity of the connectivity of the graph. [0-1]
        :param db: :class:`Runtime <unis.runtime.runtime.Runtime>` to store the graph.
        :param str subnet: IP range to use for the vertices.
        :param str prefix: Prefix for the name of generated vertices.
        
        Generates a randomly distributed graph where each edge is selected independently
        at random.
        """
        degree = max(0, min(degree, 1))
        count = sum(range(size)) * degree
        links = size
        adj = {}
        missing = []
        g = Graph(prefix=prefix, subnet=subnet, db=db)
        g.createVertex()
        
        for i in range(size - 1):
            neighbor = random.randrange(0, len(g.vertices))
            n = g.createVertex()
            adj[i] = neighbor
            g.createEdge(n, g.vertices[neighbor])
            
        for a in range(size):
            for b in range(a + 1, size):
                if a != b and adj.get(a, -1) != b and adj.get(b, -1) != a:
                    missing.append((a, b))
        while links <= count and missing:
            a, b = random.choice(missing)
            g.createEdge(g.vertices[a], g.vertices[b])
            missing.remove((a, b))
            links += 1
        
        return g
        
    @classmethod
    def cluster_graph(cls, size, depth=1, degree=0.5, db=None, subnet='10.0.0.0/8', prefix=''):
        """
        :param int size: Number of vertices in the graph.
        :param int depth: Number of recursions in the cluster.
        :param int degree: Intensity of the connectivity of the graph. [0-1]
        :param db: :class:`Runtime <unis.runtime.runtime.Runtime>` to store the graph.
        :param str subnet: IP range to use for the vertices.
        :param str prefix: Prefix for the name of generated vertices.
        
        Generates a randomly distributed graph where each edge is selected independently
        at random.  Each cluster is connected by a single edge to each other cluster with
        a probability of `degree`.  There are `size` clusters of `size` vertices at each
        depth.  Thus, a `size=5, depth=1` cluster graph contains 25 vertices, while a 
        `size=5, depth=2` cluster graph contains 125 vertices.
        """
        def _createname():
            size = 1
            while True:
                for s in itertools.product(ascii_uppercase, repeat=size):
                    yield "".join(s)
                size += 1
        def _curry(depth):
            def _f(size, degree, db, subnet, prefix):
                return cls.cluster_graph(size, depth, degree, db, subnet, prefix)
            return _f
        
        if depth == 1:
            func = Graph.build_graph
        else:
            func = _curry(depth - 1)
        
        names = _createname()
        _prefix = "{}{}.".format(prefix, next(names))
        g = func(size, degree, db, subnet, _prefix)
        result = Graph(g.vertices, g.edges, db=db, subnet=subnet, prefix=_prefix)
        for _ in range(size - 1):
            _prefix = "{}{}.".format(prefix, next(names))
            g = func(size, degree, db, subnet, _prefix)
            out_gateway = random.choice(g.vertices)
            in_gateway = random.choice(result.vertices)
            result.vertices.extend(g.vertices)
            result.edges.extend(g.edges)
            result.createEdge(in_gateway, out_gateway)
        
        return result
