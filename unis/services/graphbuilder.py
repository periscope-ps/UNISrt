import itertools
import math
import random

from collections import defaultdict
from string import ascii_uppercase

from unis.models import Node, Port, Link

class Graph(object):
    def __init__(self, vertices=[], edges=[], db=None, subnet='10.0.0.0/8', prefix=''):
        address, mask = subnet.split("/")
        subnet_size = 4 - int(mask) // 8
        self.nextip = [0 for _ in range(subnet_size)]
        self.subnet = ".".join(address.split(".")[:(4 - subnet_size)])
        self.subnet += ".{}".format(".".join(["{}" for x in range(subnet_size)]))
        self.prefix = prefix
        self.vertices = list(vertices)
        self.edges = list(edges)
        self.height = 0
        self.width  = 0
        if db:
            self._rt = db
        else:
            from unis.runtime import Runtime
            self._rt = Runtime(defer_update=True)
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
        n = Node({ "name": "{}{}".format(self.prefix, len(self.vertices)) })
        n.svg = {}
        self._rt.insert(n)
        self.vertices.append(n)
        return n
        
    def createEdge(self, src, dst):
        p_src = Port({"index": str(len(src.ports)) })
        p_dst = Port({"index": str(len(dst.ports)) })
        p_src.address.address = self.subnet.format(*self._nextaddr())
        p_src.address.type = "ipv4"
        p_dst.address.address = self.subnet.format(*self._nextaddr())
        p_dst.address.type = "ipv4"
        self._rt.insert(p_src)
        self._rt.insert(p_dst)
        l = Link({ "directed": False, "endpoints": [p_src, p_dst] })
        self._rt.insert(l)
        src.ports.append(p_src)
        dst.ports.append(p_dst)
        self.edges.append((src, dst))
        self.edges.append((dst, src))
    
    def finalize(self, include_svg=False):
        for n in self.vertices:
            if include_svg:
                n.extendSchema('svg')
            for p in n.ports:
                p.commit()
                p.link.commit()
            n.commit()
        self._rt.flush()
    
    def _getclusters(self):
        clusters = defaultdict(list)
        for n in self.vertices:
            clusters[".".join(n.name.split(".")[:-1])].append(n)
        return clusters
        
    def spring(self, intensity, repeat=1000):
        def _place(x, y, width, cluster):
            node_index = 0
            while node_index < len(cluster):
                cluster[node_index].svg.x = (node_index % width * intensity) + x
                cluster[node_index].svg.y = (node_index // width * intensity)  + y
                node_index += 1
                
        # initialize
        if self.processing_level == 0:
            for n in self.vertices:
                n.svg = { "x": 0, "y": 0 }
        clusters = list(self._getclusters().values())
        sidelen = int(math.sqrt(len(clusters)))
        cluster_index = 0
        x = 20
        y = 20
        while cluster_index < len(clusters):
            cluster = clusters[cluster_index]
            cluster_len = int(math.sqrt(len(cluster)))
            _place(x, y, cluster_len, cluster)
            cluster_index += 1
            x = 20 + (cluster_index % sidelen) * cluster_len * intensity
            y = 20 + (cluster_index // sidelen) * (cluster_len + 1) * intensity
        
        # spring
        for x in range(repeat):
            bar = "#" * int(30 * (x / repeat)) 
            bar += "-" * 30
            print("\rRunning spring simulation [{}]".format(bar[:30]), end='\r')
            forces = {}
            
            # Repulsive
            for n in self.vertices:
                forces[n] = {}
                for adj in self.vertices:
                    if n == adj:
                        continue
                    d = math.sqrt((adj.svg.x - n.svg.x)**2 + (adj.svg.y - n.svg.y)**2)
                    mag = -(intensity * 2) / d**2
                    angle = math.atan2((adj.svg.y - n.svg.y), (adj.svg.x - n.svg.x))
                    forces[n][adj] = (mag * math.cos(angle), mag * math.sin(angle))
            
            # Attractive
            for a,b in self.edges:
                d = math.sqrt((b.svg.x - a.svg.x)**2 + (b.svg.y - a.svg.y)**2)
                mag = 1.5 * math.log(d / intensity)
                angle = math.atan2((b.svg.y - a.svg.y), (b.svg.x - a.svg.x))
                forces[a][b] = (mag * math.cos(angle), mag * math.sin(angle), a.name, b.name)
                angle = math.atan2((a.svg.y - b.svg.y), (a.svg.x - b.svg.x))
                forces[b][a] = (mag * math.cos(angle), mag * math.sin(angle), a.name, b.name)
            
            # Apply Forces
            for n in self.vertices:
                x = sum([x[0] for x in forces[n].values()]) * 0.1
                n.svg.x += x
                y = sum([x[1] for x in forces[n].values()]) * 0.1
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
            n.svg.x += abs(min(x - 20, 0)) + 53
            n.svg.y += abs(min(y - 20, 0)) + 35
        self.processing_level += repeat
    
    def dump(self, filename):
        result = { "nodes": {}, "_processing_level": self.processing_level }
        if self.processing_level:
            import json
            for n in self.vertices:
                result["nodes"][n.name] = [n.svg.x, n.svg.y]
            with open(filename, 'w') as f:
                f.write(json.dumps(result))
    
    def load(self, filename):
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

    def svg(self, active=[], rules=[], output=None):
        complete = set()
        height = 0
        width = 0
        for n in self.vertices:
            width = max(width, n.svg.x + 2 * 53) # Add the height and width of potential rules
            height = max(height, n.svg.y + 2 * 35)
        result =     "<svg id='f-svg' width='{}' height='{}' transform='translate(0 0) scale(1)'>".format(width + 20, height + 20)
        for a, b in self.edges:
            if (a,b) not in complete:
                result += "  <line x1='{}' y1='{}' x2='{}' y2='{}' style='stroke:rgb(0,0,0);stroke-width:3'/>".format(a.svg.x, a.svg.y, b.svg.x, b.svg.y)
            complete |= set([(a, b), (b, a)])
        
        circle = "  <circle data-rules='{}' cx='{}' cy='{}' r='{}' stroke='black' stroke-width='2' fill='{}'><title>{}</title></circle>"
        active_nodes = set(itertools.chain.from_iterable(active))
        for n in self.vertices:
            if n not in active_nodes:
                result += circle.format(n.name.replace(".", ""), n.svg.x, n.svg.y, 6, "rgb(255, 255, 255)", n.name)
        
        nodes = set()
        palette = itertools.cycle([[0xe0, 0xb9, 0xb2], [0xd0, 0x97, 0x8b], [0xc1, 0x74, 0x65], [0xb1, 0x52, 0x3f],
                                   [0xaa, 0x41, 0x2c], [0x8c, 0x36, 0x25], [0xe5, 0xae, 0xb9], [0xda, 0x8d, 0x9e]])
        for path in active:
            color = ",".join(map(str, next(palette)))
            for i in range(len(path) - 1):
                a = path[i]
                b = path[i + 1]
                result += "  <line x1='{}' y1='{}' x2='{}' y2='{}' style='stroke:rgb(0,0,0);stroke-width:3'/>".format(a.svg.x, a.svg.y, b.svg.x, b.svg.y)
                result += "  <line x1='{}' y1='{}' x2='{}' y2='{}' style='stroke:rgb({});stroke-width:1'/>".format(a.svg.x, a.svg.y, b.svg.x, b.svg.y, color)
                nodes |= set([a, b])
        
        line = "<line class='rules {}' id='rule-{}-line' x1='{}' y1='{}' x2='{}' y2='{}' style='stroke:black;stroke-width=2' opacity='0.6'/>"
        for i, (node, _) in enumerate(rules):
            if i % 2:
                x, y = (node.svg.x + 36, node.svg.y + 10)
            else:
                x, y = (node.svg.x - 36, node.svg.y - 10)
            result += line.format(node.name.replace('.', ""), i, node.svg.x, node.svg.y, x, y)
            
        for n in nodes:
            result += circle.format(n.name.replace(".", ""), n.svg.x, n.svg.y, 5, "rgb(255,0,0)", n.name)
        
        for i, (node, text) in enumerate(rules):
            if i % 2:
                x, y = (node.svg.x + 10, node.svg.y + 10)
            else:
                x, y = (node.svg.x - 63, node.svg.y - 45)
            
            rule =  "<g class='rules {}' id='rule-{}' x='1' y='1' transform='translate({} {})' opacity='0.6'>".format(node.name.replace(".", ""), i, x, y)
            rule += "  <rect width='53' height='35' style='stroke-width:1;stroke:rgb(0,0,0);fill:rgb(250, 240, 240)' rx='4' ry='4'/>"
            rule += "  <text font-size='5' fill='rgb(50,50,50)' y='4'>"
            for line in text.split("\n"):
                rule += "<tspan dy='1.2em' x='8'>" + line + "</tspan>"
            rule += "</text>"
            rule += "</g>"
            
            result += rule
        result += "</svg>"
        if output:
            with open(output) as f:
                f.write(result)
        
        return result
        
    @classmethod
    def build_graph(cls, size, degree, db=None, subnet='10.0.0.0/8', prefix=''):
        degree = max(0, min(degree, 1))
        count = sum(range(size)) * degree
        links = size
        adj = {}
        missing = []
        g = Graph(prefix=prefix)
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
