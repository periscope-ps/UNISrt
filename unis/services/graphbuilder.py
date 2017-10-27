import itertools
import math
import random
import re

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
    
    def spring(self, intensity, repeat=1000):
        
        
        # initialize
        if self.processing_level == 0:
            for n in self.vertices:
                n.svg = { "x": 0, "y": 0 }
        sidelen = int(math.sqrt(len(self.vertices)))
        for i, n in enumerate(self.vertices):
            x, y = (20 * (i % sidelen), 20 * (i // sidelen))
            n.svg.x, n.svg.y = (x, y)
        
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
                if a != b:
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
            n.svg.x += abs(min(x - 20, 0)) + 400
            n.svg.y += abs(min(y - 20, 0)) + 400
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
    
    def svg(self, rules=[], output=None):
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
