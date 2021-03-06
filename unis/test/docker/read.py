from unis import Runtime
from unis.services.graph import UnisGrapher
from unis.services.data import DataService

rt = Runtime('http://db2:8888', runtime={'services': [UnisGrapher(), DataService()]})
rt.nodes.load()
rt.ports.load()

# Discover

print(".")
p = rt.nodes[0].ports[0]

assert len(rt.nodes) == 13, "Incorrect number of nodes after discovery: " + str(len(rt.nodes))
print(".")
assert len(rt.ports) == 22, "Incorrect number of ports after discovery: " + str(len(rt.ports))
print(".")
rt.ports.load()
rt.nodes.load()
rt.links.load()

# where

assert len(list(rt.nodes.where({'name': 'node_0'}))) == 1, "Incorrect number of matching nodes: " + str(list(rt.nodes.where({'name': 'node_0'})))
print(".")
assert list(rt.nodes.where({'name': 'node_0'}))[0].name == 'node_0', "Incorrect node returned from where"
print(".")

# chain

n = rt.nodes.first_where({'name': 'node_0'})

assert n.ports[0] in n.ports[0].link.endpoints
print(".")

# Create metadata

from unis.models import Metadata
from unis.measurements import Last

data = rt.insert(Metadata({'subject': n}), commit=True).data

rt.flush()

data.append(5)
data.attachFunction(Last())

assert data.last == 5
print(".")
