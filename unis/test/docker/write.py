from unis import Runtime
from unis.models import Node, Link, Port

from time import sleep

rt = Runtime('http://db1:8888')

ports = [rt.insert(Port({'name': "port_{}".format(i)}), commit=True) for i in range(20)]
nodes = [rt.insert(Node({'name': "node_{}".format(i), 'ports': [ports[i*2], ports[(i*2)+1]]}), commit=True) for i in range(10)]
links = [rt.insert(Link({'directed': False, 'endpoints': [ports[(i*2)+1], ports[((i*2)+2) % 20]]}), commit=True) for i in range(10)]

scratch_node = rt.insert(Node({"name": "scratch_node"}), commit=True)
scratch_node.ports.append(rt.insert(Port(), commit=True))
scratch_node.foo = "test_extend"
scratch_node.extendSchema("foo")

test_clone = scratch_node.clone()
rt.insert(test_clone)

assert test_clone.name == "scratch_node", "Cloned node has incorrect name"
print(".")
assert test_clone.foo == "test_extend", "Cloned node has no extended attributes"
print(".")
assert len(test_clone.ports) == 1, "Cloned node has incorrect number of ports"
print(".")
assert test_clone.ports[0] == scratch_node.ports[0], "Cloned node port is not link to correct port"
print(".")

assert len(nodes[0].ports) == 2, "Incorrect number of ports on node[0]"
print(".")
where_test = list(nodes[0].ports.where({'name': 'port_0'}))
assert len(where_test) == 1, ".where function returns incorrect number of ports"
print(".")
assert where_test[0].name == "port_0", ".where function returns wrong port"
print(".")
rt.flush()

# Events/pubsub

ping_node = rt.insert(Node({'name': 'ping'}), commit=True)
rt.flush()

for i in range(40):
    sleep(1)
    if hasattr(ping_node, 'v') and ping_node.v == 'pong':
        break

assert i < 39, "Pong not recieved"
print(".")

# Multisource

xn = rt.insert(Node({"name": "x_node"}), commit=True, publish_to="http://db2:8888")
xp = rt.insert(Port(), commit=True)
xn.ports.append(xp)
print(".")

rt.flush()
