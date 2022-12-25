import json, sys, time
from uuid import uuid4

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Invalid arguments")
        exit(-1)
    with open(f"{sys.argv[1]}.nodes", 'r') as f:
        nodes_in = json.load(f)
    with open(f"{sys.argv[1]}.ports", 'r') as f:
        ports_in = json.load(f)
    with open(f"{sys.argv[1]}.rels", 'r') as f:
        owns_in = json.load(f)        
    with open("nodes.json", 'r') as f:
        nodes_out = json.load(f)
    with open("ports.json", 'r') as f:
        ports_out = json.load(f)
    with open("owns.json", 'r') as f:
        owns_out = json.load(f)

"""
  {
    "subject": {
      "name": "",
      "description": "",
      "status": "UNKNOWN",
      "urn": "uuid:port:9b68772d3df3d8a645074a0e60455cdd:lo",
      "expires": 0,
      "address": "lo",
      "addressType": "phy",
      "capacity": 0
    },
    "target": {
      "name": "",
      "description": "",
      "status": "UNKNOWN",
      "urn": "",
      "expires": 1668641313870848,
      "address": "00:00:00:00:00:00",
      "addressType": "eth",
      "capacity": 0
    },
    "index": "",
    ":id": "4b06129d-2916-4d84-a2db-3611dc645e07",
    ":ts": 1668640713871176,
    ":type": "http://unis.open.sice.indiana.edu/schema/2.0.0/relationship#"
  },
"""

        
    host = nodes_in[0]
    for port in ports_in:
        rel = {
            ":id": = str(uuid4()),
            ":type": "http://unis.open.sice.indiana.edu/schema/2.0.0/relationship#",
            ":ts": int(time.time() * 1_000_000)
            "index": "",
            "subject": f"nodes/{node[':id']}",
            "target": f"ports/{port[':id']}"
        }

    with open("nodes.json", 'w') as f:
        json.dump(nodes_in + nodes_out, f, indent=2)
    with open("ports.json", 'w') as f:
        json.dump(ports_in + ports_out, f, indent=2)
    with open("owns.json", 'w') as f:
        json.dump(owns_in + owns_out, f, indent=2)
