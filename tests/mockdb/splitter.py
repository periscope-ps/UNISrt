import json, sys

if __name__ == "__main__":
    name = sys.argv[-1] or "tmp"
    nodes, ports, rels = [], [], []
    d = json.load(sys.stdin)
    for v in d:
        if "node" in v[":type"]:
            nodes.append(v)
        elif "port" in v[":type"]:
            ports.append(v)
        elif "relation" in v[":type"]:
            rels.append(v)
    with open(f"{name}.nodes", 'w') as f:
        json.dump(nodes, f, indent=2)
    with open(f"{name}.ports", 'w') as f:
        json.dump(ports, f, indent=2)
    with open(f"{name}.rels", 'w') as f:
        json.dump(rels, f, indent=2)
