import json, sys

if __name__ == "__main__":
    name = sys.argv[-1] or "tmp"
    nodes, ports, owns, contains = [], [], [], []
    d = json.load(sys.stdin)
    for k,ls in d.items():
        if k == "nodes":
            nodes.extend(ls)
        elif k == "ports":
            ports.extend(ls)
        elif k == "owns":
            owns.extend(ls)
        elif k == "contains":
            contains.extend(ls)
    with open(f"{name}.nodes", 'w') as f:
        json.dump(nodes, f, indent=2)
    with open(f"{name}.ports", 'w') as f:
        json.dump(ports, f, indent=2)
    with open(f"{name}.owns", 'w') as f:
        json.dump(owns, f, indent=2)
    with open(f"{name}.contains", 'w') as f:
        json.dump(contains, f, indent=2)
