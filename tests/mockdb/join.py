import json, sys

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Invalid arguments")
        exit(-1)
    with open(sys.argv[1], 'r') as f:
        a = json.load(f)
    with open(sys.argv[2], 'r') as f:
        b = json.load(f)

    with open(sys.argv[2], 'w') as f:
        json.dump(a + b, f, indent=2)
