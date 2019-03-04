from unis import Runtime

rt = Runtime('http://db1:8888')

def run():
    while True:
        print("looking for node...")
        for n in rt.nodes:
            if n.name == 'ping':
                print("Found ping node")
                n.extendSchema('v', 'pong')
                rt.flush()
                print("modified... shutting down...")
                return

run()
