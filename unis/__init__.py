from unis.runtime import Runtime

def from_community(community, root=None):
    import requests, socket
    from urllib.parse import urlparse
    def pred(u):
        url = urlparse(u)
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(0.1)
                s.connect((url.hostname, url.port))
                return True
        except Exception as e:
            return False

    root = root or settings.DEFAULT_ROOT
    path = "/register?fields=communities,accessPoint&communities={}".format(community)

    instances = requests.get(root + path).json()
    instances = list(filter(pred, [instance['accessPoint'] for instance in instances]))
    return Runtime(instances)
