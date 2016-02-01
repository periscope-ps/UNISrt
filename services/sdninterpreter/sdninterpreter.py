import subprocess

import kernel.models as models

def configOF(self, paths):
    '''
    the input is a list of path objects e.g. backup paths to enable or,
    diagnose-purpose subpath, which is used by BLiPP agent hosts
    
    
    Note that, this is a placeholder for demo, need real implementation to generate
    SDN controller scripts based on this path list
    '''
    assert isinstance(paths, list)
    for item in paths:
        assert isinstance(item, models.path)
    
    if len(paths) == 2:
        subprocess.call(['apps/beacon/esnet_flows2.py', '--ip=tb-of-ctrl-1.es.net', '--port=9090', 'add', 'myhop'])
    else:
        subprocess.call(['apps/beacon/esnet_flows2.py', '--ip=tb-of-ctrl-1.es.net', '--port=9090', 'del', 'myhop'])
        subprocess.call(['apps/beacon/esnet_flows.py', '--ip=tb-of-ctrl-1.es.net', '--port=9090', 'del', '2hop'])
        subprocess.call(['apps/beacon/esnet_flows.py', '--ip=tb-of-ctrl-1.es.net', '--port=9090', 'add', '1hop'])
    return