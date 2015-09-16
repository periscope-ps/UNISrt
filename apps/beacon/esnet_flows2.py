#!/usr/bin/python

import sys
import argparse
import httplib
import json

DPID_LBL = "00:00:00:00:00:00:00:18"
DPID_NER = "00:00:00:00:00:00:00:0a"
DPID_BNL = "00:00:00:00:00:00:00:03"
DPID_ANL = "00:00:00:00:00:00:00:02"

#DPID_LBL = "00:00:00:00:00:00:00:01"
#DPID_NER = "00:00:00:00:00:00:00:02"
#DPID_ANL = "00:00:00:00:00:00:00:03"
#DPID_BNL = "00:00:00:00:00:00:00:04"

node = {}

node['nersc'] = {
    'port_a': '25',
    'port_z': '26',
    'switch': DPID_NER}    

node['lbl'] = {
    'port_a': '17',
    'port_z': '18',
    'switch': DPID_LBL}

node['anl'] = {
    'port_a': '25',
    'port_z': '26',
    'switch': DPID_ANL}

paths = {}

paths['2hop'] = [{'hop':'lbl', 'vlan_a':'3291', 'vlan_z':'3291'},
                 {'hop':'nersc', 'vlan_a':'3291', 'vlan_z':'3292'},
                 {'hop':'anl', 'vlan_a':'3291', 'vlan_z':'3292'}]

paths['1hop'] = [{'hop':'lbl', 'vlan_a':'3291', 'vlan_z':'3293'},
                 {'hop':'anl', 'vlan_a':'3291', 'vlan_z':'3293'}]

# tmp
paths['myhop'] = [
                  {'hop':'lbl', 'vlan_a':'3291', 'vlan_z':'3291'},
                  
                  
                  
                  
                  {'hop':'nersc', 'vlan_a':'3291', 'vlan_z':'3292'},
                  #{'hop':'nersc', 'vlan_a':'3291', 'vlan_z':'3295'},
                  {'hop':'nersc', 'vlan_a':'3294', 'vlan_z':'3292'},
                  #{'hop':'nersc', 'vlan_a':'3294', 'vlan_z':'3295'},
                  
                  
                  
                  
                  
                  
                  {'hop':'anl', 'vlan_a':'3291', 'vlan_z':'3292'}
                  ]
# tmp end

class RestApi(object):

    def __init__(self, server, port):
        self.server = server
        self.port = port


    def get(self, data):
        ret = self.rest_call({}, 'GET')
        return json.loads(ret[2])

    def set(self, data):
        ret = self.rest_call(data, 'POST')
        return ret[0] == 200

    def remove(self, data):
        ret = self.rest_call(data, 'DELETE')
        return ret[0] == 200

    def rest_call(self, data, action):
        #path = '/wm/staticflowentrypusher/json'
        path = '/wm/staticflowpusher/json'
        headers = {
            'Content-type': 'application/json',
            'Accept': 'application/json',
            }
        body = json.dumps(data)
        conn = httplib.HTTPConnection(self.server, self.port)
        conn.request(action, path, body, headers)
        response = conn.getresponse()
        ret = (response.status, response.reason, response.read())
        print ret
        conn.close()
        return ret

def make_direct_flow(dpid, entry, name, vlan):
    return {'switch': dpid,
            'name': 'direct-'+str(name),
            'active': 'true',
            'vlan-id': vlan,
            'dst-mac': entry['mac'],
            'actions': 'output='+str(entry['port'])
            }

def make_rewrite_flow(dpid, smac, inp, outp, inv, outv, name):
    
    # temporary solution
    # matching ipv4_dst on flows coming from vlan 3292
    import re
    p = re.compile('nersc-*')
    if outv == '3291' and p.match(name):
        return {'switch': dpid,
            'name': 'mod-'+str(name),
            'active': 'true',
            #'vlan-id': inv,
            #'eth_vlan_vid': inv,
            #'ingress-port': inp,
            #'in_port': inp,
            #'src-mac': smac,
            'priority': 1000,
            'eth_type': 0x0800,
            'ipv4_dst': '192.168.2.3',
            'actions': 'set_vlan_vid='+str(outv)+',output='+str(outp)
            }
    
    if outv == '3294' and p.match(name):
        return {'switch': dpid,
            'name': 'mod-'+str(name),
            'active': 'true',
            #'vlan-id': inv,
            #'eth_vlan_vid': inv,
            #'ingress-port': inp,
            #'in_port': inp,
            #'src-mac': smac,
            'priority': 1000,
            'eth_type': 0x0800,
            'ipv4_dst': '192.168.2.4',
            'actions': 'set_vlan_vid='+str(outv)+',output='+str(outp)
            }
        
    # end of temporary solution
        
    return {'switch': dpid,
            'name': 'mod-'+str(name),
            'active': 'true',
            #'vlan-id': inv,
            'eth_vlan_vid': inv,
            #'ingress-port': inp,
            'in_port': inp,
            #'src-mac': smac,
            'priority': 1000,
            'actions': 'set_vlan_vid='+str(outv)+',output='+str(outp)
            }

def make_path_flows(path):
    
    # temporary solution
    # ARP from ANL to LBL needs an explicit flow
    # It doesn't look right to me, since ARP needs be forwarded to all vlans. Works for now...
    def add_arp_flows(dpid, smac, inp, outp, inv, outv, name):
        '''import re
        p = re.compile('nersc-*')        
        if outv == '3291' and p.match(name):
            return {'switch': dpid,
                    'name': 'mod-'+str(name),
                    'active': 'true',
                    #'vlan-id': inv,
                    #'eth_vlan_vid': inv,
                    #'ingress-port': inp,
                    'in_port': inp,
                    #'src-mac': smac,
                    'priority': 1000,
                    'eth_type': 0x0806,
                    'actions': 'set_vlan_vid='+str(outv)+',output=all'#+str(outp)
                    #'actions': 'output=all'
            }'''
            
        return None
    # end of temporary solution
    
    pflows = []
    for h in path:
        n = node[h['hop']]
        
        name = h['hop']+'-'+n['port_a']+'-'+h['vlan_a']+'-'+n['port_z']+'-'+h['vlan_z']
        pflows.append(make_rewrite_flow(n['switch'], None, n['port_a'], n['port_z'], h['vlan_a'], h['vlan_z'], name))
        
        extra = add_arp_flows(n['switch'], None, n['port_a'], n['port_z'], h['vlan_a'], h['vlan_z'], name+'extra')
        if extra:
            pflows.append(extra)
            
        name = h['hop']+'-'+n['port_z']+'-'+h['vlan_z']+'-'+n['port_a']+'-'+h['vlan_a']
        pflows.append(make_rewrite_flow(n['switch'], None, n['port_z'], n['port_a'], h['vlan_z'], h['vlan_a'], name))
        
        extra = add_arp_flows(n['switch'], None, n['port_z'], n['port_a'], h['vlan_z'], h['vlan_a'], name+'extra')
        if extra:
            pflows.append(extra)
        
    return pflows

usage_desc = """
test_flow.py {add|del} [path id] ...
"""

parser = argparse.ArgumentParser(description='process args', usage=usage_desc, epilog='foo bar help')
parser.add_argument('--ip', default='localhost')
parser.add_argument('--port', default=9000)
parser.add_argument('cmd')
parser.add_argument('path', nargs='?', default=None)
parser.add_argument('otherargs', nargs='*')
args = parser.parse_args()

#print "Called with:", args
cmd = args.cmd

# handle to Floodlight REST API
rest = RestApi(args.ip, args.port)

flows = []

if args.path in ['1hop', '2hop', 'myhop']:
    flows.extend(make_path_flows(paths[args.path]))
else:
    print "Unknown path"
    exit

for f in flows:
    if (cmd=='add'):
        print "Adding flow:\n", f
        rest.set(f)
    if (cmd=='del'):
        print "Deleting flow:\n", f
        rest.remove(f)
