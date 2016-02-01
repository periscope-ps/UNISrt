'''
This command should take an input config file and generate corresponding nre topo objects accordingly
Note that, this command will also create and upload ipports to unis, if it learns this from the config file
'''
import re
import trie
from collections import defaultdict

import kernel.models as models
from libnre.utils import *

def makedict(self, remain):
    '''
    parse a juniper configuration into a dictionary of its interfaces
    '''
    ret = defaultdict(dict)
    while True:
        if len(remain) == 0:
            break
        line = remain.pop(0)
        if "inactive:" in line:
            if ";" in line:
                continue
            elif "{" in line:
                self.closection(remain, 1)
            continue
        elif "}" in line:
            # here put an extra syntax requirement, that } must
            # be in a separate line
            break
        else:
            line_sp = line.split()
            if len(line_sp) == 1:
                key = line_sp[0].strip().rstrip(';')
                value = True
                ret[key] = value
            elif line_sp[-1] == "{":
                if len(line_sp) == 2:
                    key = line_sp[0].strip()
                    value = self.makedict(remain)
                    ret[key] = value
                else:
                    assert len(line_sp) == 3
                    key = line_sp[0].strip()
                    value = self.makedict(remain)
                    try:
                        ret[key][line_sp[1].strip()] = value
                    except TypeError:
                        print key
                        print line_sp[1].strip()
                        print value
                        exit
            elif ";" in line_sp[-1]:
                if line_sp[0] == "description":
                    # haven't come up with a good idea to deal with ""'s
                    ret[line_sp[0]] = ' '.join(line_sp[1:])
                elif line_sp[0] == "vlan-tags":
                    tags = {}
                    if "outer" in line_sp:
                        outer = line_sp[line_sp.index('outer') + 1]
                        tags["outer"] = outer
                    if "inner" in line_sp:
                        inner = line_sp[line_sp.index('inner') + 1]
                        tags["inner"] = inner
                    ret[line_sp[0]] = tags                    
                elif len(line_sp) == 2:
                    key = line_sp[0]
                    value = line_sp[1].strip().rstrip(';')
                    # an 'ip address' or an 'inet/inet6 family' is special,
                    # as they may be a key or a value
                    if key == "address" or key == "family":
                        ret[key][value] = None
                    else:
                        ret[key] = value
                else:
                    # there are still some keywords left unprocessed, see testconf
                    print line
        
    return ret

def buildIPresolver(unisrt, tree):
    '''
    return a dictionary that maps an IP to an L2 port
    '''
    ip_resolver = {}
    for node, ports in tree.iteritems():
        for port, port_conf in ports.iteritems():
            if "unit" not in port_conf:
                continue
            for unit_conf in port_conf["unit"].itervalues():
                try:
                    for ip in unit_conf["family"]["inet"]["address"].iterkeys():
                        ip_resolver[ip[:-3]] = node
                        # don't upload ipport at this moment (even though I don't do it, unisencoder+esnetrspec already done it)
                        # these rules to compose UNIS refs are so artificial
                        # don't use 'ip' list of ports, because they may not exist in the topo
                        models.ipport({'address': {
                                        'address': ip[:-3],
                                        'type': 'ipv4'
                                   },
                                   'relations': {
                                        'over': [{'href': unisrt.unis_url + '/ports/' + node + '_port_' + port.replace('/', '_')}]
                                   },
                                   'node': unisrt.unis_url + '/nodes/' + node
                                   }, unisrt, False)
                except (KeyError, TypeError):
                    continue
                    
    # Okay, now let's build ip_resolver for ESnet. Really should just use ipport dictionary...
    tmp = {}
    for k, v in unisrt.ipports['existing'].iteritems():
        if k in ip_resolver:
            continue
        try:
            tmp[k] = v.node.id
        except:
            tmp[k] = v.node
    ip_resolver.update(tmp)
        
    return ip_resolver

def resolve(unisrt, in_res_list):
    '''
    This function is supposed to find the UNIS resources determined by the input.
    The input should be a list of ip addresses obtained by user's traceroute.
    This function should query UNIS and resolve a list of resource.
    
    IP addresses -> ingress and egress L2 ports (we assume L2 ports are the essences
    of resource scheduling)
    '''
    ip_resolver = unisrt._resources["ipports"]._contents
    
    # use ipports to build forwarding table for each node
    # !!! apparently, shouldn't be done each time got invoked, too expensive !!!
    nodes = {}
    for i in ip_resolver:
        nodes.setdefault(i["attach"]["node"], {})[ToBin(i["ipaddress"])] = i["attach"]["port"]
    for key, value in nodes.iteritems():
        nodes[key] = trie(value)
    
    # resolve the egress port
    out_res_list = []
    for res in in_res_list:
        out_res_list.append(res)
        try:
            next_hop = in_res_list[in_res_list.index(res) + 1]["ref"]
        except IndexError:
            # the last item from input
            pass
        # !!! ATTENTION: ip_resolver is a list, and cannot be indexed by an ip yet
        # !!! needs fixing
        egress = nodes[ip_resolver[res]["attach"]["node"]].longest_prefix_value(next_hop)
        out_res_list.append(egress)
    
    for index in range(len(out_res_list)):
        try:
            # egress may already be in the form of lower level port
            out_res_list[index] = ip_resolver[out_res_list[index]]["attach"]["port"]
        except IndexError:
            continue
    return out_res_list
        
    '''
    # read topology from keel directly instead of UNIS
    top = r2u()
    
    # construct IP oriented data structure
    ip_resolver = {}
    for node, ports in top.iteritems():
        for port, port_conf in ports.iteritems():
            if "unit" not in port_conf:
                continue
            for unit_conf in port_conf["unit"].itervalues():
                try:
                    for ip in unit_conf["family"]["inet"]["address"].iterkeys():
                        # should I check the uniqueness here?
                        ip_resolver[ip] = {"node": node, "port": port}
                except (KeyError, TypeError):
                    continue
    
    # first pass, fill the egress port
    out_res_list = []
    for res in in_res_list:
        out_res_list.append(res)
        try:
            next_hop = in_res_list[in_res_list.index(res) + 1]
        except IndexError:
            # the last item from input
            pass
        for port_conf in top[ip_resolver[res]["node"]].itervalues():
            if "unit" not in port_conf:
                continue
            for unit_conf in port_conf["unit"].itervalues():
                try:
                    for ip in unit_conf["family"]["inet"]["address"].iterkeys():
                        if ToBin(next_hop) == ToBin(ip):
                            # needs a better algorithm to determine egress ip
                            # at least shouldn't ToBin(next_hop) inside loops
                            out_res_list.append(ip)
                            # should actually break 3 loops
                            break
                except (KeyError, TypeError):
                    continue
    
    # second pass, translate the IPs to L2 ports
    for index in range(len(out_res_list)):
        out_res_list[index] = ip_resolver[out_res_list[index]]["port"]
        
    return out_res_list
    '''
            
if __name__ == '__main__':
    with open('/home/mzhang/workspace/UNISrt/samples/juniper/configs', 'r') as f:
        parentheses = 0
        interfaces = []
        for line in f:
            assert parentheses >= 0
            if parentheses or re.match( r'^domain_ion\.internet2\.edu_node_rtr\.', line):
                interfaces.append(line.rstrip('\n'))
                if "{" in line:
                    parentheses += 1
                elif "}" in line:
                    parentheses -= 1
            else:
                continue
                
    tree = makedict(interfaces)
    buildIPresolver(unisrt, tree)
    resolve(["108.59.26.9/31", "64.57.28.5/31"])