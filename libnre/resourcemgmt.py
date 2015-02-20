'''
Created on Feb 12, 2015

@author: mzhang
'''
import re, json
import time
from collections import defaultdict
from copy import deepcopy

from kernel import models
from libnre.utils import *

TRACEROUTE = "ps:tools:blipp:linux:net:traceroute:hopip"

def getResourceLists(unisrt, services):
    '''
    take a list of services (BLiPP services etc.), each of which runs on a node, and return
    the resource lists between each pair.
    It obtains resource lists via various approaches: query a graph, operate BLiPP to run
    an actual traceroute or other possible solutions. Eventually, a cross-layer-graph is constructed
    according to how much we know about the network. And the resource list is derived from this graph
    '''    
    def makedict(self, remain):
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

    def buildIPresolver():
        '''
        trying to consult the router config files (for the I2 case)
        note that, for the case of ESnet, we can instead use its ipport info directly
        '''
        with open('/home/mzhang/workspace/UNISrt/samples/KEEL/configs', 'r') as f:
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
        
    def runTR(src, dst):
        # start making a traceroute measurement object
        measurement = build_measurement(unisrt, src.selfRef)
        measurement['eventTypes'] = [TRACEROUTE]
        measurement['configuration']['name'] = "traceroute"
        measurement['configuration']['regex'] = "^\\s*\\d+.*(?P<hopip>\(.*\))"
        measurement['configuration']['eventTypes'] = {"hopip": TRACEROUTE}
        measurement['configuration']['probe_module'] = "traceroute_probe"        
        measurement['configuration']['collection_schedule'] = "builtins.simple"
        measurement['configuration']['reporting_params'] = 1
        measurement['configuration']['address'] = dst.ip
        measurement['configuration']['command'] = "traceroute %s" % measurement['configuration']['address']
                
        # some default fields, otherwise BLiPP will add this info itself and post the measurement again
        measurement['configuration']['collection_size'] = 10000000
        measurement['configuration']['collection_ttl'] = 1500000
        measurement['configuration']['schedule_params'] = {"every": 30}
                
        traceroutes.append((measurement, src, dst))
        unisrt.updateRuntime([measurement], models.measurement, True)
        
    def psapi(src, dst):
        '''
        in case traceroute information can be found through periscope API (and downloaded as a file)
        '''
        try:
            with open('/home/mzhang/workspace/UNISrt/samples/HELM/i2_trace', 'r') as f:
                i2tr = json.load(f)
            with open('/home/mzhang/workspace/UNISrt/samples/HELM/esnet_trace', 'r') as f:
                esnettr = json.load(f)
                
            i2tr.update(esnettr)            
            return i2tr[' '.join([src, dst])]
        
        except (IOError, KeyError):
            return None

    def incr(ntwkrsrc):
        '''
        currently, L2 graph is of name strings, rather than a graph of objects (should be updated momentarily)
        So, you need to know what object does a name represent, in order to retrieve it from UNISrt
        '''
        if unisrt.unis_url + '/nodes/' + ntwkrsrc in unisrt.nodes['existing']:
            unisrt.nodes['existing'][unisrt.unis_url + '/nodes/' + ntwkrsrc].usecounter += 1
        elif ntwkrsrc in unisrt.links['existing']:
            unisrt.links['existing'][ntwkrsrc].usecounter += 1
            for s, d in unisrt.links['existing'][ntwkrsrc].endpoints.iteritems():
                try:
                    unisrt.ports['existing']['selfRef'][s].usecounter += 1
                    unisrt.ports['existing']['selfRef'][d].usecounter += 1
                except KeyError as e:
                    print e
        elif ntwkrsrc in unisrt.ipports['existing']:
            unisrt.ipports['existing'][ntwkrsrc].usecounter += 1
        else:
            print ntwkrsrc + " cannot be found in UNISrt"

    paths = {}
    # pair up to create m(m-1) traceroute measurements
    traceroutes = []
    for src in services:
        for dst in services:
            if src.id == dst.id:
                continue
            paths[(src.node.name, dst.node.name)] = psapi(src.node.name, dst.node.name)
            if not paths[(src.node.name, dst.node.name)]:
                runTR(src, dst)

    unisrt.syncRuntime(resources = [models.measurement])
        
    while traceroutes:
        time.sleep(60)
        unisrt.syncRuntime(resources = [models.metadata])
        found = []
        for v in traceroutes:
            if '.'.join([v[0]['selfRef'], TRACEROUTE]) in unisrt.metadata['existing']:                    
                hops = unisrt.poke_remote(unisrt.metadata['existing']['.'.join([v[0]['selfRef'], TRACEROUTE])].id)
                paths[(v[1].node.name, v[2].node.name)] = hops[0]['value']
                    
                # turn off this traceroute measurement, after it posted its result
                v[0]['configuration']['status'] = "OFF"
                unisrt.updateRuntime([v[0]], models.measurement, True)

                found.append(v)
            else:
                break

        map(lambda x: traceroutes.remove(x), found)
        break

    unisrt.syncRuntime(resources = [models.measurement])

    ip_resolver = buildIPresolver()

    for v in paths.values():
        load = []
        temp = deepcopy(v)
        for hop in temp:
            if hop in ip_resolver:
                incr(hop)
                v[v.index(hop)] = ip_resolver[hop] # at the IP-L2 edge, we only map the vertex to a node(switch/router)
                load.append(ip_resolver[hop])
            else:
                # when an IP hop cannot be mapped to a L2 port, it's like floating in the air
                # it exists in the L3 topo but lacks info to tie it to any hardware.
                # these are the third kind of ipports (first two kinds are built by rspec and router config respectively)
                # don't upload ipport at this moment
                models.ipport({'address':{'address': hop, 'type': 'ipv4'}}, unisrt, False)
                incr(hop)
                del load[:]

            if len(load) == 2:
                resourcesL2 = []
                map(lambda x: resourcesL2.extend(list(x)), unisrt.graphL2.dijkstra(*load)[1])
                v[v.index(ip_resolver[hop]) - 1 : v.index(ip_resolver[hop]) + 1] = resourcesL2[:-1]
                load = [load[1]]
                    
        # update counters after the entire path got identified, to avoid double counting on an adjacent switch
        map(incr, list(set(v) - set(temp)))

    return paths
    
def getGENIResourceLists(unisrt, pairs):
    '''
    in GENI, L3 resources are hidden by either stitching VLANs or local VMs. It requires a different way to identify the path
    After users reserve their slice, the (stitching) manifest can be uploaded to UNIS paths objects for this function to query
    '''        
    paths = {}
    for pair in pairs:
        key = unisrt.unis_url + "/paths/" + unisrt.services['existing'][pair['from']].node.name + \
        '%' + unisrt.services['existing'][pair['to']].node.name
        hops = unisrt.paths['existing'][key].hops
            
        dst_ip_set = map(lambda x: x.address, unisrt.services['existing'][pair['to']].node.ipports.values())
        candi0 = unisrt.ports['existing'][unisrt.unis_url + "/ports/" + hops[0].replace('+', '_')].ip
        candi1 = unisrt.ports['existing'][unisrt.unis_url + "/ports/" + hops[-1].replace('+', '_')].ip
        dst_ip = filter(lambda x: x in dst_ip_set, [candi0, candi1])
        assert len(dst_ip) == 1
        dst_ip = dst_ip[0]
        paths[(pair['from'], dst_ip)] = hops
        #paths[(src, dst)].insert(0, src.over)
        #paths[(src, dst)].append(dst.over)
        
    return paths