'''
Created on Feb 12, 2015

@author: mzhang
'''
import re, json
import time
from collections import defaultdict
from copy import deepcopy

from kernel import models
from libnre.networkfunc import *
from libnre.utils import *

TRACEROUTE = "ps:tools:blipp:linux:net:traceroute:hopip"

def getResourceLists(unisrt, ends, obj_class, obj_layer='l3'):
    '''
    INPUT:
    ends -- a pair of unis objects that can be the ends of a communication
    obj_layer -- specifies the interested layer of the returned resources
    obj_class -- specifies the interested object classes of the returned resources
                 needs to comply to obj_layer
    
    it takes a pair of unis objects (nodes, BLiPP services etc.), and return the resource list in between.
    it obtains the resources via various approaches:
    1. try to find the L3 hops directly from perfSONAR archive
    2. if failed, uses traceroute for BLiPP services* or forwarding simulation for nodes
    3. depends on the requested layers, L2 info may be queried by SSSP algorithm
    4. filter the right classes of objects
    
    ----------------------------------------------------
    * traceroute takes some time, therefore this function should be spawned concurrently to overlap the
      total waiting time, if there are multiple end pairs. shall address this ASAP.
    '''
    
    def incr(ntwkrsrc):
        '''
        currently, L2 graph is of name strings, rather than a graph of objects (should be updated momentarily)
        So, you need to know what object a name represent, in order to retrieve it from UNISrt
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
    
    def vtraceroute(src, dst):
        '''
        need to split this function into two,
        1. query path objects
        2. use forwarding tables
        '''
        try:
            hops = unisrt.paths['existing']['%'.join([src.name, dst.name])].hops
            return hops
        except KeyError:
            return None
        
        hops = [src]
        while hops[-1] != dst:
            out_port = hops[-1].services['routing'].rules[dst.id]
            the_link = unisrt.links['existing'][out_port]
            two_ends = the_link.endpoints
            the_other_end = two_ends.values()[0]
            the_node = unisrt.ports['existing'][the_other_end].node
            hops.append(the_link)
            hops.append(the_node)
        return hops
        
    def run_traceroute(src, dst):
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
        
        unisrt.updateRuntime([measurement], models.measurement, True)
        unisrt.uploadRuntime('measurements')
        
        while True:
            time.sleep(60)
            if '.'.join([measurement['selfRef'], TRACEROUTE]) in unisrt.metadata['existing']:
                # turn off this traceroute measurement, after it posted its result
                measurement['configuration']['status'] = "OFF"
                unisrt.updateRuntime([measurement], models.measurement, True)
                unisrt.uploadRuntime('measurements')
                
                hops = unisrt.poke_remote(unisrt.metadata['existing']['.'.join([measurement['selfRef'], TRACEROUTE])].id)
                return hops[0]['value']
        
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

    
    
    hops = psapi(ends[0].name, ends[1].name)
    
    if not hops:
        if type(ends[0]) is models.service:
            hops = run_traceroute(ends[0], ends[1])    
        elif type(ends[0]) is models.node:
            hops = vtraceroute(ends[0], ends[1])
        else:
            print "ERROR: only service or node is acceptable"
            return None
    
    if obj_layer == 'l3':
        return filter(lambda x: type(x) is obj_class, hops)

    # from here to the end, attempt to expend l3 to multi-layer
    multi_hops = hops
    ip_resolver = buildIPresolver()

    load = []
    temp = deepcopy(multi_hops)
    for hop in temp:
        if hop in ip_resolver:
            incr(hop)
            multi_hops[multi_hops.index(hop)] = ip_resolver[hop] # at the IP-L2 edge, we only map the vertex to a node(switch/router)
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
            multi_hops[multi_hops.index(ip_resolver[hop]) - 1 : multi_hops.index(ip_resolver[hop]) + 1] = resourcesL2[:-1]
            load = [load[1]]
                    
    # update counters after the entire path got identified, to avoid double counting on an adjacent switch
    map(incr, list(set(multi_hops) - set(temp)))

    return multi_hops
    
def getGENIResourceLists(unisrt, pairs):
    '''
    in GENI, L3 resources are hidden by either stitching VLANs or local VMs. It requires a different way to identify the path
    After users reserve their slice, the (stitching) manifest can be uploaded to UNIS paths objects for this function to query
    '''        
    paths = {}
    for pair in pairs:
        key = (pair[0], pair[1])
        hops = map(lambda x: x, unisrt.paths['existing'][pair[0] + '%' + pair[1]].hops)
        
        # dst_ip_set = map(lambda x: x.address, unisrt.services['existing'][pair['to']].node.ipports.values())
        # candi0 = unisrt.ports['existing'][unisrt.unis_url + "/ports/" + hops[0].replace('+', '_')].ip
        # candi1 = unisrt.ports['existing'][unisrt.unis_url + "/ports/" + hops[-1].replace('+', '_')].ip
        # dst_ip = filter(lambda x: x in dst_ip_set, [candi0, candi1])
        # assert len(dst_ip) == 1
        # dst_ip = dst_ip[0]
        # paths[(pair['from'], dst_ip)] = hops
        
        hops.insert(0, pair[0])
        hops.append(pair[1])
        paths[key] = hops
        
    return paths