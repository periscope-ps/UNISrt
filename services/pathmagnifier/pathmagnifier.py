import sys
import time
import threading
from copy import deepcopy

from kernel.models import ipport, link, path
from libnre.utils import *
from test.test_math import acc_check
from wx.lib.agw.cubecolourdialog import Distance
from matplotlib.backend_bases import LocationEvent

TRACEROUTE = "ps:tools:blipp:linux:net:traceroute:hopip"
logger = settings.get_logger('pathmagnifier')

class Pathmagnifier(object):
    '''
    '''
    def __init__(self, unisrt):
        '''
        '''
        self.unisrt = unisrt
        threading.Thread(name='traceroute_maintainer', target=self.update_paths, args=()).start()
        
    def update_paths(self):
        '''
        1. retrieve hopip measurement results
        2. map to/create interface objects
        3. construct links and paths
        '''
        def get_ipport(hop, rt, uc):
            '''
            input: a list of ip string at this hop (step)
            output: a list of L3 port object at this hop (step)
            '''
            if len(hop) > 1:
                return [get_ipport([ip_str], self.unisrt, meta_obj.currentclient) for ip_str in hop]
            
            hop = hop[0]
                
            if hop == '*':
                return None
            elif hop in self.unisrt.ipports['existing']:
                return self.unisrt.ipports['existing'][hop]
            else:
                # not been posted to unis yet, still in ['new'], no selfRef, L2 port node info etc.
                l3 = ipport({
                             'address': {
                                         'type': 'ipv4',
                                         'address': hop
                                         }
                             },
                            rt,
                            uc,
                            True)
                return l3
            
        def form_paths(a_to_z, z_to_a):
            '''
            input: hops of two directions
            output: replace hops via links if possible
            
            
            for i in a_to_z:
                if not type(i) is list:
                    print i.address
                else:
                    print '['
                    for j in i:
                        try:
                            print j.address
                        except AttributeError, e:
                            print '*'
                    print ']'
            print '--------------------'
            for i in reversed(z_to_a):
                if not type(i) is list:
                    print i.address
                else:
                    print '['
                    for j in i:
                        try:
                            print j.address
                        except AttributeError, e:
                            print '*'
                    print ']'
            '''
            
            def evaluate(p1, p2, v1, v2):
                '''
                evaluate how possible to IPs are on a same network, consider factors:
                1. ip distance
                2. sequence relative location
                3. divide evenly
                '''
                if type(v1) is list:
                    return max([evaluate(v1_, v2) for v1_ in v1])
                
                if type(v2) is list:
                    return max([evaluate(v1, v2_) for v2_ in v2])
                
                ret = 0
                # equation for ip distances, most weighted
                if v1.address == '*' or v2.address == '*':
                    # * means unknown ip similarity, neutral
                    ret += 0
                else:
                    pass
                
                # equation for sequence relative location -- p1 and p2 should be at similar portion along the path
                if abs(p1 - p2) == 0:
                    ret += 100000000
                
                # equation to encourage p1 and p2 to be closer to 50%
                if p1 == 0.5 and p2 == 0.5:
                    ret += 100000000
                
                return ret

            def best_cut_match(s1, s2):
                '''
                recursively binary-divide two ip sequences at the best match ip
                output: [index of the s2 sequence that matches 1st s1 element the best, ... 2nd element the best, 3rd, ...]
                '''
                maximum = -sys.maxint
                threshold = 100000000
                the_i1 = None
                the_i2 = None
                for i1, v1 in enumerate(s1):
                    for i2, v2 in enumerate(s2):
                        score = evaluate(float(i1)/float(len(s1)), float(i2)/float(len(s2)), v1, v2)
                        if score < threshold:
                            # too low to be considered as a match
                            pass
                        elif score > maximum:
                            maximum = score
                            the_i1 = i1
                            the_i2 = i2
                
                if not the_i1 and not the_i2:
                    # these two sequences (this segment along the path) cannot be matched up
                    return [None for _ in s1]
                
                head = best_cut_match(s1[0 : the_i1], s2[0 : the_i2])
                tail = best_cut_match(s1[the_i1 + 1 : -1], s2[the_i2 + 1 : -1])
                
                head.append(the_i2)
                head.extend(tail)
                return head
            
            match_result = best_cut_match(a_to_z, list(reversed(z_to_a)))
            
            # now use the match results to populate link objects and replace ipport objects wherever possible
            for i, v in enumerate(match_result):
                if v:
                    l = (a_to_z[i], z_to_a[len(z_to_a) - v - 1])
                    a_to_z[i] = l
                    z_to_a[len(z_to_a) - v - 1] = l
                else:
                    pass

            return
        
        while True:
            meta_traceroute = filter(lambda x: x.eventType == TRACEROUTE, self.unisrt.metadata['existing'].values())
            paths_raw = {}
            
            # loop 1: for each path, get all IPs extracted, mapped and their corresponding objects created
            for meta_obj in meta_traceroute:
                # obtain IPs from each traceroute
                hops = meta_obj.currentclient.get('/data/' + str(meta_obj.id))[0] # TODO: should query the newest one record
                
                # IP string to interface objects
                hop_objs = [get_ipport(hop, self.unisrt, meta_obj.currentclient) for hop in hops['value']]
                
                # construct raw paths
                paths_raw[(meta_obj.measurement.src, meta_obj.measurement.dst)] = {
                                                                                   'hop_objs': hop_objs,
                                                                                   'uc': meta_obj.currentclient
                                                                                   }
                
            # loop 2: for each path, synthesize links and paths from bi-directional traffics
            temp = paths_raw.keys()
            for ends in paths_raw.keys():
                if (ends[1], ends[0]) in temp:
                    # here is an assumption: always run traceroute between pairs of end hosts
                    # use the sister traceroute result to match the ends of links
                    paths = form_paths(paths_raw[(ends[0], ends[1])]['hop_objs'], paths_raw[(ends[1], ends[0])]['hop_objs'])

                    raw_path_dict = {
                                     "schema": "/path",
                                     "hops": [
                                              ]
                                     }
                    
                    map(lambda x: path(x), paths)
                    
                    temp.remove((ends[0], ends[1]))
                    temp.remove((ends[1], ends[0]))
                    
            # update unis
            self.unisrt.pushRuntime(['ipport', 'link', 'path'])
            
            time.sleep(600)

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
    
    def vtraceroute(src, dst):
        '''
        use stored forwarding tables to figure the routes
        '''
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

    
    # 1. query archive
    hops = psapi(ends[0].name, ends[-1].name)
    
    if not hops:
        if type(ends[0]) is models.service:
            # 2. assign traceroute tasks if BLiPP service instances
            hops = run_traceroute(ends[0], ends[-1])    
        elif type(ends[0]) is models.node:
            # 3. consult soft forwarding tables if just nodes
            hops = vtraceroute(ends[0], ends[-1])
        else:
            print "ERROR: run out of ideas resolving the hops"
            return None
    
    if obj_layer == 'l3':
        return filter(lambda x: type(x) is obj_class, hops)

    # from here to the end, attempt to expend l3 to multi-layer
    multi_hops = hops
    ip_resolver = unisrt.ipresolver # TODO: not done yet, but nre should have an ipport dict for ip-to-L2port query

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

def run(unisrt, kwargs):
    pathmagnifier = Pathmagnifier(unisrt)
    setattr(unisrt, 'pathmagnifier', pathmagnifier)
    
if __name__ == '__main__':
    import kernel.unisrt
    unisrt = kernel.unisrt.UNISrt()
    pathmagnifier = Pathmagnifier(unisrt, 'args')
    setattr(unisrt, 'pathmagnifier', pathmagnifier)