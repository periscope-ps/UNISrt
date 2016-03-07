import re
from netaddr import IPNetwork, IPAddress

from kernel.models import measurement
from libnre.utils import *

UNIS_SERVICE_SCHEMA = 'http://unis.crest.iu.edu/schema/20140214/service#'

def work(nre):
    # the slice that we are interested in
    p = re.compile("urn:.*\+idms-ig-ill\+.*")
    slice_nodes = filter(lambda x: p.match(x.urn), nre.nodes['existing'].values())
#    slice_nodes = nre.nodes['existing'].values()
    
    # same slice name may contain historical objects with same names, try to use the last set of objects...
    from cluster import HierarchicalClustering
    data = {n.data['ts']: n for n in slice_nodes}
    hc = HierarchicalClustering(data.keys(), lambda x,y: abs(x-y))
    clsts = hc.getlevel(100000000)
    big_value = 0
    big_index = 0
    for i, cl in enumerate(clsts):
        if cl[0] > big_value:
            big_value = cl[0]
            big_index = i
    tss = clsts[big_index]
    nodes = filter(lambda n: n.data['ts'] in tss, slice_nodes)
    
    # return immediately after this block as we now only want to do management ports
    disgard = re.compile("^127\.|10\.")
    for node in nodes:
        for local_intf in node.ports.values():
            if 'geni' in local_intf.data['properties'] or\
                disgard.match(local_intf.data['properties']['ipv4']['address']):
                continue
            src = local_intf.data['properties']['ipv4']['address']
            for another_node in nodes:
                if another_node.id == node.id:
                    continue
                for remote_intf in another_node.ports.values():
                    if 'geni' in remote_intf.data['properties'] or\
                        disgard.match(remote_intf.data['properties']['ipv4']['address']):
                        continue
                    
                    dst = remote_intf.data['properties']['ipv4']['address']
                    blipp = node.services['ps:tools:blipp'].selfRef
                    
                    # PingIperfTraceroute
                    meas_p = build_measurement(nre, blipp)
                    meas_p.update({
                        "eventTypes": [
                            "ps:tools:blipp:linux:net:ping:ttl",
                            "ps:tools:blipp:linux:net:ping:rtt"
                        ],
                        "configuration": {
                            "ms_url": nre.ms_url,
                            "collection_schedule":"builtins.simple",
                            "schedule_params": {"every": 120},
                            "reporting_params": 3,
                            "reporting tolerance": 10,
                            "collection_size":100000,
                            "collection_ttl":1500000,
                            "unis_url": nre.unis_url,
                            "use_ssl": False,
                            "name": "ping-" + dst,
                            "src": src,
                            "dst": dst,
                            "probe_module": "cmd_line_probe",
                            "command": "ping -c 1 " + dst,
                            "regex": "ttl=(?P<ttl>\\d+).*time=(?P<rtt>\\d+\\.*\\d*) ",
                            "eventTypes": {
                                "ttl": "ps:tools:blipp:linux:net:ping:ttl",
                                "rtt": "ps:tools:blipp:linux:net:ping:rtt"
                            }
                        }
                    })
                    measurement(meas_p, nre, True)

                    meas_i = build_measurement(nre, blipp)
                    meas_i.update({
                        "eventTypes": [
                            "ps:tools:blipp:linux:net:iperf:bandwidth"
                        ],
                        "configuration": {
                            "ms_url": nre.ms_url,
                            "collection_schedule":"builtins.simple",
                            "schedule_params": {"every": 120},
                            "reporting_params": 3,
                            "reporting tolerance": 10,
                            "collection_size":100000,
                            "collection_ttl":1500000,
                            "unis_url": nre.unis_url,
                            "use_ssl": False,
                            "name": "iperf-" + dst,
                            "src": src,
                            "dst": dst,
                            "probe_module": "cmd_line_probe",
                            "command": "iperf -c " + dst,
                            "regex": "(?P<bandwidth>\\d*\\.?\\d*) Mbits\\/sec",
                            "eventTypes": {
                                "bandwidth": "ps:tools:blipp:linux:net:iperf:bandwidth"
                            }
                        }
                    })
                    measurement(meas_i, nre, True)
                    
                    meas_t = build_measurement(nre, blipp)
                    meas_t.update({
                        "eventTypes": [
                            "ps:tools:blipp:linux:net:traceroute:hopip"
                        ],
                        "configuration": {
                            "ms_url": nre.ms_url,
                            "collection_schedule":"builtins.simple",
                            "schedule_params": {"every": 120},
                            "reporting_params": 3,
                            "reporting tolerance": 10,
                            "collection_size":100000,
                            "collection_ttl":1500000,
                            "unis_url": nre.unis_url,
                            "use_ssl": False,
                            "name": "traceroute-" + dst,
                            "src": src,
                            "dst": dst,
                            "probe_module": "traceroute_probe",
                            "command": "traceroute " + dst,
                            "regex": "^\\s*\\d+.*(?P<hopip>\\(.*\\))",
                            "eventTypes": {
                                "hopip": "ps:tools:blipp:linux:net:traceroute:hopip"
                            }
                        }
                    })
                    measurement(meas_t, nre, True)
    nre.pushRuntime('measurements')
    return
    
    for node in nodes:
        for local_port in node.ports.values():
            if 'geni' not in local_port.data['properties']:
                continue
            ipv4 = local_port.data['properties']['geni']['ip']['address']
            cidr = local_port.data['properties']['geni']['ip']['netmask']
            slash = (cidr[:cidr.index('.0')].count('.') + 1) * 8
            subnet = IPNetwork(ipv4 + '/' + str(slash))
            
            for another_node in nodes:
                if another_node.id == node.id:
                    continue
                for remote_intf in another_node.ports.values():
                    if 'geni' not in remote_intf.data['properties']:
                        continue
                    
                    if remote_intf.data['properties']['geni']['ip']['address'] in subnet:
                        dst = remote_intf.data['properties']['geni']['ip']['address']
                        blipp = node.services['ps:tools:blipp'].selfRef
                        
                        
                        
                        
                        
                        
                        
                        #the index of measurement objects. does it need semantic meaning?
                        #semantic meaning is useful to reuse a same object, or just create newer ones for blipp services
                        #build_measurement---uuid---nre.push---faultlocator query
                        
                        
                        
                        
                        
                        
                        
                        meas_t = build_measurement(nre, blipp)
                        meas_t.update({
                            "eventTypes": [
                                "ps:tools:blipp:linux:net:traceroute:hopip"
                            ],
                            "configuration": {
                                "ms_url": nre.ms_url,
                                "collection_schedule":"builtins.simple",
                                "schedule_params": {"every": 120},
                                "reporting_params": 3,
                                "reporting tolerance": 10,
                                "collection_size":100000,
                                "collection_ttl":1500000,
                                "unis_url": nre.unis_url,
                                "use_ssl": False,
                                "name": "traceroute-" + dst,
                                "src": ipv4,
                                "dst": dst,
                                "probe_module": "traceroute_probe",
                                "command": "traceroute " + dst,
                                "regex": "^\\s*\\d+.*(?P<hopip>\\(.*\\))",
                                "eventTypes": {
                                    "hopip": "ps:tools:blipp:linux:net:traceroute:hopip"
                                }
                            }
                        })
                        measurement(meas_t, nre, True)
                        
                        meas_i = build_measurement(nre, blipp)
                        meas_i.update({
                            "eventTypes": [
                                "ps:tools:blipp:linux:net:iperf:bandwidth"
                            ],
                            "configuration": {
                                "ms_url": nre.ms_url,
                                "collection_schedule":"builtins.simple",
                                "schedule_params": {"every": 120},
                                "reporting_params": 3,
                                "reporting tolerance": 10,
                                "collection_size":100000,
                                "collection_ttl":1500000,
                                "unis_url": nre.unis_url,
                                "use_ssl": False,
                                "name": "iperf-" + dst,
                                "src": ipv4,
                                "dst": dst,
                                "probe_module": "cmd_line_probe",
                                "command": "iperf -c " + dst,
                                "regex": "(?P<bandwidth>\\d*\\.?\\d*) Mbits\\/sec",
                                "eventTypes": {
                                    "bandwidth": "ps:tools:blipp:linux:net:iperf:bandwidth"
                                }
                            }
                        })
                        measurement(meas_i, nre, True)
                        

    nre.pushRuntime('measurements')