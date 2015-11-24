'''
Created on Sep 28, 2013

@author: mzhang

I have a feeling that some of these utility functions should be formalized
to "system calls" of the network
'''
import random, string
import json, uuid
import settings
#from pytrie import StringTrie as trie

logger = settings.get_logger('unisrt')

def rand_name(length):
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(length))
            
def ToBin(ipv4):
    '''
    convert an ipv4 addr or subnet to binary
    '''
    splited = ipv4.split('/', 2)
    binary = ''.join([bin(int(x) + 256)[3:] for x in splited[0].split('.')])
    if len(splited) == 1:
        return binary
    elif len(splited) == 2:
        return binary[:int(splited[1])]
        
def get_file_config(filepath):
    try:
        with open(filepath) as f:
            conf = f.read()
            return json.loads(conf)
    except IOError as e:
        logger.exc('get_file_config', e)
        logger.error('get_file_config',
                     msg="Could not open config file... exiting")
        exit(1)
    except ValueError as e:
        logger.exc('get_file_config', e)
        logger.error('get_file_config',
                     msg="Config file is not valid json... exiting")
        exit(1)

def add_defaults(data, schema):
    # assume data is valid with schema
    if not "properties" in schema:
        return
    for key, inner_schema in schema["properties"].items():
        if not key in data:
            if "default" in inner_schema:
                data[key] = inner_schema["default"]
        elif inner_schema["type"] == "object":
            add_defaults(data[key], inner_schema)
            
def merge_dicts(base, overriding):
    '''
    Recursively merge 'overriding' into base (both nested
    dictionaries), preferring values from 'overriding' when there are
    colliding keys
    '''
    for k,v in overriding.iteritems():
        if isinstance(v, dict):
            merge_dicts(base.setdefault(k, {}), v)
        else:
            base[k] = v

def build_measurement(unisrt, service):
    '''
    form a bare bone measurement data structure
    '''
    measurement = {"configuration": {}}
    muuid = uuid.uuid1().hex
    measurement['$schema'] = "http://unis.crest.iu.edu/schema/20151104/measurement#"
    measurement['service'] = service
    measurement['selfRef'] = unisrt.unis_url + "/measurements/" + muuid
    measurement['id'] = muuid
    measurement['configuration']['status'] = "ON"
    measurement['configuration']['ms_url'] = unisrt.ms_url
    return measurement

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
    resolve(["108.59.26.9/31", "64.57.28.5/31"])