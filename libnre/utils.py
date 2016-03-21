'''
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

def get_most_recent(resources):
    '''
    same id different records -- shouldn't be needed, as nre only keeps the latest
    '''
    res_dict = {}
    for res in resources:
        if res.id in res_dict:
            if res.data['ts'] > res_dict[res.id].data['ts']:
                res_dict[res.id] = res
        else:
            res_dict[res.id] = res

    res = []
    for key in res_dict:
        res.append(res_dict[key])
    return res

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

def build_metadata(unisrt, meas_obj, eventType, isforecasted=False):
    '''
    form a bare bone metadata (maybe a bare bone structure is enough?)
    '''
    metadata = {
        "$schema": 'http://unis.crest.iu.edu/schema/20151104/metadata#',
        "id": uuid.uuid1().hex,
        "subject": {
            "href": meas_obj.selfRef,
            "rel": "full"
        },
        "eventType": eventType,
        "parameters": {
            "datumSchema": settings.SCHEMAS["datum"],
            "measurement": {
                "href": meas_obj.selfRef,
                "rel": "full"
            }
        },
        "forecasted": isforecasted
    }
    return metadata

def get_eventtype_related(eventtype, subject):
    to_eventtype_l = {
        'ping': ["ps:tools:blipp:linux:net:ping:ttl", "ps:tools:blipp:linux:net:ping:rtt"],
        'iperf': ["ps:tools:blipp:linux:net:iperf:bandwidth"],
        'traceroute': ["ps:tools:blipp:linux:net:traceroute:hopip"]
    }
    
    to_eventtype_d = {
        'ping': {
            "ttl": "ps:tools:blipp:linux:net:ping:ttl",
            "rtt": "ps:tools:blipp:linux:net:ping:rtt"
        },
        'iperf': {
            "bandwidth": "ps:tools:blipp:linux:net:iperf:bandwidth"
        },
        'traceroute': {
            "hopip": "ps:tools:blipp:linux:net:traceroute:hopip"
        }
    }
    
    to_probe_module = {
        'ping': "cmd_line_probe",
        'iperf': "cmd_line_probe",
        'traceroute': "traceroute_probe"
    }
    
    to_command = {
        'ping': "ping -c 1 %s",
        'iperf': "iperf -c %s",
        'traceroute': "traceroute %s"
    }
    
    to_regex = {
        'ping': "ttl=(?P<ttl>\\d+).*time=(?P<rtt>\\d+\\.*\\d*) ",
        'iperf': "(?P<bandwidth>\\d*\\.?\\d* [M,G]bits\\/sec)",
        'traceroute': "^\\s*\\d+.*(?P<hopip>\\(.*\\))"
    }
    
    if subject == 'eventtype_l':
        return to_eventtype_l[eventtype]
    elif subject == 'eventtype_d':
        return to_eventtype_d[eventtype]
    elif subject == 'probe_module':
        return to_probe_module[eventtype]
    elif subject == 'command':
        return to_command[eventtype]
    elif subject == 'regex':
        return to_regex[eventtype]
    else:
        return None