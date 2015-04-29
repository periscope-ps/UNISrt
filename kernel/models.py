'''
Created on Feb 9, 2014

@author: mzhang
'''
from libnre import utils
import re

class NetworkResource(object):
    '''
    Super class of UNIS models
    '''
    def __init__(self, data, unisrt, localnew):
        self.localnew = localnew
        self.unisrt = unisrt
        self.usecounter = 0
        # "data" holds a dict duplicating object attribute values
        # I keep this duplicated data because it was the input to
        # construct an object, and is also needed as an output to
        # populate json form uploading to UNIS
        # It can be removed, but you have to convert data back and
        # forward again
        self.data = data
        
    def prep_schema(self):
        '''
        subclasses should return dict presentation follow their UNIS JSON schema
        '''
        return {}     

class node(NetworkResource):
    '''
    a node in a network
    '''
    def __init__(self, data, unisrt, localnew, domain=None):
        super(node, self).__init__(data, unisrt, localnew)
        if 'domain' in data:
            self.domain = data['domain']
        else:
            self.domain = domain
        try:
            self.id = data['id']
            self.name = data['name']
#            self.name = re.match('(?P<name>[^.]+)', data['id'], re.M|re.I).group('name')
        except KeyError:
            try:
                self.id = data['id']
                self.name = data['id']
            except KeyError, e:
                print "node %s hasn't been found in rt" % str(e)
                return
        
        # currently, node-port model from router config is still differ from UNIS schema
        # this build step distinguish them according to localnew flag
        if 'ports' in data and localnew:
            for k, v in data['ports'].iteritems():
                v['name'] = k
                if not hasattr(self, 'ports'): self.ports = {}
                self.ports[v['name']] = port(v, unisrt, localnew, self)
        if 'ports' in data and not localnew:
            for v in data['ports']:
                #self.ports.append(v['href'])
                value = unisrt._unis.get(v['href'])
                if 'address' in value:
                    if not hasattr(self, 'ipports'): self.ipports = {}
                    self.ipports[value['selfRef']] = ipport(value, unisrt, localnew, self)
                else:
                    if not hasattr(self, 'ports'): self.ports = {}
                    self.ports[value['selfRef']] = port(value, unisrt, localnew, self)
                
        if 'services' in data:
            for k, v in data['services'].iteritems():
                v['name'] = k
                if not hasattr(self, 'services'): self.services = {}
                self.services[v['name']] = service(v, unisrt, localnew, self)
                
        if 'selfRef' in data:
            self.selfRef = data['selfRef']
        else:
            self.selfRef = self.unisrt.unis_url + '/nodes/' + self.id
        
        unisrt.nodes[self.localnew and 'new' or 'existing'][self.selfRef] = self
        
    def add_attr(self, attr):
        attr_nm = attr.__class__.__name__ + 's'
        if hasattr(self, attr_nm):
            getattr(self, attr_nm)[attr.id] = attr
        else:
            setattr(self, attr_nm, {attr.id: attr})
        
    def prep_schema(self):
        ret = {}
        ret['id'] = self.id
        ret['name'] = self.name
        ret['selfRef'] = self.selfRef
        ports = []
        for port in getattr(self, 'ports', {}).values():
            ports.append({'href': self.unisrt.unis_url + '/ports/' + self.id + '_port_' + port.name, 'rel': 'full', 'name': port.name})
        ret['ports'] = ports
        
        return ret
        
class port(NetworkResource):
    '''
    a layer 2 port
    '''
    def __init__(self, data, unisrt, localnew, node=None, capacity=1e3, queue=None):
        super(port, self).__init__(data, unisrt, localnew)
        self.node = node
        if 'name' in data:
            self.name = data['name']
        if 'id' in data:
            self.id = data['id']
        else:
            self.id = self.node.id + '_port_' + self.name

#        if 'capacity' in data:
#            capacity = data['capacity']

        self.capacity = capacity
        self.queue = {'flip': [], 'flop': []}
        # better than leaving it to node, still need a better solution
        if 'ip' in data:
            self.ip = data['ip']
        elif 'address' in data:
            self.ip = data['address']['address']
        '''
        vlan, protocol families etc. are complex, hang them for this
        moment. since UNISsp hasn't use ip's, I will try to fit it in
        without implementing the aforementioned complex elements...
        
        unit 506 {
            description "the only vlan on this interface";
            vlan-id 506;
            family inet {
                mtu 9000;
                address 10.0.7.0/24;
            }
        }
        family inet {
            mtu 9000;
            address 10.0.1.0/24;
        }
        '''
        if 'selfRef' not in data:
            data['selfRef'] = self.unisrt.unis_url + '/ports/' + self.id
        unisrt.ports[self.localnew and 'new' or 'existing'][data['selfRef']] = self
        
    def prep_schema(self):
        ret = {}
        ret['name'] = self.name
        ret['id'] = self.id
        
        return ret

class link(NetworkResource):
    def __init__(self, data, unisrt, localnew, end=None):
        super(link, self).__init__(data, unisrt, localnew)
        self.id = data['id']
        if 'name' in data:
            self.name = data['name']
        self.selfRef = data['selfRef']
        self.curr_load = 0

        if 'capacity' in data:
            self.capacity = data['capacity']
        else:
            self.capacity = None
    
        if 'endpoints' in data:
            try:
                self.endpoints = {data['endpoints'][0]['href']:data['endpoints'][1]['href']}
            except:
                self.endpoints = {data['endpoints']['source']['href']:data['endpoints']['sink']['href']}
                

        unisrt.links[self.localnew and 'new' or 'existing'][data['selfRef']] = self
        
class service(NetworkResource):
    '''
    a service running in a network
    '''
    def __init__(self, data, unisrt, localnew, node=None):
        super(service, self).__init__(data, unisrt, localnew)
        self.id = data['id']
        self.name = data['name']
        self.serviceType = data['serviceType']
        
        # ok, write ip to a node for now, for blipp use case
        if 'ip' in data:
            self.ip = data['ip']
            
        if 'selfRef' in data:
            self.selfRef = data['selfRef']
            
        if 'rules' in data:
            self.rules = data['rules']
        
        if node:
            self.node = node
        elif 'runningOn' in data:#see the comments in prep_schema()
            try:
                self.node = unisrt.nodes['existing'][data['runningOn']['href']]
                self.node.add_attr(self)
            except KeyError, e:
                print "node %s hasn't been found in rt" % str(e)
                
        try:
            # this exception should be caused by some BLiPP service nodes are not included in I2 topo
            unisrt.services[self.localnew and 'new' or 'existing'][self.selfRef] = self
        except AttributeError, e:
            print "no attribute found"
        
    def prep_schema(self):
        ret = {}
        ret['status'] = "ON"
        ret['id'] = self.id
        ret['name'] = self.name
        ret['serviceType'] = self.serviceType
        ret['runningOn'] = {'href': self.node.selfRef, 'rel': "full"}
        if hasattr(self, 'rules'): ret['rules'] = self.rules
        if hasattr(self, 'ip'): ret['ip'] = self.ip
        
        return ret
        
class ipport(NetworkResource):
    '''
    a layer 3 addressable port
    '''
    def __init__(self, data, unisrt, localnew, node=None):
        super(ipport, self).__init__(data, unisrt, localnew)
        self.version = data['address']['type']
        self.address = data['address']['address']

        if 'node' in data:
            self.node = data['node']
            self.port = data['relations']['over'][0]['href']
        elif node:
            self.node = node
            self.port = data['relations']['over'][0]['href']
            
        unisrt.ipports[self.localnew and 'new' or 'existing'][self.address] = self
        
    def prep_schema(self):
        ret = {}
        ret['name'] = self.data['name']
        ret['ip'] = self.data['ip']
        
        return ret
        
class measurement(NetworkResource):
    '''
    a network measurement event
    '''
    def __init__(self, data, unisrt, localnew):
        super(measurement, self).__init__(data, unisrt, localnew)
        if 'ts' in data: self.ts = data['ts']
        self.probe = data['configuration']
        self.resources = self.probe.get('resources', None)
        self.selfRef = data['selfRef']
        self.eventTypes = data['eventTypes']
        self.scheduled_times = data.get('scheduled_times', None)
        self.services = data.get('services', None)
        self.measurement_params = data['configuration']['schedule_params']
        self.every = data['configuration']['schedule_params']['every']
        self.num_tests = data['configuration']['schedule_params'].get('num_tests', 'inf')
        src = data['configuration']['src']
        dst = data['configuration']['dst']
        
            
        #unisrt.measurements[self.localnew and 'new' or 'existing'][data['id']] = self
        unisrt.measurements[self.localnew and 'new' or 'existing']['%'.join([src, dst])] = self
        
    def prep_schema(self):
        return self.data
    
class metadata(NetworkResource):
    '''
    metadata can refer to information like measurement data etc.
    '''
    def __init__(self, data, unisrt, localnew):
        super(metadata, self).__init__(data, unisrt, localnew)
        self.measurement = data['parameters']['measurement']['href']
        self.eventType = data['eventType']
        self.id = data['id']
        
        unisrt.metadata[self.localnew and 'new' or 'existing']['%'.join([self.measurement, self.eventType])] = self
        
    def prep_schema(self):
        return self.data
    
class path(NetworkResource):
    '''
    path objects tell the hops between two ends
    '''
    def __init__(self, data, unisrt, localnew):
        super(path, self).__init__(data, unisrt, localnew)
        
        # should convert each hop to the corresponding object
        str_hops = map(lambda x: x['href'], data['hops'])
        self.hops = map(lambda x: unisrt.links['existing'][x], str_hops)
        
        unisrt.paths[self.localnew and 'new' or 'existing']['%'.join([data['src'], data['dst']])] = self

class domain(NetworkResource):
    '''
    a domain in a network
    '''
    def __init__(self, data, unisrt, localnew):
        super(domain, self).__init__(data, unisrt, localnew)
        try:
            self.name = data['id']
        except KeyError:
            return
        if self.name in self.unisrt.conf['domains']:
            #print "FOUND THE DOM"
            self.selfRef = data['selfRef']
            self.nodes = []
            self.links = []
            if 'nodes' in data:
                for v in data['nodes']:
                    if 'href' in v:
                        self.nodes.append(node(unisrt._unis.get(v['href']), unisrt, localnew))
                    else:
                        self.nodes.append(node(v, unisrt, localnew, self))

            if 'links' in data:
                for v in data['links']:
                    self.links.append(link(unisrt._unis.get(v['href']), unisrt, localnew))
            
            unisrt.domains[self.localnew and 'new' or 'existing'][data['id']] = self

    def prep_schema(self):
        ret = {}
        ret['id'] = self.data['id']
        ret['selfRef'] = self.data['selfRef']
        
        return ret
