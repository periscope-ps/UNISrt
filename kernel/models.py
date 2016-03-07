from bitarray import bitarray
from abc import ABCMeta, abstractmethod
from mercurial.util import dst
from Cython.Compiler.MemoryView import src_conforms_to_dst

class Crossing(object):
    '''
    switches, routers should implement this interface
    '''
    __metaclass__ = ABCMeta

    @abstractmethod
    def nextStep(self, args):
        pass    

class Node(object):
    '''
    a special vertex (as it requires attach points) in the graph abstraction
    '''
    def setAttachPoint(self, attachpoint):
        '''
        in order to exist in a graph, a node has to have at least one attach point
        to glue it to other objects in the graph. attach points on one node can
        belong to different layers
        '''
        assert isinstance(attachpoint, AttachPoint)
        if hasattr(self, 'attachpoints'):
            if attachpoint.layer in self.attachpoints:
                self.attachpoints[attachpoint.layer].append(attachpoint)
            else:
                self.attachpoints[attachpoint.layer] = [attachpoint]
        else:
            setattr(self, 'attachpoints', {attachpoint.layer: [attachpoint]})
        
class Connection(object):
    '''
    a special edge (as it requires attach points) in the graph abstraction
    '''
    def setup(self, src, dst, layer):
        assert isinstance(src, AttachPoint)
        assert isinstance(dst, AttachPoint)
        assert layer == src.layer and layer == dst.layer
        setattr(self, 'src', src)
        setattr(self, 'dst', dst)
        setattr(self, 'layer', layer)
        
class AttachPoint(object):
    '''
    base class of the concept of "port"
    '''
    def setLayer(self, layer):
        setattr(self, 'layer', layer)
        
    def setNode(self, node):
        setattr(self, 'node', node)
        
    def setAssociatedPoint(self, attachpoint):
        if hasattr(self, 'associated'):
            if attachpoint.layer in self.associated:
                self.associated[attachpoint.layer].append(attachpoint)
            else:
                self.associated[attachpoint.layer] = [attachpoint]
        else:
            setattr(self, 'associated', {attachpoint.layer: [attachpoint]})

class NetworkResource(object):
    '''
    Super class of UNIS models
    '''
    __metaclass__ = ABCMeta

    @abstractmethod
    def updateReference(self, args):
        '''
        A network resource object may be re-created. If some other objects in the hierarchy
        already referred to it, a reference update is needed.
        '''
        pass
    
    def __init__(self, data, unisrt, localnew):
        self.unisrt = unisrt
        self.localnew = localnew
        try:
            self.id = data['id']
        except KeyError:
            pass
        # "data" holds a dict duplicating object attribute values
        # I keep this duplicated data because it was the input to
        # construct an object, and is also needed as an output to
        # populate json form uploading to UNIS
        # It can be removed, but you have to convert data back and
        # forward again
        self.data = data
        
    def renew_local(self, res_inst_key):
        res_name = self.__class__.__name__ + 's'
        res_obj = getattr(self.unisrt, res_name)
        
        if not isinstance(res_obj['existing'][res_inst_key], list):
            res_obj['new'][res_inst_key] = self
            del res_obj['existing'][res_inst_key]
        else:
            if res_inst_key not in res_obj['new']:
                res_obj['new'][res_inst_key] = list()
            res_obj['new'][res_inst_key].append(self)
            res_obj['existing'][res_inst_key].remove(self)
        
    def prep_schema(self):
        '''
        subclasses should return dict presentation follow their UNIS JSON schema
        '''
        return {}
    
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
            self.selfRef = data['selfRef']
            self.nodes = []
            self.links = []
            if 'nodes' in data:
                for v in data['nodes']:
                    if 'href' in v:
                        self.nodes.append(node(unisrt._unis.get(v['href']), unisrt, localnew))
                    else:
                        self.nodes.append(node(v, unisrt, localnew, self))

            # TODO: instead of being built separately, link objects should be built with nodes, just like port objects
            if 'links' in data:
                for v in data['links']:
                    self.links.append(link(unisrt._unis.get(v['href']), unisrt, localnew))
            
            unisrt.domains[self.localnew and 'new' or 'existing'][data['id']] = self
            
    def updateReference(self):
        pass

    def prep_schema(self):
        ret = {}
        ret['id'] = self.data['id']
        ret['selfRef'] = self.data['selfRef']
        
        return ret

class node(NetworkResource, Node):
    '''
    a node in a network
    '''
    def __init__(self, data, unisrt, localnew, domain='Unknown'):
        super(node, self).__init__(data, unisrt, localnew)
        if 'domain' in data:
            self.domain = data['domain']
        else:
            self.domain = domain
        if 'deviceType' in data:
            self.deviceType = data['deviceType']
        else:
            self.deviceType = 'Generic'     
        self.urn = data['urn']
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
        # this building step distinguish them according to localnew flag
        if 'ports' in data and localnew:
            for k, v in data['ports'].iteritems():
                v['name'] = k
                if not hasattr(self, 'ports'): self.ports = {}
                self.ports[v['name']] = port(v, unisrt, localnew, self)
        if 'ports' in data and not localnew:
            for v in data['ports']:
                value = unisrt._unis.get(v['href'])
                
                # TODO: all kinds of ports should be indexed by selfRef in their layers in the node AP property
                p = port(value, unisrt, localnew, self)
                p.setLayer(2)
                p.setNode(self)
                self.setAttachPoint(p)
                
                if not hasattr(self, 'ports'): self.ports = {}
                self.ports[value['selfRef']] = p
                
                if 'address' in value:
                    ipp = ipport(value, unisrt, localnew, self)
                    ipp.setLayer(3)
                    ipp.setNode(self)
                    self.setAttachPoint(ipp)
                    if not hasattr(self, 'ipports'): self.ipports = {}
                    self.ipports[value['selfRef']] = ipp
                    
                    ipp.setAssociatedPoint(p)
                    p.setAssociatedPoint(ipp)
                
        if 'services' in data:
            for v in data['services']:
                value = unisrt._unis.get(v['href'])
                if not hasattr(self, 'services'): self.services = {}
                self.services[value['serviceType']] = service(value, unisrt, localnew, self)
                
        if 'selfRef' in data:
            self.selfRef = data['selfRef']
        else:
            self.selfRef = self.unisrt.unis_url + '/nodes/' + self.id
        
        self.updateReference()
        unisrt.nodes[self.localnew and 'new' or 'existing'][self.selfRef] = self
        
    def updateReference(self):
        '''
        use urn to ensure theoretical unique matching
        '''
        
        
        
        # TODO: why not use dict and index nodes inside domains?
        for domain in self.unisrt.domains['existing'].values():
            for node in domain.nodes:
                if node.urn == self.urn:
                    domain.nodes.remove(node)
                    domain.nodes.append(self)
                    return
                
                
                
        
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
        
class port(NetworkResource, AttachPoint):
    '''
    a layer 2 port
    '''
    def __init__(self, data, unisrt, localnew, node=None, capacity=1e3, queue=None):
        super(port, self).__init__(data, unisrt, localnew)
        self.node = node
        self.capacity = capacity
        self.queue = {'flip': [], 'flop': []}
        
        self.urn = data['urn']
        if 'name' in data:
            self.name = data['name']
        if 'id' in data:
            self.id = data['id']
        else:
            self.id = self.node.id + '_port_' + self.name
        
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
        
        if data['selfRef'] not in unisrt.ports[self.localnew and 'new' or 'existing']:
            unisrt.ports[self.localnew and 'new' or 'existing'][data['selfRef']] = self
            
    def updateReference(self):
        pass
        
    def prep_schema(self):
        ret = {}
        ret['name'] = self.name
        ret['id'] = self.id
        
        return ret
    
class ipport(NetworkResource, AttachPoint):
    '''
    a layer 3 addressable port
    '''
    def __init__(self, data, unisrt, localnew, node=None):
        super(ipport, self).__init__(data, unisrt, localnew)
        if node:
            self.node = node
            self.port = data['selfRef']
        elif 'node' in data:
            self.node = data['node']
            self.port = data['selfRef']
            
        self.version = data['address']['type']
        self.address = data['address']['address']
            
        unisrt.ipports[self.localnew and 'new' or 'existing'][self.address] = self
        
    def updateReference(self):
        pass
        
    def prep_schema(self):
        ret = {}
        ret['name'] = self.data['name']
        ret['ip'] = self.data['ip']
        
        return ret

class link(NetworkResource, Connection):
    def __init__(self, data, unisrt, localnew):
        super(link, self).__init__(data, unisrt, localnew)
        self.urn = data['urn']
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
                
        # conditional. because some topology may be incomplete and have dangling links
        if self.endpoints.keys()[0] in self.unisrt.ports['existing'] and self.endpoints.values()[0] in self.unisrt.ports['existing']:
            ap0 = self.unisrt.ports['existing'][self.endpoints.keys()[0]]
            ap1 = self.unisrt.ports['existing'][self.endpoints.values()[0]]
            self.setup(ap0, ap1, 2)
                
        # this attribute marks the available time slots (in seconds) of the link object
        # right now, scheduler only consider contentions on link objects within a calendar year
        self.booking = bitarray(3600 * 24 * 365)

        unisrt.links[self.localnew and 'new' or 'existing'][self.endpoints.keys()[0]] = self

    def updateReference(self):
        pass
    
class service(NetworkResource):
    '''
    a service running in a network
    '''
    def __init__(self, data, unisrt, localnew, node=None):
        super(service, self).__init__(data, unisrt, localnew)
        self.id = data['id']
        #self.name = data['name']
        self.serviceType = data['serviceType']
        
        # ok, write ip to a node for now, for blipp use case
        if 'ip' in data:
            self.ip = data['ip']
            
        if 'selfRef' in data:
            self.selfRef = data['selfRef']
            
        if 'rules' in data:
            self.rules = data['rules']
            
        if 'name' in data:
            self.name = data['name']
        
        if node:
            self.node = node
        elif 'runningOn' in data:
            try:
                self.node = unisrt.nodes['existing'][data['runningOn']['href']]
            except KeyError, e:
                print "node %s hasn't been found in rt" % str(e)
        if hasattr(self, 'node'):
            if hasattr(self.node, 'services'):
                self.node.services[data['serviceType']] = self
            else:
                self.node.services = {data['serviceType']: self}
                
        try:
            # this exception should be caused by some BLiPP service nodes are not included in I2 topo
            unisrt.services[self.localnew and 'new' or 'existing'][self.selfRef] = self
        except AttributeError, e:
            print "no attribute found"
            
    def updateReference(self):
        pass
        
    def prep_schema(self):
        ret = {}
        ret['status'] = "ON"
        ret['id'] = self.id
        #ret['name'] = self.name
        ret['serviceType'] = self.serviceType
        ret['runningOn'] = {'href': self.node.selfRef, 'rel': "full"}
        if hasattr(self, 'rules'): ret['rules'] = self.rules
        if hasattr(self, 'ip'): ret['ip'] = self.ip
        
        return ret
        
class measurement(NetworkResource):
    '''
    a network measurement event
    '''
    def __init__(self, data, unisrt, localnew):
        super(measurement, self).__init__(data, unisrt, localnew)
        if 'ts' in data: self.ts = data['ts']
        self.probe = data['configuration']
        self.service = unisrt.services['existing'][data['service']]
        self.selfRef = data.get('selfRef')
        self.eventTypes = data.get('eventTypes')
        
        self.resources = self.probe.get('resources', None)
        self.collection_schedule = data['configuration']['collection_schedule']
        self.scheduled_times = data.get('scheduled_times', None)
        self.measurement_params = data['configuration']['schedule_params']
        self.every = data['configuration']['schedule_params'].get('every', 3600)
        self.num_tests = data['configuration']['schedule_params'].get('num_tests', 'inf')
        
        self.command = data['configuration'].get('command', None)
        self.regex = data['configuration'].get('regex', None)
        
        if 'src' in data['configuration'] and 'dst' in data['configuration']:
            self.src = data['configuration']['src']
            self.dst = data['configuration']['dst']
        
        # need to consider the key some more: a service should be allowed to run multiple measurement of a same eventType
        unisrt.measurements[self.localnew and 'new' or 'existing']['%'.join([self.service.selfRef, '+'.join(self.eventTypes)])] = self
        #unisrt.measurements[self.localnew and 'new' or 'existing'][self.selfRef] = self
        
    def updateReference(self):
        pass
        
    def prep_schema(self):
        self.data['eventTypes'] = self.eventTypes
        if hasattr(self, 'scheduled_times') and self.scheduled_times:
            self.data['scheduled_times'] = self.scheduled_times
        self.data['configuration']['status'] = getattr(self, 'status', 'ON')
        self.data['configuration']['collection_schedule'] = getattr(self, 'collection_schedule')
        self.data['configuration']['src'] = self.src
        self.data['configuration']['dst'] = self.dst
        if hasattr(self, 'regex'):
            self.data['configuration']['regex'] = self.regex
        self.data['configuration']['command'] = getattr(self, 'command')
        
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
        
    def updateReference(self):
        pass
            
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
        self.hops = map(lambda x: unisrt.nodes['existing'][x], str_hops)
        self.status = data['status']
        
        if 'healthiness' in data:
            self.healthiness = data['healthiness']
        if 'performance' in data:
            self.performance = data['performance']
        
        if '%'.join([data['src'], data['dst']]) not in unisrt.paths[self.localnew and 'new' or 'existing']:
            unisrt.paths[self.localnew and 'new' or 'existing']['%'.join([data['src'], data['dst']])] = list()
        
        unisrt.paths[self.localnew and 'new' or 'existing']['%'.join([data['src'], data['dst']])].append(self)
        
    def updateReference(self):
        pass
    
    def prep_schema(self):
        self.data['status'] = self.status
        self.data['performance'] = self.performance
        self.data['healthiness'] = self.healthiness
        return self.data
