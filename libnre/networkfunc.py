'''
Created on Feb 12, 2015

@author: mzhang
'''
import utils
#from pytrie import StringTrie as trie

flipflop = {'flip': 'flop', 'flop': 'flip'}

def launchRuntime():
    '''
    running logics in a network e.g. 'services' should be presented as functions
    in this rt. Here, we start all the concurrent functions
    Note: only 'services' are launched as functions at this moment        
    '''
    def switch(nm, prts, ft, pace):
        name = nm
        ports = prts
        table = ft
        pacer = 0
        while True:
            rhythm = yield
            pacer = (pacer + 1) % pace
            if pacer:
                # if a slow pacer skips this step, flows get copied in the flipflop queue
                for port in filter(lambda x: x.queue[rhythm], ports):
                    port.queue[flipflop[rhythm]].extend(port.queue[rhythm])
                    del port.queue[rhythm][0 : len(port.queue[rhythm])]
                    
                continue
                
            requested_thrpt = {}
            requested_flows = {}
            for port in filter(lambda x: x.queue[rhythm], ports):
                flows = list(port.queue[rhythm])
                del port.queue[rhythm][0 : len(port.queue[rhythm])]
                for flow in flows:
                    result = table.longest_prefix_value(utils.ToBin(flow['dst']), None)
                            
                    requested_thrpt[(result['intf'], result['dst'])] = requested_thrpt.setdefault((result['intf'], result['dst']), 0) + flow['feedin']
                    requested_flows.setdefault((result['intf'], result['dst']), []).append(flow)
                            
            for k, v in requested_thrpt.items():
                # loop all ports on this switch by name, because the interface is a local
                # name on this switch, not a global reference
                target = None
                for port in ports:
                    if port.name == k[0]:
                        target = port
                        break
                    
                try:
                    if target.capacity < v:
                        target.curr_load = target.capacity
                        # distribute available bandwidth to related flows proportionally
                        self.ports['existing']['ip'][k[1]].queue[flipflop[rhythm]].extend(map(lambda x: {'src': x['src'],
                                               'dst': x['dst'],
                                               'start': x['start'],
                                               'end': x['end'],
                                               'feedin': target.capacity * x['feedin'] / v},
                                    requested_flows[k]))
                    else:
                        target.curr_load = v
                        self.ports['existing']['ip'][k[1]].queue[flipflop[rhythm]].extend(requested_flows[k])
                        
                except KeyError:
                    print 'switch ' + name + ' reaches its boarder at: ' + k[1]
                
    for k, v in self.services['existing'].items():
        if k.split('.')[-1] == 'forwarding':
            swt = switch(k, map(lambda x: self.ports['existing']['selfRef'][x], v.node.ports), \
                         trie(v.data['rules']), pace=1) # pace = 1 means evaluate at every step
            next(swt) # push the functions to the first yield statement
            self.switches[k] = swt
                
def launchRuntimeL2():
    '''
    running logics in a network e.g. 'services' should be presented as functions
    in this rt. Here, we start all the concurrent functions
    Note: only 'services' are launched as functions at this moment        
    '''
    def switch(nm, prts, ft, pace):
        name = nm
        ports = prts
        table = ft
        pacer = 0
        while True:
            rhythm = yield
            pacer = (pacer + 1) % pace
            if pacer:
                # if a slow pacer skips this step, flows get copied in the flipflop queue
                for port in filter(lambda x: x.queue[rhythm], ports):
                    port.queue[flipflop[rhythm]].extend(port.queue[rhythm])
                    del port.queue[rhythm][0 : len(port.queue[rhythm])]
                    
                continue
                
            requested_thrpt = {}
            requested_flows = {}
            for port in filter(lambda x: x.queue[rhythm], ports):
                flows = list(port.queue[rhythm])
                del port.queue[rhythm][0 : len(port.queue[rhythm])]
                for flow in flows:
                    result = table[flow['dst']]
                    if result == None:
                        continue
                            
                    requested_thrpt[result] = requested_thrpt.setdefault(result, 0) + flow['feedin']
                    requested_flows.setdefault(result, []).append(flow)
                            
            for k, v in requested_thrpt.items():
                target = self.ports['existing']['selfRef'][self.links['existing'][k].endpoints.values()[0]]

                if target.capacity < v:
                    self.links['existing'][k].curr_load = target.capacity
                    # distribute available bandwidth to related flows proportionally
                    target.queue[flipflop[rhythm]].extend(map(lambda x: {'src': x['src'],
                                               'dst': x['dst'],
                                               'start': x['start'],
                                               'end': x['end'],
                                               'feedin': target.capacity * x['feedin'] / v},
                                    requested_flows[k]))
                else:
                    self.links['existing'][k].curr_load = v
                    target.queue[flipflop[rhythm]].extend(requested_flows[k])
                
    for k, v in self.services['existing'].items():
        if k.split('.')[-1] == 'forwarding':
            swt = switch(k, map(lambda x: self.ports['existing']['selfRef'][x], v.node.ports), \
                         v.data['rules'], pace=1) # pace = 1 means evaluate at every step
            next(swt) # push the functions to the first yield statement
            self.switchesL2[k] = swt