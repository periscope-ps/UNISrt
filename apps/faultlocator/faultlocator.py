'''
Created on Jan 26, 2015

@author: mzhang
'''
from time import sleep
from multiprocessing import Process, Pipe

from libnre.resourcemgmt import *
from libnre.utils import *
from apps.helm import helm

# temporarily hard code all eventType to PING
# it should be determined by symptom
HARDCODED_EVENTTYPE = "ps:tools:blipp:linux:net:ping:ttl"
HARDCODED_TYPE = "ping"

class FaultLocator(object):
    '''
    define a class whose objects are:
    1) triggered by user registered alarm
    2) make probe plan accordingly
    3) analyze the fault location
    '''
    def __init__(self, unisrt, config_file):
        self.unisrt = unisrt
        with open(config_file) as f:
            self.conf = json.loads(f.read())
            
        self.pairs = map(lambda x: tuple(map(lambda y: self.unisrt.nodes['existing'][y].name, x)), self.conf['pairs'])
        self.alarms = self.conf['alarms']
    
    def trigger(self, pair, alarm):
        '''
        pull probe results for a certain pair of (BLiPP enabled) nodes from nre,
        and apply statistical tool (user defined alarm function) to tell if this
        path went wrong
        '''
    
        # (src, dst, type)==>measurement definition==>metadata==>ms results
        meas = self.unisrt.measurements['existing']['%'.join([pair[0], pair[1]])].selfRef
        meta = self.unisrt.metadata['existing']['%'.join([meas, HARDCODED_EVENTTYPE])].id
        measurement_data = self.unisrt.poke_remote(meta)
        
        alarm_module = __import__(alarm, fromlist = [alarm])
        return alarm_module.alarm(measurement_data)
        
    def querySubPath(self, pairs):
        '''
        input: [[node1, node2], [node3, node4], ...]
        output: {(node1, node2):[[node1, <possible middle non-blipp hops>, nodeX], 
                                 [nodeX, <possible middle non-blipp hops>, nodeY]...
                                 [nodeZ, <possible middle non-blipp hops>, node2]],
                 (node3, node4):[[node3, <possible middle non-blipp hops>, nodeI],
                                 [nodeI, <possible middle non-blipp hops>, nodeJ]...
                                 [nodeK, <possible middle non-blipp hops>, node4]],
                 ...}
        1) pull all the potential insertion nodes
        2) filter out the nodes without BLiPP agents connected
        3) return sub paths for each inquiry path
        '''    
        def blipp_sec(hops):
            # make "blipp sections": transform [1, 2, 3] into [[1, 2], [2, 3]]
            ret = list()
            section = list()
            for hop in hops:
                section.append(hop)
                if True:#hasattr(hop, 'services') and 'blipp' in map(lambda x: x.name, hop.services.values()):
                    if len(section) > 1:
                        ret.append(section)
                        section = list()
                        section.append(hop)
                    else:
                        pass
                else:
                    pass
                    
            return ret
        
        paths = getGENIResourceLists(self.unisrt, pairs)
        
        for k, v in paths.items():                    
            paths[k] = blipp_sec(v)
            
        return paths
    
    def querySchedule(self, paths, schedule_params):
        '''
        consult HELM for schedules of all the paths
        '''
        scheduler = helm.Helm(self.unisrt)
        return scheduler.schedule(paths, schedule_params)
    
    def configOF(self, paths, schedules):
        '''
        1) derive entrance hops from the inputed paths 
        2) at the controller, configure entrance hops to include blipp agent hosts into the slice
        '''
        pass
    
    def diagnose(self, pair, path, symptom, schedules, schedule_params, conn):
        '''
        param:
        1) post BLiPP tasks and wait for reports of all sections
        2) analyze for this bad path
        3) then send report back to parent_conn
        '''
        print "%s assigning tasks to sections..." % pair.__str__()        
        for section in path:
            
            measurement = build_measurement(self.unisrt, section[0])
            measurement["eventTypes"] = [HARDCODED_EVENTTYPE]
            measurement["type"] = HARDCODED_TYPE
            probe = {
                       "$schema": "http://unis.incntre.iu.edu/schema/tools/ping",
                       "--client": section[1],
                       "address": section[1]
            }
            self.unisrt.validate_add_defaults(probe)
            measurement["configuration"] = probe
            measurement["configuration"]["name"] = "ping"
            measurement["configuration"]["collection_size"] = 10000000
            measurement["configuration"]["collection_ttl"] = 1500000
            measurement["configuration"]["collection_schedule"] = "builtins.scheduled"
            measurement["configuration"]["schedule_params"] = schedule_params
            measurement["configuration"]["reporting_params"] = 1
            measurement["configuration"]["resources"] = path
            measurement["scheduled_times"] = schedules[(section[0], section[-1])]
            
            self.unisrt.updateRuntime([measurement], 'measurements', True)
        
        self.unisrt.uploadRuntime('measurements')
        
        print "%s waiting for each section..." % pair.__str__()
        sections = list(path)
        report = {}
        while sections:
            time.sleep(60)
            found = []
            for section in sections:
                if '.'.join([section[0]['selfRef'], HARDCODED_EVENTTYPE]) in self.unisrt.metadata['existing']:                    
                    report[(section[0], section[-1])] = self.unisrt.poke_remote(self.unisrt.metadata['existing']['.'.join([section[0]['selfRef'], HARDCODED_EVENTTYPE])].id)
                    
                    # turn off this blipp measurement, after it posted its result
                    #section[0]['configuration']['status'] = "OFF"
                    #self.unisrt.updateRuntime([v[0]], models.measurement, True)

                    found.append(section)

            map(lambda x: sections.remove(x), found)
        
        result = self.analyze(symptom, report)
        
        conn.send(pair, result)
        conn.close()
    
    def analyze(self, path_perf, sec_perf):
        '''
        looking at the performances of a path and each section of this path
        need some algorithm to tell where and what went wrong along the path
        '''            
        return object()
            
    
    def loop(self):
        patient_list = []
        symptom_list = {}
        schedules = None
        reports = []
        
        while True:
            for report in reports:
                # check through diagnose processes for results, report to user and push the task back to the monitoring queue
                if report.poll():
                    pair, result = report.recv()
                    self.pairs.append(pair)
                    print result
        
            for index, pair in enumerate(self.pairs):
                # the nth blipp pair corresponds to the nth alarm defined in the conf file
                symptom = self.trigger(pair, self.alarms[index])
                if symptom:
                    patient_list.append(pair)
                    symptom_list[pair] = symptom
                    del self.pairs[index]
        
            if patient_list:
                schedule_params = {'duration':10, 'num_tests':1, 'every':0}
                
                # prepare for diagnosis: decompose the paths, schedule probes
                decomp_paths = self.querySubPath(patient_list)
                
                restru_paths = dict()
                for decomp_path in decomp_paths.values():
                    for section in decomp_path:
                        # BUG ATTENTION: failed on repeated section
                        restru_paths[(section[0], section[-1])] = section
                        
                # restru_paths: a collection of all sections of different paths
                schedules = self.querySchedule(restru_paths, schedule_params)
                
                # for now, OF configuration is done here; could be dispatched to each diagnose process,
                # so that each diagnose process continues on success of its configuring to allow more flexibility
                if schedules:
                    self.configOF(decomp_paths, schedules)
                else:
                    print "Ooops, cannot schedule the test. Big trouble. Gonna try again now..."
                    continue
            
                # spawn processes to handle each bad path
                for k, v in decomp_paths.items():
                    parent_conn, child_conn = Pipe()
                    reports.append(parent_conn)
                    diagnose_proc = Process(target = self.diagnose, args = (k, v, symptom_list[k], schedules, schedule_params, child_conn, ))
                    diagnose_proc.start()
                    
                del patient_list[:]
                    
            sleep(60)
            
def run(unisrt, args):
    '''
    all nre apps are required to have a run() function as the
    driver of this application
    '''
    faultlocator = FaultLocator(unisrt, args)
    faultlocator.loop()
    
if __name__ == '__main__':
    import kernel.unisrt
    unisrt = kernel.unisrt.UNISrt()
    faultlocator = FaultLocator(unisrt, '/home/mzhang/workspace/nre/apps/faultlocator/faultlocator.conf')
    faultlocator.loop()