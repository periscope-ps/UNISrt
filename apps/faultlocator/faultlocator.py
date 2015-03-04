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
HARDCODED_EVENTTYPE = "ps:tools:blipp:linux:net:ping"

class FaultLocator(object):
    '''
    define a class whose objects:
    1) triggered by user registered alarm
    2) make probe plan accordingly
    3) analyze the fault location
    '''
    def __init__(self, unisrt, config_file):
        self.unisrt = unisrt
        with open(config_file) as f:
            self.conf = json.loads(f.read())
            
        # turn service string names into service objects in UNISrt
        self.probes = map(lambda x: map(lambda y: self.unisrt.services['existing'][y], x), self.conf['blipps'])
        self.alarms = self.conf['alarms']
    
    def trigger(self, pair, alarm):
        '''
        pull probe results for a certain pair of BLiPP service from UNISrt,
        and apply statistical tool (alarm function) to tell if this path went wrong
        alarm function should be chosen/provided
        '''
        
        # (src, dst, type)==>measurement definition==>metadata==>measurement results
        measurement_data = \
        self.unisrt.poke_remote(
        self.unisrt.metadata[self.localnew and 'new' or 'existing'][
        self.unisrt.measurements['existing']['.'.join([pair[0], pair[1], HARDCODED_EVENTTYPE])].href].id)        
        
        alarm_func = __import__(alarm, fromlist = [alarm])
        return alarm_func(measurement_data)
        
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
        #before finishing the blipp-filter mechanism, bogus it:
        blipp_vm4 = self.unisrt.services['existing']['http://dev.incntre.iu.edu:8889/services/54d9881de779893d95b3faec']
        blipp_vm5 = self.unisrt.services['existing']['http://dev.incntre.iu.edu:8889/services/54d98849e779893d95b3faf4']
        return {(pairs[0][0], pairs[0][1]): [[pairs[0][0], blipp_vm4], [blipp_vm4, blipp_vm5], [blipp_vm5, pairs[0][1]]]}
    
        def blipp_sec(hops):
            # make blipp sections: transform [1, 2, 3] into [[1, 2], [2, 3]]
            return
        
        paths = getGENIResourceLists(self.unisrt, pairs)
        
        for k, v in paths:                    
            paths[k] = blipp_sec(v)
            
        return paths
    
    def querySchedule(self, paths):
        '''
        consult HELM for schedules of all the paths
        '''
        scheduler = helm.Helm(self.unisrt)
        return scheduler.schedule(paths, True)
        return True
    
    def configOF(self, paths, schedules):
        '''
        1) derive entrance hops from the input paths 
        2) at the controller, configure entrance hops to include blipp agent hosts into the slice
        '''
        pass
    
    def diagnose(self, pair, path, symptom, schedules, conn):
        '''
        param:
        1) post BLiPP tasks and wait for reports of all sections
        2) analyze for this bad path
        3) then send report back to parent_conn
        '''
        print "%s assigning tasks to sections...", pair        
        for section in path:
            
            measurement = build_measurement(self.unisrt, section[0])
            measurement["eventTypes"] = [HARDCODED_EVENTTYPE]
            measurement["type"] = "ping"
            ping_probe = {
                       "$schema": "http://unis.incntre.iu.edu/schema/tools/ping",
                       "--client": section[1] # dst name
            }
            self.unisrt.validate_add_defaults(ping_probe)
            measurement["configuration"] = ping_probe
            measurement["configuration"]["name"] = "ping"
            measurement["configuration"]["collection_size"] = 10000000
            measurement["configuration"]["collection_ttl"] = 1500000
            measurement["configuration"]["collection_schedule"] = "builtins.scheduled"
            #measurement["configuration"]["schedule_params"] = HM.probe['schedule_params']
            measurement["configuration"]["reporting_params"] = 1
            measurement["configuration"]["resources"] = path
            measurement["scheduled_times"] = schedules[(section[0], section[-1])]
            
            self.unisrt.updateRuntime([measurement], models.measurement, True)
        
        self.unisrt.syncRuntime(resources = [models.measurement])
        
        print "%s waiting for each section...", pair
        sections = list(path)
        report = {}
        while sections:
            time.sleep(60)
            self.unisrt.syncRuntime(resources = [models.metadata])
            found = []
            for section in sections:
                if '.'.join([section[0]['selfRef'], HARDCODED_EVENTTYPE]) in self.unisrt.metadata['existing']:                    
                    report[(section[0], section[-1])] = self.unisrt.poke_remote(self.unisrt.metadata['existing']['.'.join([section[0]['selfRef'], HARDCODED_EVENTTYPE])].id)
                    
                    # turn off this blipp measurement, after it posted its result
                    #section[0]['configuration']['status'] = "OFF"
                    #self.unisrt.updateRuntime([v[0]], models.measurement, True)

                    found.append(section)
                else:
                    break

            map(lambda x: sections.remove(x), found)
            break
        
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
        bad_pairs = []
        symptom_list = {}
        schedules = None
        reports = []
        
        while True:
            for report in reports:
                # check through diagnose processes for results, report to user and push the task back to the monitoring queue
                if report.poll():
                    pair, result = report.recv()
                    self.probes.append(pair)
                    print result
        
            for index, pair in enumerate(self.probes):
                # the nth blipp pair corresponds to the nth alarm defined in the conf file
                symptom = self.trigger(pair, self.alarms[index])
                if symptom:
                    bad_pairs.append(pair)
                    symptom_list[pair] = symptom
                    del self.probes[index]
        
            if bad_pairs:
                # prepare for diagnosis: decompose the paths, schedule probes
                decomp_paths = self.querySubPath(bad_pairs)
                
                restru_paths = dict()
                for decomp_path in decomp_paths.values():
                    for section in decomp_path:
                        # BUG ATTENTION: section may be repeated
                        restru_paths[(section[0], section[-1])] = section
                        
                # restru_paths: a collection of all sections of different paths
                schedules = self.querySchedule(restru_paths)
                
                # for now, configOF is done here; could be dispatched to each diagnose process, so that each diagnose
                # process continues on success of its configuring to allow more flexibility
                if schedules:
                    self.configOF(decomp_paths, schedules)
                else:
                    print "Ooops, cannot schedule the test. Big trouble. Gonna try again now..."
                    continue
            
                # spawn processes to handle each bad path
                for k, v in decomp_paths.items():
                    parent_conn, child_conn = Pipe()
                    reports.append(parent_conn)
                    diagnose_proc = Process(target = self.diagnose, args = (k, v, symptom_list[k], schedules, child_conn, ))
                    diagnose_proc.start()
                    
                del bad_pairs[:]
                    
            sleep(30)
            
def run(unisrt, args):
    '''
    all nre apps are required to have a run() function as the
    driver of this application
    '''
    faultlocator = FaultLocator(unisrt, args)
    faultlocator.loop()