'''
Created on Jan 26, 2015

@author: mzhang
'''
from time import sleep
from multiprocessing import Process, Pipe

from libnre.resourcemgmt import *
from apps.helm import helm

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
            
        self.probes = map(lambda x: map(lambda y: self.unisrt.services['existing'][y], x), self.conf['blipps'])
    
    def trigger(self, pair):
        '''
        pull probe results for a certain pair of nodes from UNISrt
        apply statistical tool to tell if this path went wrong
        '''
        return object()
        
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
    
        raw_decomp = map(lambda x: getGENIResourceLists(self.unisrt, x), pairs)
        for chain in raw_decomp:
            for ring in chain:
                pass
    
    def querySchedule(self, paths):
        '''
        consult HELM for a schedule of all the paths
        '''
        scheduler = helm.Helm(self.unisrt)
        #return scheduler.schedule(paths, True)
        return None
    
    def configOF(self, paths, schedule):
        '''
        1) derive entrance hops from the input paths 
        2) at the controller, configure entrance hops to include blipp agent hosts into the slice
        '''
        pass
    
    def diagnose(self, orig_path, sections, schedule, conn):
        '''
        param:
        path -- already decomposed bad path
        schedule -- schedule to run probes
        1) post BLiPP tasks and wait for reports of all sections
        2) analyze for this bad path
        3) then send report back to parent_conn
        '''
        print 'in diagnose'
        pass
    
    def analyze(self, path_perf, sec_perf):
        '''
        by looking at the performances of a path and each section of this path,
        this function tries to tell where and what went wrong
        '''
        pass
    
    def loop(self):
        bad_pairs = []
        schedule = None
        reports = []
        
        while True:
            for report in reports:
                # check through diagnose processes for results, report to user and push the task back to the monitoring queue
                pass
        
            for index, pair in enumerate(self.probes):
                symptom = self.trigger(pair)
                if symptom:
                    bad_pairs.append(pair)
                    del self.probes[index]
        
            if bad_pairs:
                # prepare for diagnosis: decompose the paths, schedule probes
                decomp_paths = self.querySubPath(bad_pairs)
                restru_paths = dict()
                for decomp_path in decomp_paths.values():
                    for section in decomp_path:
                        # BUG ATTENTION: section may be repeated
                        restru_paths[(section[0], section[-1])] = section
                schedule = self.querySchedule(restru_paths)
                # for now, configOF is done here; could be dispatched to each diagnose process, so that each diagnose
                # process continues on success of its configuring to allow more flexibility
                self.configOF(decomp_paths, schedule)
            
                # spawn processes to handle each bad path
                for k, v in decomp_paths.items():
                    parent_conn, child_conn = Pipe()
                    reports.append(parent_conn)
                    diagnose_proc = Process(target = self.diagnose, args = (k, v, schedule, child_conn, ))
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