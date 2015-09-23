'''
Created on Jan 26, 2015

@author: mzhang
'''
import sys, threading, re
from time import sleep

from libnre.resourcemgmt import *
from libnre.utils import *
from apps.helm import helm

# temporarily hard code constants
HARDCODED_SERVICETYPE = "ps:tools:blipp"
HARDCODED_EVENTTYPE = "ps:tools:blipp:linux:net:ping:rtt"
HARDCODED_TYPE = "ping"

class Beacon(object):
    '''
    define a class whose objects are:
    1) triggered by user registered alarm
    2) make probe plan accordingly
    3) analyze the fault location
    '''
    
    class report():
        def __init__(self):
            self.content = None
            
    def __init__(self, unisrt, config_file):
        self.unisrt = unisrt
        with open(config_file) as f:
            self.conf = json.loads(f.read())
        
        try:
            self.pairs = map(lambda x: tuple(map(lambda y: self.unisrt.nodes['existing'][y], x)), self.conf['pairs'])
        except KeyError:
            print "nodes intended to monitor may not exist in UNIS yet"
            sys.exit()
            
        self.alarms = self.conf['alarms']
    
    def trigger(self, pair, alarm):
        '''
        pull probe results for a certain pair of (BLiPP enabled) nodes from nre,
        and apply statistical tool (user defined alarm function) to tell if this
        path went wrong
        '''
        # (src, dst, type) ==> measurement definition ==> metadata ==> ms results
        measurements = filter(lambda x: '%'.join([x.src, x.dst]) == '%'.join([pair[0].name, pair[-1].name]), self.unisrt.measurements['existing'].values())
        
        if measurements:
            for measurement in measurements:
                if HARDCODED_EVENTTYPE in measurement.eventTypes:
                    try:
                        meta = self.unisrt.metadata['existing']['%'.join([measurement.selfRef, HARDCODED_EVENTTYPE])]
                        alarm_module = __import__(alarm, fromlist = [alarm])
                        return alarm_module.alarm(self.unisrt, measurement, meta)
                    except KeyError:
                        print 'The measurement task has not returned any results yet'
                        return None
        else:
            print 'The source-destination pair %s - %s has not been under monitoring' % (pair[0].name, pair[1].name)
            return None
        
    def querySubPath(self, pairs):
        '''
        input:  [(node1, node2), (node3, node4), ...]
        output:  {(node1, node2):[[node1, <possible middle non-blipp hops>, nodeX], 
                                 [nodeX, <possible middle non-blipp hops>, nodeY]...
                                 [nodeZ, <possible middle non-blipp hops>, node2]],
                 (node3, node4):[[node3, <possible middle non-blipp hops>, nodeI],
                                 [nodeI, <possible middle non-blipp hops>, nodeJ]...
                                 [nodeK, <possible middle non-blipp hops>, node4]],
                 ...}
        '''    
        def blipp_sec(hops):
            '''
            make "blipp sections": transform [1(B), 2(B), 3, 4(B)] into [[1, 2], [2, 3, 4]]
            '''
            # Approximation: to find the optimal coverage inside a network is non-trivial
            # for now, we use predefined knowledge to answer this specific testbed
            for index, hop in enumerate(hops):
                if hop.name == 'domain_es.net_node_lbl-mr2':
                    hops[index] = self.unisrt.nodes['existing']['http://dev.crest.iu.edu:8889/nodes/domain_utah.edu_node_slc-slice-perf.chpc.utah.edu']
                if hop.name == 'domain_es.net_node_nersc-mr2':
                    hops[index] = self.unisrt.nodes['existing']['http://dev.crest.iu.edu:8889/nodes/domain_utah.edu_node_slc-slice-beacon.chpc.utah.edu']
                if hop.name == 'domain_es.net_node_anl-mr2':
                    hops[index] = self.unisrt.nodes['existing']['http://dev.crest.iu.edu:8889/nodes/domain_indiana.edu_node_dresci']
                    
            ret = list()
            section = list()
            for hop in hops:
                section.append(hop)
                if hasattr(hop, 'services') and HARDCODED_SERVICETYPE in hop.services.keys():
                    if len(section) > 1:
                        ret.append(section)
                        section = list()
                        section.append(hop)
                    else:
                        pass
                else:
                    pass
                    
            if len(section) > 1:
                ret.append(section)
                
            return ret
        
        paths = dict()
        for pair in pairs:
            paths[pair] = getResourceLists(self.unisrt, pair, models.node)
        
        for k, v in paths.items():
            paths[k] = blipp_sec(v)
            
        return paths
    
    def querySchedule(self, sections, schedule_params):
        '''
        consult HELM for schedules of all the paths
        '''
        import datetime, pytz
        import apps.helm.schedulers as schedulers
        now = datetime.datetime.utcnow()
        now += datetime.timedelta(seconds = 60) # there will be certain time gap before blipp reads its schedule
        now = pytz.utc.localize(now)
        schedules = {}
        for section in sections:
            schedules[(section[0], section[-1])] = schedulers.adaptive.build_basic_schedule(now,
                        datetime.timedelta(seconds = schedule_params['every']),
                        datetime.timedelta(seconds = schedule_params['duration']),
                        schedule_params['num_to_schedule'],
                        [])
        return schedules
    
        scheduler = helm.Helm(self.unisrt)
        return scheduler.schedule(sections, schedule_params)
    
    def configOF(self, paths, schedules):
        '''
        1) derive entrance hops from the inputed paths 
        2) at the controller, configure entrance hops to include blipp agent hosts into the slice
        '''
        import subprocess
        subprocess.call(['./esnet_flows.py', '--ip=tb-of-ctrl-1.es.net', '--port=9090', 'del', '2hop'])
        subprocess.call(['./esnet_flows2.py', '--ip=tb-of-ctrl-1.es.net', '--port=9090', 'add', 'myhop'])
    
    def diagnose(self, pair, decomp_path, symptom, schedules, schedule_params, report):
        '''
        param:
        1) post BLiPP tasks and wait for reports of all sections
        2) analyze for this bad path
        3) then send report back to parent_conn
        '''
        # Approximation: instead of one BLiPP probe host, we have to use two different hosts to
        # probe on two directions. This produce a challenge to tell which one to use on certain target
        # host, since it is actually caused by the SDN switch's limitation.
        # Here we drop one side manually and only test the nersc-dresci subpath.
        decomp_path = filter(lambda x: filter(lambda y: y.name == 'dresci', x), decomp_path)
        
        probes = []
        for section in decomp_path:
            print "assigning tasks to section %s - %s" % (section[0].name, section[-1].name)
            
            # use only one end (0 end) to launch a ping
            probe_service = section[0].services[HARDCODED_SERVICETYPE]
            # messy, just pick the only proper measurement
            p = re.compile(probe_service.selfRef + '%.*' + HARDCODED_EVENTTYPE + '.*')
            probe_meas_k = filter(lambda x: p.match(x), self.unisrt.measurements['existing'].keys())
            assert len(probe_meas_k) == 1
            probe_meas_k = probe_meas_k[0]
            m = self.unisrt.measurements['existing'][probe_meas_k]
            # should modify the attribute, not the data directly
            m.data["eventTypes"] = [HARDCODED_EVENTTYPE]
            m.data["type"] = HARDCODED_TYPE
            m.data["configuration"]["$schema"] = "http://unis.incntre.iu.edu/schema/tools/ping"
            m.data["configuration"]["status"] = "ON"
            m.data["configuration"]["name"] = "ping"
            m.data["configuration"]["collection_size"] = 10000000
            m.data["configuration"]["collection_ttl"] = 1500000
            m.data["configuration"]["collection_schedule"] = "builtins.scheduled"
            m.data["configuration"]["schedule_params"] = schedule_params
            m.data["configuration"]["reporting_params"] = 1
            m.data["configuration"]["resources"] = 'path resources'
            m.data['configuration']['src'] = section[0].name
            m.data['configuration']['dst'] = section[-1].name
            m.data["configuration"]["--client"] = section[0].name
            m.data["configuration"]["address"] = section[-1].name
            m.data["configuration"]["command"] = "ping -c 1 dresci.incntre.iu.edu"
            m.data["scheduled_times"] = schedules[(section[0], section[-1])]
            self.unisrt.validate_add_defaults(m.data["configuration"])
            
            m.renew_local(probe_meas_k)
            probes.append(probe_meas_k)
            
        
        self.unisrt.uploadRuntime('measurements')
        
        section_performances = {}
        while probes:
            time.sleep(60)
            found = []
            for probe in probes:
                print "looking for result of measurement %s" % probe
                
                metadata_key = '%'.join([self.unisrt.measurements['existing'][probe].selfRef, HARDCODED_EVENTTYPE])
                if metadata_key in self.unisrt.metadata['existing']:
                    data = self.unisrt.poke_remote(self.unisrt.metadata['existing'][metadata_key].id)
                    if data:
                        section_performances[metadata_key] = data
                        found.append(probe)

            map(lambda x: probes.remove(x), found)
            
        report.content = self.analyze(symptom, section_performances)
    
    def analyze(self, path_perf, sec_perf):
        '''
        looking at the performances of a path and each section of this path
        need some algorithm to tell where and what went wrong along the path
        '''
        s = ''
        for k, v in sec_perf.iteritems():
            s = s + k.__str__() + v.__str__()
        return 'result of ' + path_perf.__str__() + s
    
    def loop(self):
        patient_list = {}
        schedules = None
        reports = {}
        
        while True:
            for report in reports.values():
                if report.content:
                    print 'get report ' + report.content
                else:
                    print 'no report yet'
                # restore the pair, it is actually tricky as the fault may not be fixed at this moment
                #self.pairs.append(the old pair)
        
            for index, pair in enumerate(self.pairs):
                # the nth blipp pair corresponds to the nth alarm defined in the conf file
                symptom = self.trigger(pair, self.alarms[index])
                if symptom:
                    patient_list[pair] = symptom
                    del self.pairs[index]
                else:
                    print 'no symptom discovered'
        
            if patient_list:
                schedule_params = {'duration':10, 'num_to_schedule':1, 'every':0}
                # decompose the paths to BLiPP sections
                decomp_paths = self.querySubPath(patient_list.keys())
                
                # section_pile: a collection of all sections of different paths. it's for scheduling purposes
                section_pile = dict()
                for decomp_path in decomp_paths.values():
                    for section in decomp_path:
                        # BUG ATTENTION: repeated sections will cause troubles
                        section_pile[(section[0], section[-1])] = section
                
                # schedule all sections by using helm
                schedules = self.querySchedule(section_pile, schedule_params)
                
                # modify the slice topology to inject probes
                if schedules:
                    self.configOF(decomp_paths, schedules)
                else:
                    print "Ooops, cannot schedule the test. Big trouble. Gonna try again now..."
                    continue
            
                # spawn threads to handle each bad path
                for k, v in decomp_paths.items():
                    report = self.report()
                    reports[k] = report
                    
                    threading.Thread(name=k, target=self.diagnose, args=(k, v, patient_list[k], schedules, schedule_params, report, )).start()
                    
                patient_list.clear()
                    
            sleep(60)
            
def run(unisrt, args):
    '''
    all nre apps are required to have a run() function as the
    driver of this application
    '''
    beacon = Beacon(unisrt, args)
    beacon.loop()
    
if __name__ == '__main__':
    import kernel.unisrt
    unisrt = kernel.unisrt.UNISrt()
    beacon = Beacon(unisrt, '/home/mzhang/workspace/nre/apps/beacon/beacon.conf')
    beacon.loop()