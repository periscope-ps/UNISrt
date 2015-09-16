'''
Created on Jan 26, 2015

@author: mzhang
'''
from time import sleep
import threading

from libnre.resourcemgmt import *
from libnre.utils import *
from apps.helm import helm

# temporarily hard code all eventType to PING
# it should be determined by symptom
HARDCODED_EVENTTYPE = "ps:tools:blipp:linux:net:ping:ttl"
HARDCODED_TYPE = "ping"

class Beacon(object):
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
            
        self.pairs = map(lambda x: tuple(map(lambda y: self.unisrt.nodes['existing'][y], x)), self.conf['pairs'])
        self.alarms = self.conf['alarms']
    
    def trigger(self, pair, alarm):
        '''
        pull probe results for a certain pair of (BLiPP enabled) nodes from nre,
        and apply statistical tool (user defined alarm function) to tell if this
        path went wrong
        '''
        # (src, dst, type)==>measurement definition==>metadata==>ms results
        p = filter(lambda x: '%'.join([x.src, x.dst]) == '%'.join([pair[0].name, pair[1].name]), self.unisrt.measurements['existing'].values())
        if p:
            # can further add different measurement eventTypes, but ping only at this moment    
            meas = p[0]
        else:
            print 'The source-destination pair %s - %s has not been under monitoring' % (pair[0].name, pair[1].name)
            return None
        try:
            meta = self.unisrt.metadata['existing']['%'.join([meas.selfRef, HARDCODED_EVENTTYPE])]
        except KeyError:
            print 'The measurement task has not returned any results yet'
            return None
        
        alarm_module = __import__(alarm, fromlist = [alarm])
        return alarm_module.alarm(meas, self.unisrt.poke_remote(meta.id))
        
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
            ''' make "blipp sections": transform [1(B), 2(B), 3, 4(B)] into [[1, 2], [2, 3, 4]] '''
            ret = list()
            section = list()
            for hop in hops:
                section.append(hop)
                if hasattr(hop, 'services') and 'blipp' in map(lambda x: x.name, hop.services):
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
            paths[(pair[0], pair[-1])] = getResourceLists(self.unisrt, pair, models.node)
        
        # Cheating: before we query which node has BLiPP plugged in, we plug one in nersc
        blipp_services = filter(lambda x: x.node.name == 'slc-slice-beacon.chpc.utah.edu', self.unisrt.services['existing'].values())
        #blipp_services = filter(lambda x: x.node.name == 'MiaoZhang_Gentoo', self.unisrt.services['existing'].values())
        for hops in paths.values():
            for node in hops:
                if node.name == 'domain_es.net_node_nersc-mr2':
                    setattr(node, 'services', blipp_services)
        
        for k, v in paths.items():                    
            paths[k] = blipp_sec(v)
            
        return paths
    
    def querySchedule(self, paths, schedule_params):
        '''
        consult HELM for schedules of all the paths
        '''
        import datetime, pytz
        import apps.helm.schedulers as schedulers
        now = datetime.datetime.utcnow()
        now += datetime.timedelta(seconds = 60) # there will be certain time gap before blipp reads its schedule
        now = pytz.utc.localize(now)
        return schedulers.adaptive.build_basic_schedule(now,
                        datetime.timedelta(seconds = schedule_params['every']),
                        datetime.timedelta(seconds = schedule_params['duration']),
                        schedule_params['num_to_schedule'],
                        [])
    
        scheduler = helm.Helm(self.unisrt)
        return scheduler.schedule(paths, schedule_params)
    
    def configOF(self, paths, schedules):
        '''
        1) derive entrance hops from the inputed paths 
        2) at the controller, configure entrance hops to include blipp agent hosts into the slice
        '''
        import subprocess
        subprocess.call(['./esnet_flows.py', '--ip=tb-of-ctrl-1.es.net', '--port=9090', 'del', '2hop'])
        subprocess.call(['./esnet_flows2.py', '--ip=tb-of-ctrl-1.es.net', '--port=9090', 'add', 'myhop'])
    
    def diagnose(self, pair, path, symptom, schedules, schedule_params, report):
        '''
        param:
        1) post BLiPP tasks and wait for reports of all sections
        2) analyze for this bad path
        3) then send report back to parent_conn
        '''
        # temporary solution: instead of one BLiPP probe host, we have to use two different hosts to
        # probe on two directions. This produce a challenge to tell which one to use on certain target
        # host, since it is actually caused by the SDN switch's limitation.
        # Here we drop one side manually and only test the nersc-dresci subpath.
        path = filter(lambda x: filter(lambda y: y.name == 'domain_es.net_node_anl-mr2', x), path)
        
        probes = []

        for section in path:

            print "assigning tasks to section %s - %s" % (section[0].name, section[-1].name)
            
            eventType = list(["ps:tools:blipp:linux:net:ping:rtt",
                             "ps:tools:blipp:linux:net:ping:ttl"])
            # only one node has service and has only one service, need to generalize
            probe_service = filter(lambda x: hasattr(x, 'services'), section)[0].services[0]
            probe_meas_k = '%'.join([probe_service.selfRef, '+'.join(eventType)])
            probes.extend([probe_meas_k])
            m = self.unisrt.measurements['existing'][probe_meas_k]
            
            '''
            service_selfRef = filter(lambda x: hasattr(x, 'services'), section)[0].services[0].selfRef
            measurement = build_measurement(self.unisrt, service_selfRef)
            measurement["eventTypes"] = [HARDCODED_EVENTTYPE]
            measurement["type"] = HARDCODED_TYPE
            probe = {
                       "$schema": "http://unis.incntre.iu.edu/schema/tools/ping",
                       # within each BLiPP section, only assign an unidirectional ping 0 --> 1 is enough
                       "--client": section[0].name,
                       "address": section[-1].name,
                       "command": "ping -c 1 dresci.incntre.iu.edu",
            }
            self.unisrt.validate_add_defaults(probe)
            measurement["configuration"] = probe
            measurement["configuration"]["status"] = "ON"
            measurement["configuration"]["name"] = "ping"
            measurement["configuration"]["collection_size"] = 10000000
            measurement["configuration"]["collection_ttl"] = 1500000
            measurement["configuration"]["collection_schedule"] = "builtins.scheduled"
            measurement["configuration"]["schedule_params"] = schedule_params
            measurement["configuration"]["reporting_params"] = 1
            measurement["configuration"]["resources"] = 'path resources'
            measurement['configuration']['src'] = probe['--client']
            measurement['configuration']['dst'] = probe['address']
            measurement["scheduled_times"] = schedules
            '''
            
            # should modify the attribute, not the data directly
            m.data["eventTypes"] = [HARDCODED_EVENTTYPE]
            m.data["type"] = HARDCODED_TYPE
            probe = {
                       "$schema": "http://unis.incntre.iu.edu/schema/tools/ping",
                       # within each BLiPP section, only assign an unidirectional ping 0 --> 1 is enough
                       "--client": section[0].name,
                       "address": section[-1].name,
                       "command": "ping -c 1 dresci.incntre.iu.edu",
            }
            self.unisrt.validate_add_defaults(probe)
            m.data["configuration"] = probe
            m.data["configuration"]["status"] = "ON"
            m.data["configuration"]["name"] = "ping"
            m.data["configuration"]["collection_size"] = 10000000
            m.data["configuration"]["collection_ttl"] = 1500000
            m.data["configuration"]["collection_schedule"] = "builtins.scheduled"
            m.data["configuration"]["schedule_params"] = schedule_params
            m.data["configuration"]["reporting_params"] = 1
            m.data["configuration"]["resources"] = 'path resources'
            m.data['configuration']['src'] = probe['--client']
            m.data['configuration']['dst'] = probe['address']
            m.data["scheduled_times"] = schedules
            m.renew_local(probe_meas_k)
        
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
                    if not data:
                        continue
                    section_performances[metadata_key] = data
                    found.append(probe)

            map(lambda x: probes.remove(x), found)
            
        az = self.analyze(symptom, section_performances)
        report.content = az
    
    def analyze(self, path_perf, sec_perf):
        '''
        looking at the performances of a path and each section of this path
        need some algorithm to tell where and what went wrong along the path
        '''
        s = ''
        for k, v in sec_perf.iteritems():
            s = s + k.__str__() + v.__str__()
        return 'result of ' + path_perf.__str__() + s
            
    
    class r():
        def __init__(self):
            self.content = None
    
    def loop(self):
        patient_list = []
        symptom_list = {}
        schedules = None
        reports = []
        
        while True:
            for report in reports:
                # check through diagnose processes for results, report to user and push the task back to the monitoring queue
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
                    patient_list.append(pair)
                    symptom_list[pair] = symptom
                    del self.pairs[index]
                else:
                    print 'no symptom discovered'
        
            if patient_list:
                schedule_params = {'duration':10, 'num_to_schedule':1, 'every':0}
                
                # prepare for diagnosis

                # decompose the paths to BLiPP sections
                decomp_paths = self.querySubPath(patient_list)
                
                restru_paths = dict()
                for decomp_path in decomp_paths.values():
                    for section in decomp_path:
                        # BUG ATTENTION: failed on repeated section
                        restru_paths[(section[0], section[-1])] = section
                        
                # restru_paths: a collection of all sections of different paths
                # for scheduling purposes
                schedules = self.querySchedule(restru_paths, schedule_params)
                
                # for now, OF configuration is done here; could be dispatched to each diagnose process,
                # so that each diagnose process continues on success of its configuring to allow more flexibility
                if schedules:
                    self.configOF(decomp_paths, schedules)
                else:
                    print "Ooops, cannot schedule the test. Big trouble. Gonna try again now..."
                    continue
            
                # spawn threads to handle each bad path
                for k, v in decomp_paths.items():
                    #parent_conn, child_conn = Pipe()
                    report = self.r()
                    reports.append(report)
                    #diagnose_proc = Process(target = self.diagnose, args = (k, v, symptom_list[k], schedules, schedule_params, child_conn, ))
                    #diagnose_proc.start()
                    
                    threading.Thread(name=k, target=self.diagnose, args=(k, v, symptom_list[k], schedules, schedule_params, report, )).start()
                    
                del patient_list[:]
                    
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