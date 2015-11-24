'''
Created on Jan 26, 2015

@author: mzhang
'''
import re
import sys
import threading
import kernel.models as models

from time import sleep, time
from libnre.utils import *

# temporarily hard code constants
INTERVAL = 60
PING_TOLERANCE = 50
IPERF_TOLERANCE = 90
INPATIANT_TOLERANCE = 1
HARDCODED_SERVICETYPE = "ps:tools:blipp"
RTT = "ps:tools:blipp:linux:net:ping:rtt"
BANDWIDTH = "ps:tools:blipp:linux:net:iperf:bandwidth"
SLICE_IP = {
    'slc-slice-perf.chpc.utah.edu': '192.168.2.3',
    'slc-slice-beacon.chpc.utah.edu': '192.168.2.4',
    'slc-slice-beacon1.chpc.utah.edu': '192.168.2.5',
    'dresci': '192.168.2.2'
}

logger = settings.get_logger('beacon')

class Beacon(object):
    '''
    define a class whose objects are:
    1) triggered by user registered alarm
    2) make probe plan accordingly
    3) analyze the fault location
    '''
            
    def __init__(self, unisrt, config_file):
        if config_file == None:
            raise IOError("configuration file is missing")
        
        self.unisrt = unisrt
        with open(config_file) as f:
            self.conf = json.loads(f.read())
        
        try:
            self.pairs = map(lambda x: tuple(map(lambda y: self.unisrt.nodes['existing'][y], x)), self.conf['pairs'])
        except KeyError:
            logger.info("nodes intended to monitor may not exist in UNIS yet")
            sys.exit()
            
        self.alarms = self.conf['alarms']
        self.events = self.conf['monitoring-events']
    
    def trigger(self, event_type, measurement, tolerance, no_look_back = 0):
        '''
        poll probe results from certain measurement, and apply statistical tool
        (user defined alarm function) to tell if this path went wrong
        the resolving logic: measurement ==> metadata ==> ms results
        '''
        start = time()
        while '%'.join([measurement.selfRef, event_type]) not in self.unisrt.metadata['existing'] and time() - start < tolerance:
            logger.info("The measurement task {m} has not returned any results yet".format(m = measurement))
            sleep(5)
        
        remain = max(0, tolerance - (time() - start))
        
        alarm_name = self.alarms[event_type]
        alarm_module = __import__(alarm_name, fromlist = [alarm_name])
        try:
            meta = self.unisrt.metadata['existing']['%'.join([measurement.selfRef, event_type])]
            return alarm_module.alarm(self.unisrt, measurement, meta, no_look_back, remain)
        except KeyError:
            return alarm_module.complain(None)
        
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
            # TODO: to find the optimal coverage inside a network is non-trivial
            # for now, we use predefined knowledge to answer this specific testbed
            for index, hop in enumerate(hops):
                if hop.name == 'domain_es.net_node_nersc-mr2':
                    hops.insert(index, self.unisrt.nodes['existing']['http://dev.crest.iu.edu:8889/nodes/domain_utah.edu_node_slc-slice-beacon.chpc.utah.edu'])
                    hops.insert(index, self.unisrt.nodes['existing']['http://dev.crest.iu.edu:8889/nodes/domain_utah.edu_node_slc-slice-beacon1.chpc.utah.edu'])
                    hops.insert(index, self.unisrt.nodes['existing']['http://dev.incntre.iu.edu:8889/nodes/domain_es.net_node_nersc-mr2'])
                    break
                    
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
            
            # work around the one-place-two-probe issue at NERSC
            for section in ret:
                if section[0].name in ['slc-slice-beacon.chpc.utah.edu', 'slc-slice-beacon1.chpc.utah.edu'] and\
                        section[-1].name in ['slc-slice-beacon.chpc.utah.edu', 'slc-slice-beacon1.chpc.utah.edu']:
                    ret.remove(section)
                
            return ret
        
        
        paths = dict()
        for pair in pairs:
            try:
                sleep(20) # waiting for subscribed channel to update
                backbone_path = filter(lambda x: x.status == 'ON', self.unisrt.paths['existing']['%'.join([pair[0].selfRef, pair[1].selfRef])])
                assert len(backbone_path) == 1
                
                paths[pair] = [pair[0]] + backbone_path[0].hops + [pair[1]]
            except KeyError:
                logger.info("the queried path is not found in UNIS")
        
        for k, v in paths.items():
            paths[k] = blipp_sec(v)
            
        return paths
    
    def querySchedule(self, sections, schedule_params):
        '''
        consult HELM for schedules of all the paths
        But now, it merely return the current time as the schedule
        '''
        import datetime, pytz
        import apps.helm.schedulers.adaptive as adaptive
        now = datetime.datetime.utcnow()
        now += datetime.timedelta(seconds = 60) # there will be certain time gap before blipp reads its schedule
        now = pytz.utc.localize(now)
        schedules = {}
        for section in sections:
            schedules[frozenset([section[0], section[-1]])] = adaptive.build_basic_schedule(now,
                        datetime.timedelta(seconds = schedule_params['every']),
                        datetime.timedelta(seconds = schedule_params['duration']),
                        schedule_params['num_to_schedule'],
                        [])
        return schedules
    
    def configOF(self, paths):
        '''
        the input is a list of path objects e.g. backup paths to enable or,
        diagnose-purpose subpath, which is used by BLiPP agent hosts
        '''
        assert isinstance(paths, list)
        for item in paths:
            assert isinstance(item, models.path)
        
        # TODO: need some mechanism to generate SDN controller scripts based on this path list
        import subprocess
        if len(paths) == 2:
            subprocess.call(['apps/beacon/esnet_flows2.py', '--ip=tb-of-ctrl-1.es.net', '--port=9090', 'add', 'myhop'])
        else:
            subprocess.call(['apps/beacon/esnet_flows2.py', '--ip=tb-of-ctrl-1.es.net', '--port=9090', 'del', 'myhop'])
            subprocess.call(['apps/beacon/esnet_flows.py', '--ip=tb-of-ctrl-1.es.net', '--port=9090', 'del', '2hop'])
            subprocess.call(['apps/beacon/esnet_flows.py', '--ip=tb-of-ctrl-1.es.net', '--port=9090', 'add', '1hop'])
        return
    
    def analyze(self, path_symp, sec_perf):
        '''
        looking at the performances of a path and each section of this path
        need some algorithm to tell where and what went wrong along the path
        '''
        logger.info("path symptom is {ps}".format(ps = path_symp))
        # no real analysis at this point, but to display the diagnose path healthiness (and turn it off after a pause)
        for sec, perf in sec_perf.iteritems():
            logger.info("section between {s} and {d} has issue: {p}".format(s = sec[0].name, d = sec[-1].name, p = perf[0]))
            diag_path_name = '%'.join([sec[0].selfRef, sec[-1].selfRef])
            if diag_path_name not in self.unisrt.paths['existing']:
                diag_path_name = '%'.join([sec[-1].selfRef, sec[0].selfRef])
            diag_path = self.unisrt.paths['existing'][diag_path_name][0]# arbitrarily pick 0
            if not perf[0]:
                diag_path.healthiness = 'good'
            else:
                diag_path.healthiness = 'bad'
                
            diag_path.renew_local(diag_path_name)
        
        self.unisrt.pushRuntime('paths')
        
        
        print "!!! map refreshed: diagnose paths change colors"
        sleep(10)
        
        
        for sec, perf in sec_perf.iteritems():
            diag_path_name = '%'.join([sec[0].selfRef, sec[-1].selfRef])
            if diag_path_name not in self.unisrt.paths['existing']:
                diag_path_name = '%'.join([sec[-1].selfRef, sec[0].selfRef])
            diag_path = self.unisrt.paths['existing'][diag_path_name][0]# arbitrarily pick 0
            diag_path.status = 'OFF'
            diag_path.renew_local(diag_path_name)
        
        self.unisrt.pushRuntime('paths')
        
    def investigate(self, subpaths, event_type, schedules):
        probes = {}
        for section in subpaths:
            # use beacon not end host, as the end host's long term probe causes nre measurement index issue
            if section[0].name == 'slc-slice-perf.chpc.utah.edu' and\
                section[-1].name == 'slc-slice-beacon1.chpc.utah.edu':
                section = list(reversed(section))
            
            logger.info("assigning tasks to section [{end0}--{end1}]".format(end0 = section[0].name, end1 = section[-1].name))
            
            # should programmatically chose an end by node-service-measurement-eventType constraints
            probe_service = section[0].services[HARDCODED_SERVICETYPE]
            p = re.compile(probe_service.selfRef + '%.*' + event_type + '.*')
            probe_meas_k = filter(lambda x: p.match(x), self.unisrt.measurements['existing'].keys())
            if len(probe_meas_k) != 1:
                logger.info("there are either too few or too many registered measurement(s) on the node can run this test. cannot decide...")
                return None
            probe_meas_k = probe_meas_k[0]
            probe_meas = self.unisrt.measurements['existing'][probe_meas_k]
            
            probe_meas.status = "ON"
            probe_meas.scheduled_times = schedules[frozenset([section[0], section[-1]])]
            probe_meas.collection_schedule = "builtins.scheduled"
            probe_meas.src = section[0].name
            probe_meas.dst = section[-1].name
            
            if event_type == RTT:
                probe_meas.probe_module = "cmd_line_probe"
                probe_meas.command = "ping -c 1 " + SLICE_IP[probe_meas.dst]
                probes[(section[0], section[-1])] = {'meas': probe_meas_k, 'no_look_back': time(), 'tolerance': PING_TOLERANCE}
            elif event_type == BANDWIDTH:
                probe_meas.probe_module = "json_probe"
                probe_meas.command = "iperf3 -p 6001 --get-server-output -c " + SLICE_IP[probe_meas.dst]
                probes[(section[0], section[-1])] = {'meas': probe_meas_k, 'no_look_back': time(), 'tolerance': IPERF_TOLERANCE}
                
            probe_meas.renew_local(probe_meas_k)
            
        self.unisrt.pushRuntime('measurements')
        
        sleep(30)
        
        section_results = {}
        while probes:
            found = []
            for ends, probe in probes.iteritems():
                logger.info("looking for result between {e0} and {e1}".format(e0 = ends[0].name, e1 = ends[1].name))
                
                if 'start time' not in probe:
                    probe['start time'] = time()
                elif time() - probe['start time'] > probe['tolerance']:
                    # expire this probe
                    found.append(ends)
                    continue
                
                symptom, data = self.trigger(event_type, self.unisrt.measurements['existing'][probe['meas']], INPATIANT_TOLERANCE, probe['no_look_back'])
                section_results[ends] = symptom, data
                if not symptom:
                    # since it is an impatient trigger, symptoms could be false, so we keep the probe
                    # until the end. not completely safe, imagine: probe returns some time-sensitive
                    # data and an actual symptom. beacon may keep trying during the tolerated time, and
                    # later trials may make wrong decisions about the data
                    found.append(ends)
            
            map(lambda x: probes.pop(x), found)
                
        return section_results
        
    def recover(self, pair, subpaths, symptom):
        '''
        for each problematic path, it investigate the sections, analyze the results, apply some (not yet implemented)
        decision making intelligence and may recover the path with a backup one, followed by a performance verification
        '''
        def get_diagnose_paths(subpaths):
            diagnose_paths = []
            for section in subpaths:
                path_name = '%'.join([section[0].selfRef, section[-1].selfRef])
                if path_name in self.unisrt.paths['existing']:
                    path = self.unisrt.paths['existing'][path_name][0]# arbitrarily pick 0
                    path.status = 'ON'
                    path.healthiness = 'unknown'
                    path.renew_local(path_name)
                else:
                    tmp = list(section)
                    data = {
                        "directed": True,
                        "$schema": "http://unis.crest.iu.edu/schema/20151104/path#",
                        "src": tmp.pop(0).selfRef,
                        "hops": [],
                        "dst": tmp.pop(-1).selfRef,
                        "healthiness": "unknown",
                        "performance": "unknown",
                        "status": "ON",
                    }
                    for item in tmp:
                        data['hops'].append({'href': item.selfRef, 'rel': 'full'})
                    path = models.path(data, self.unisrt, True)
                diagnose_paths.append(path)
                
            self.unisrt.pushRuntime('paths')
            
            
            print "!!! map refreshed: diagnose paths assigned (no results yet)"
            
            
            return diagnose_paths
        
        
        logger.info("phase 1. investigating the problematic path...")
        diagnose_paths = get_diagnose_paths(subpaths)
        # modify the slice topology to inject probes
        self.configOF(diagnose_paths)
        schedule_params = {'duration':50, 'num_to_schedule':1, 'every':2}
        schedules = self.querySchedule(subpaths, schedule_params)
        section_performances = self.investigate(subpaths, RTT, schedules)
        
        logger.info("phase 2. analyzing the result...")
        self.analyze(symptom, section_performances)
        
        logger.info("phase 3. enabling a back up path regardless...")
        key = '%'.join(map(lambda endhost: endhost.selfRef, pair))
        old_path = filter(lambda x: x.status == 'ON', self.unisrt.paths['existing'][key])
        new_path = filter(lambda x: x.status == 'OFF', self.unisrt.paths['existing'][key])
        assert len(old_path) == 1
        old_path[0].status = 'OFF'
        new_path[0].status = 'ON' # a random pick path0 from all available paths of this pair
        old_path[0].renew_local(key)
        new_path[0].renew_local(key)
        self.unisrt.pushRuntime('paths')
        
        
        print "!!! map refreshed: a new main path emerged, and diagnose paths are gone"
        
        
        logger.info("phase 4. verifying the performance of the new path...")
        subpaths = [list(pair)]
        self.configOF([new_path[0]])
        schedule_params = {'duration':1, 'num_to_schedule':1, 'every':0}
        schedules = self.querySchedule(subpaths, schedule_params)
        
        new_path_performances = self.investigate(subpaths, BANDWIDTH, schedules)
        assert len(new_path_performances) == 1 # because it measures the whole path
        new_path = filter(lambda x: x.status == 'ON', self.unisrt.paths['existing'][key])
        new_path[0].performance = new_path_performances.values()[0][1]
        new_path[0].healthiness = 'good' # criteria for good?
        new_path[0].renew_local(key)
        self.unisrt.pushRuntime('paths')
        
        logger.info("phase 5. adding pair {src} and {dst} back to monitoring...".format(src = pair[0].name, dst = pair[1].name))
        
        
        
        
        print "!!! map refreshed: new main path turns in green (and hover to see its performance)"
        
        
        
        self.pairs.append(pair)
    
    def loop(self):
        last_check = {}
        patients = {}
        measurements = {}
        while True:
            # step 1. examine each monitored path on their own trigger condition
            for index, pair in enumerate(self.pairs):
                event_type = self.events[index]
                if pair not in measurements:
                    measurement = filter(lambda measurement: measurement.src == pair[0].name and\
                                         measurement.dst == pair[1].name and\
                                         event_type in measurement.eventTypes, self.unisrt.measurements['existing'].values())
                    if len(measurement) == 0:
                        logger.info("The event type {et} between source-destination pair {src} and {dst} is not found. Not monitoring them...".format(et = event_type, src = pair[0].name, dst = pair[1].name))
                        sleep(INTERVAL)
                        continue
                    elif len(measurements) > 1:
                        logger.info("The same event type {et} between source-destination pair {src} and {dst} is found in multiple measurement instances. Which to use?".format(et = event_type, src = pair[0].name, dst = pair[1].name))
                        sleep(INTERVAL)
                        continue
                    else:
                        measurements[pair] = measurement[0]
                
                
                
                
                
                
                # check triggers and update path states accordingly
                symptom, data = self.trigger(event_type, measurements[pair], PING_TOLERANCE, no_look_back=last_check.get(pair, 0))
                last_check[pair] = time()
                if symptom:
                    path_name = '%'.join([pair[0].selfRef, pair[1].selfRef])
                    path = filter(lambda x: x.status == 'ON', self.unisrt.paths['existing'][path_name])
                    assert len(path) == 1
                    path = path[0]
                    path.healthiness = 'bad'
                    path.renew_local(path_name)
                    self.unisrt.pushRuntime('paths')
                    
                    
                    
                    print "!!! map refreshed: main path turning yellow"
                    
                    
                    
                    patients[pair] = symptom, data
                    del self.pairs[index]
                else:
                    logger.info("no symptom discovered for source-destination pair {src} and {dst}".format(src = pair[0].name, dst = pair[1].name))
        
            # step 2. spawn recovery function on each problematic path
            if patients:
                # decompose the paths to BLiPP sections
                decomp_paths = self.querySubPath(patients.keys())
                
                for pair, path in decomp_paths.items():
                    # try to recover each problematic path
                    threading.Thread(name=pair, target=self.recover, args=(pair, path, patients[pair], )).start()
                    
                patients.clear()
                    
            sleep(INTERVAL)
            
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