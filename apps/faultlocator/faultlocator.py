'''
write this application in my language?
'''
import re
import sys
import threading
from time import sleep, time

import kernel.models as models
from services.forecaster import forecaster
from libnre.utils import *

# temporarily hard code constants
INTERVAL = 60
PING_TOLERANCE = 50
IPERF_TOLERANCE = 90
INPATIANT_TOLERANCE = 1
HARDCODED_SERVICETYPE = "ps:tools:blipp"
RTT = "ps:tools:blipp:linux:net:ping:rtt"
BANDWIDTH = "ps:tools:blipp:linux:net:iperf:bandwidth"
PROBE_CMD = {
    RTT: "ping -c 1 ",
    BANDWIDTH: "iperf3 -p 6001 --get-server-output -c "
}
SLICE_IP = {
    'slc-slice-perf.chpc.utah.edu': '192.168.2.3',
    'slc-slice-beacon.chpc.utah.edu': '192.168.2.4',
    'slc-slice-beacon1.chpc.utah.edu': '192.168.2.5',
    'dresci': '192.168.2.2'
}

logger = settings.get_logger('beacon')

class Faultlocator(object):
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

    def queryBLiPPPath(self, pair):
        '''
        This function determine the hops between source and destination, and
        identify the hops with BLiPP agent installed. Note that this function
        relies on Path objects.
        input:  (node1, node2)
        output: [[node1, <possible middle non-blipp hops>, nodeX], 
                 [nodeX, <possible middle non-blipp hops>, nodeY]...
                 [nodeZ, <possible middle non-blipp hops>, node2]]
        '''
        try:
            sleep(20) # waiting for subscribed channel to update
            backbone_path = filter(lambda x: x.status == 'ON', self.unisrt.paths['existing']['%'.join([pair[0].selfRef, pair[1].selfRef])])
            assert len(backbone_path) == 1
            hops = [pair[0]] + backbone_path[0].hops + [pair[1]]
        
        except KeyError:
            logger.info("the queried path is not found in UNIS")
            
        
        # TODO: to find the optimal agent selection inside a network is non-trivial
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
    
    def investigate(self, pair, subpaths, symptom):
        '''
        for each problematic path, it investigates its sections
        during the investigation, it enable a predefined path to keep the traffic flowing
        '''
        
        def enable_backup_path():
            logger.info("enabling a back up path regardless...")
            key = '%'.join(map(lambda endhost: endhost.selfRef, pair))
            old_path = filter(lambda x: x.status == 'ON', self.unisrt.paths['existing'][key])
            new_path = filter(lambda x: x.status == 'OFF', self.unisrt.paths['existing'][key])
            assert len(old_path) == 1
            old_path[0].status = 'OFF'
            new_path[0].status = 'ON' # a random pick path0 from all available paths of this pair
            old_path[0].renew_local(key)
            new_path[0].renew_local(key)
            self.unisrt.pushRuntime('paths')
            
            logger.info("verifying the performance of the new path...")
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

            logger.info("adding pair {src} and {dst} back to monitoring...".format(src = pair[0].name, dst = pair[1].name))
            self.pairs.append(pair)
            
        def get_diagnose_paths(subpaths):
            '''
            return path objects needed for each BLiPP section
            '''
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
            return diagnose_paths
            
        def analyze(self, path_symp, sec_perf):
            '''
            looking at the symptom of a path and performance data of each of its section
            need some algorithm to tell where and what went wrong along the path
            Note that, a missing section in the input means no data can be collected there
            '''
            logger.info("path symptom is {ps}".format(ps = path_symp))
            # TODO: no real analysis at this point, but to display the diagnose path healthiness (and turn it off after a pause)
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
        
            for sec, perf in sec_perf.iteritems():
                diag_path_name = '%'.join([sec[0].selfRef, sec[-1].selfRef])
                if diag_path_name not in self.unisrt.paths['existing']:
                    diag_path_name = '%'.join([sec[-1].selfRef, sec[0].selfRef])
                diag_path = self.unisrt.paths['existing'][diag_path_name][0]# arbitrarily pick 0
                diag_path.status = 'OFF'
                diag_path.renew_local(diag_path_name)
        
            self.unisrt.pushRuntime('paths')
        
        
#        logger.info("phase 0. enable backup if it is provided...")
#        enable_backup_path()
        
        logger.info("phase 1. collect data on each subpath...")
        path_objs = get_diagnose_paths(subpaths)
        self.unisrt.sdninterpreter.configOF(path_objs)
        schedules = self.unisrt.scheduler.schedule(subpaths)
        
        probes = {}
        for section in subpaths:
            # use beacon not end host, as the end host's long term probe causes nre measurement index issue
            if section[0].name == 'slc-slice-perf.chpc.utah.edu' and\
                section[-1].name == 'slc-slice-beacon1.chpc.utah.edu':
                section = list(reversed(section))
            
            logger.info("assigning tasks to section [{end0}--{end1}]".format(end0 = section[0].name, end1 = section[-1].name))
            
            # should programmatically chose an end by node-service-measurement-eventType constraints
            probe_service = section[0].services[HARDCODED_SERVICETYPE]
            p = re.compile(probe_service.selfRef + '%.*' + BANDWIDTH + '.*')
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
            
            probe_meas.probe_module = "json_probe"
            probe_meas.command = PROBE_CMD[BANDWIDTH] + SLICE_IP[probe_meas.dst]
            probes[(section[0], section[-1])] = {'meas': probe_meas_k, 'no_look_back': time(), 'tolerance': IPERF_TOLERANCE}
                
            probe_meas.renew_local(probe_meas_k)
            
        self.unisrt.pushRuntime('measurements')
        
        sleep(30)
        
        start = time()
        section_performances = {}
        while probes:
            found = []
            for ends, probe in probes.iteritems():
                logger.info("looking for result between {e0} and {e1}".format(e0 = ends[0].name, e1 = ends[1].name))
                
                if 'start time' not in probe:
                    probe['start time'] = time()
                elif time() - probe['start time'] > probe['tolerance']:
                    # expire this probe, without collecting any data
                    found.append(ends)
                    continue
                
                meas_obj = self.unisrt.measurements['existing'][probe['meas']]

                if '%'.join([meas_obj.selfRef, BANDWIDTH]) not in self.unisrt.metadata['existing']:
                    logger.info("The measurement between {src} and {dst} has not run yet".format(src = pair[0], dst = pair[1]))
                    logger.info("waiting for {sec} more seconds...".format(max(0, start + probe['tolerance'] - time())))
                    continue
                
                meta_obj = self.unisrt.metadata['existing']['%'.join([meas_obj.selfRef, BANDWIDTH])]
                data = unisrt.poke_remote(str(meta_obj.id) + '?ts=gt=' + str(int(time() - probe['no_look_back'])) + '000000')
                
                if len(data) == 0:
                    logger.info("measurement between {src} and {dst} returns no data until timeout")
                    logger.info("waiting for {sec} more seconds...".format(max(0, start + probe['tolerance'] - time())))
                    continue
        
                section_performances[ends] = symptom, data
                found.append(ends)
                
            map(lambda x: probes.pop(x), found)
            
        logger.info("phase 2. analyzing the result...")
        analyze(symptom, section_performances)
    
    def loop(self):
        # need the forecaster service
        if self.unisrt.forecastor == None:
            self.unisrt.forecastor = forecaster.Forecaster(unisrt, ['pairs that I care'])
        elif 'pairs that I care' not in self.unisrt.forecastor.targets:
            self.unisrt.forecastor.follow(['pairs that I care'])
        
        patients = {}
        while True:
            # step 1. use the forecasted value as the alarm threshold
            for index, pair in enumerate(self.pairs):
                # TODO: the tolerance parameter actually blocks further execution of other pairs, so
                # this step (step 1) should be parallelized in a compound thread forecast+investigate
                forecasted_value = self.unisrt.forecastor.forecast(pair, 60)
                
                # trigger: if the forecasted value is below the alarm threshold, ring the bell
                alarm_name = self.alarms[BANDWIDTH]
                alarm_module = __import__(alarm_name, fromlist = [alarm_name])
                symptom = alarm_module.lte(forecasted_value, 0)
                
                if symptom:
                    patients[pair] = symptom
                    del self.pairs[index]
                    
                    # an extra job here: change the path state
                    path_name = '%'.join([pair[0].selfRef, pair[1].selfRef])
                    path = filter(lambda x: x.status == 'ON', self.unisrt.paths['existing'][path_name])
                    assert len(path) == 1
                    path = path[0]
                    path.healthiness = 'bad'
                    path.renew_local(path_name)
                    self.unisrt.pushRuntime('paths')
                else:
                    logger.info("no symptom discovered between {src} and {dst}".format(src = pair[0].name, dst = pair[1].name))
        
            # step 2. investigate each problematic path
            for pair, symptom in patients.items():
                patients.pop(pair)
                decomp_path = self.queryBLiPPPath(pair)
                threading.Thread(name=pair, target=self.investigate, args=(pair, decomp_path, symptom, )).start()
                    
            sleep(INTERVAL)
            
def run(unisrt, args):
    '''
    all nre apps are required to have a run() function as the
    driver of this application
    '''
    faultlocator = Faultlocator(unisrt, args)
    faultlocator.loop()
    
if __name__ == '__main__':
    import kernel.unisrt
    unisrt = kernel.unisrt.UNISrt()
    faultlocator = Faultlocator(unisrt, '/home/mzhang/workspace/nre/apps/beacon/beacon.conf')
    faultlocator.loop()