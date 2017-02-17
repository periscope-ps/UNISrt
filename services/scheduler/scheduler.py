#!/usr/bin/env python
import re
import pytz
import datetime, dateutil.parser

import services.scheduler.schedalgorithms.adaptive as adaptive
import services.scheduler.schedalgorithms.graphcoloring as coloring
from services.pathmagnifier.pathmagnifier import *
from libnre.utils import *

BANDWIDTH_EVENTTYPE = "ps:tools:blipp:linux:net:iperf3:bandwidth"
logger = settings.get_logger('scheduler')

class Scheduler(object):
    '''
    It schedules all the newly posted raw measurements (on demand)
    the contention resource is LINK, may expand it in future
    '''
    def __init__(self, unisrt):
        '''
        '''
        self.unisrt = unisrt
        
    def _conflicting_measurements(self, resource_list, now):
        ret = []
        for m in filter(lambda x: x.eventTypes == [BANDWIDTH_EVENTTYPE], self.unisrt.measurements['new'].values()):
            try:
                if(m.scheduled_times[-1]["end"] > now.isoformat() and set(m.resources) & set(resource_list)):
                    # this if statement assume the last scheduled time is always the latest
                    ret.append(m)
            except KeyError:
                pass
    
        return ret

    def _conflicting_time(self, conflicting_measurements):
        '''
        from the given conflicting measurements, this method extracts a sorted list of their scheduled times
        '''
        conflicting_times = []
        for meas in conflicting_measurements:
            conflicting_times.extend(deepcopy(meas.scheduled_times))
        # and convert them to datetime objects
        for tobj in conflicting_times:
            tobj["start"] = dateutil.parser.parse(tobj["start"])
            tobj["end"] = dateutil.parser.parse(tobj["end"])
            
        # sort conflicting intervals by start time
        conflicting_times = sorted(conflicting_times, key = lambda t: t["start"])
        return conflicting_times
        
    def _calcTime(self, res_list, every, duration, num_to_schedule):
        '''
        calculate the schedule
        '''
        now = datetime.datetime.utcnow()
        now += datetime.timedelta(seconds = 300) # there will be certain time gap before blipp reads its schedule
        now = pytz.utc.localize(now)
        
        # note that the current setup (using the "new" list, and update/sync accordingly) only considers
        # the iperf probes within one execution of HELM. Need more work if want to consider already
        # scheduled (a.k.a "existing") iperf probes
        conflicts = self._conflicting_measurements(res_list, now)
                
        # get the conflicting time from conflicts
        conflicting_time = self._conflicting_time(conflicts)
        # build schedule, avoiding all conflicting time slots
        return adaptive.build_basic_schedule(now,
                        datetime.timedelta(seconds = every),
                        datetime.timedelta(seconds = duration),
                        num_to_schedule,
                        conflicting_time)
        
    def schedule(self, measurements, schedule_params):
        '''
        measurements: list of measurement objects to be scheduled
        schedule_params: every, duration, num_tests
        '''
        def add_measuring_task(meas, task_set):
            try:
                task_set[meas] = self.unisrt.paths['existing'][(meas.src, meas.dst)]['main'].get_bottom()
                #task_set[meas] = [meas.src, meas.dst]
                
                
            except KeyError:
                # paths shall return L2 ports at its best understanding, e.g. all ports along link layers,
                # or ports along the network layer or at least the two end ports at the transport layer, however,
                # if the paths object has not been constructed by the pathfinder service, this exception has to
                # make up some on the fly
                task_set[meas] = [filter(lambda p: ('ipv4' in p.data['properties'] and p.data['properties']['ipv4']['address'] == meas.src),\
                                 self.unisrt.ports['existing'].values())[0],\
                             filter(lambda p: ('ipv4' in p.data['properties'] and p.data['properties']['ipv4']['address'] == meas.dst),\
                                    self.unisrt.ports['existing'].values())[0]]
                
            # recursive, cascading add interfered measurements
            for res in task_set[meas]:#self.unisrt.paths['existing'][(meas.src, meas.dst)]['main'].get_bottom():
                for scheduled_meas in res.stressed_measurements:
                    if scheduled_meas not in task_set.keys():
                        add_measuring_task(scheduled_meas, task_set)
        
        new_tasks = {}
        for meas in measurements:
            add_measuring_task(meas, new_tasks)
        
        # register measurements back to their resources
        for meas, resources in new_tasks.iteritems():
            for res in resources:
                res.stressed_measurements.add(meas)
        
        # construct the intersection graph of this run
        ug, vprop_name = coloring.construct_graph(new_tasks)
        
        # start the coloring algorithm
        vprop_order = [None] * ug.num_vertices()
        vprop_degree = ug.degree_property_map('total')
        vprop_marked = ug.new_vertex_property('int')
        bucketSorter = [[] for _ in range(ug.num_vertices())]
        coloring.smallest_last_vertex_ordering(ug, vprop_order, vprop_degree, vprop_marked, bucketSorter)

        vprop_color = ug.new_vertex_property('int')
        coloring.coloring(ug, vprop_order, vprop_color)
        
        '''
        for i, v in enumerate(vprop_order):
            print 'order: ' + str(i) + ' name: ' + vprop_name[v] + ' color: ' + str(vprop_color[v]) + ' degree: ' + str(v.out_degree())
        from graph_tool.draw import graph_draw
        graph_draw(ug, vertex_fill_color = vprop_color)
        '''
        
        # map back to time domain
        now = datetime.datetime.utcnow()
        now += datetime.timedelta(seconds = 600) # estimated safety time gap before BLiPP reads its schedule
        now = pytz.utc.localize(now)
        schedules = {}
        duration = schedule_params['duration']
        vlist = ug.vertices()
        for meas, path in new_tasks.items():
            # TODO: need to make sure the order of the iterators remains consistent
            v = vlist.next()
            offset = duration * vprop_color[v]
            for repeat in range(schedule_params['num_tests']):
                round_offset = repeat * schedule_params['every']
                s = now + datetime.timedelta(seconds = offset) + datetime.timedelta(seconds = round_offset)
                e = s + datetime.timedelta(seconds = duration)
                schedules.setdefault(meas.id, []).append({"start": adaptive.datetime_to_dtstring(s), "end": adaptive.datetime_to_dtstring(e)})
        
        # assign schedules
        for meas in measurements:
            meas.scheduled_times = schedules[meas.id]
        
    def post_measurements(self, paths, schedules, schedule_params, test_flag=False):
        '''
        make and post
        '''
        for pair, path in paths.items():
            probe_service = pair[0].services['ps:tools:blipp']
            p = re.compile(probe_service.selfRef + '%.*' + "ps:tools:blipp:linux:net:iperf:bandwidth" + '.*')
            probe_meas_k = filter(lambda x: p.match(x), self.unisrt.measurements['existing'].keys())
            probe_meas_k = probe_meas_k[0]
            probe_meas = self.unisrt.measurements['existing'][probe_meas_k]
            
            probe_meas.status = "ON"
            probe_meas.scheduled_times = schedules[pair]
            probe_meas.collection_schedule = "builtins.scheduled"
            
            probe_meas.src = pair[0].name
            probe_meas.dst = pair[-1].name
            
            #probe_meas.probe_module = "json_probe"
            #probe_meas.command = "/home/miaozhan/iperf3/bin/iperf3 -p 6001 --get-server-output -c dresci.crest.iu.edu"
            probe_meas.probe_module = "cmd_line_probe"
            probe_meas.command = "iperf -c " + " "
            probe_meas.regex = ",(?P<bandwidth>\\d+)$"
            
            probe_meas.renew_local(probe_meas_k)
            
        self.unisrt.pushRuntime('measurements')

def run(unisrt, kwargs):
    scheduler = Scheduler(unisrt)
    setattr(unisrt, 'scheduler', scheduler)
    
    '''
    scheduler.schedule(*prep_test(1, unisrt))
    scheduler.schedule(*prep_test(2, unisrt))
    scheduler.schedule(*prep_test(3, unisrt))
    scheduler.schedule(*prep_test(4, unisrt))
    '''
    
def prep_test(tmp, unisrt):
        from kernel.models import port, measurement, path
        test_port1 = {
            "$schema": "http://unis.crest.iu.edu/schema/20160630/port#",
            "capacity": -42,
            "name": "eth1:1",
            "selfRef": "http://dev.crest.iu.edu:8889/ports/test_port1",
            "urn": "urn:ogf:network:domain=pcvm1-1.instageni.illinois.edu:node=ibp-105-1.idms-ig-ill.ch-geni-net.instageni.illinois.edu:port=eth1:1",
            "id": "test_port1",
            "nodeRef": "urn:ogf:network:domain=pcvm1-1.instageni.illinois.edu:node=ibp-105-1.idms-ig-ill.ch-geni-net.instageni.illinois.edu",
            "properties": {
                "ipv4": {
                    "type": "ipv4",
                    "address": "10.10.1.1"
                }
            }
        }
        test_port2 = {
            "$schema": "http://unis.crest.iu.edu/schema/20160630/port#",
            "capacity": -42,
            "name": "eth1:1",
            "selfRef": "http://dev.crest.iu.edu:8889/ports/test_port2",
            "urn": "urn:ogf:network:domain=pcvm1-1.instageni.illinois.edu:node=ibp-105-1.idms-ig-ill.ch-geni-net.instageni.illinois.edu:port=eth1:1",
            "id": "test_port2",
            "nodeRef": "urn:ogf:network:domain=pcvm1-1.instageni.illinois.edu:node=ibp-105-1.idms-ig-ill.ch-geni-net.instageni.illinois.edu",
            "properties": {
                "ipv4": {
                    "type": "ipv4",
                    "address": "10.10.1.2"
                }
            }
        }
        test_port3 = {
            "$schema": "http://unis.crest.iu.edu/schema/20160630/port#",
            "capacity": -42,
            "name": "eth1:1",
            "selfRef": "http://dev.crest.iu.edu:8889/ports/test_port3",
            "urn": "urn:ogf:network:domain=pcvm1-1.instageni.illinois.edu:node=ibp-105-1.idms-ig-ill.ch-geni-net.instageni.illinois.edu:port=eth1:1",
            "id": "test_port3",
            "nodeRef": "urn:ogf:network:domain=pcvm1-1.instageni.illinois.edu:node=ibp-105-1.idms-ig-ill.ch-geni-net.instageni.illinois.edu",
            "properties": {
                "ipv4": {
                    "type": "ipv4",
                    "address": "10.10.2.1"
                }
            }
        }
        test_port4 = {
            "$schema": "http://unis.crest.iu.edu/schema/20160630/port#",
            "capacity": -42,
            "name": "eth1:1",
            "selfRef": "http://dev.crest.iu.edu:8889/ports/test_port4",
            "urn": "urn:ogf:network:domain=pcvm1-1.instageni.illinois.edu:node=ibp-105-1.idms-ig-ill.ch-geni-net.instageni.illinois.edu:port=eth1:1",
            "id": "test_port4",
            "nodeRef": "urn:ogf:network:domain=pcvm1-1.instageni.illinois.edu:node=ibp-105-1.idms-ig-ill.ch-geni-net.instageni.illinois.edu",
            "properties": {
                "ipv4": {
                    "type": "ipv4",
                    "address": "10.10.2.2"
                }
            }
        }
        test_port5 = {
            "$schema": "http://unis.crest.iu.edu/schema/20160630/port#",
            "capacity": -42,
            "name": "eth1:1",
            "selfRef": "http://dev.crest.iu.edu:8889/ports/test_port5",
            "urn": "urn:ogf:network:domain=pcvm1-1.instageni.illinois.edu:node=ibp-105-1.idms-ig-ill.ch-geni-net.instageni.illinois.edu:port=eth1:1",
            "id": "test_port5",
            "nodeRef": "urn:ogf:network:domain=pcvm1-1.instageni.illinois.edu:node=ibp-105-1.idms-ig-ill.ch-geni-net.instageni.illinois.edu",
            "properties": {
                "ipv4": {
                    "type": "ipv4",
                    "address": "10.10.1.3"
                }
            }
        }
        test_port6 = {
            "$schema": "http://unis.crest.iu.edu/schema/20160630/port#",
            "capacity": -42,
            "name": "eth1:1",
            "selfRef": "http://dev.crest.iu.edu:8889/ports/test_port6",
            "urn": "urn:ogf:network:domain=pcvm1-1.instageni.illinois.edu:node=ibp-105-1.idms-ig-ill.ch-geni-net.instageni.illinois.edu:port=eth1:1",
            "id": "test_port6",
            "nodeRef": "urn:ogf:network:domain=pcvm1-1.instageni.illinois.edu:node=ibp-105-1.idms-ig-ill.ch-geni-net.instageni.illinois.edu",
            "properties": {
                "ipv4": {
                    "type": "ipv4",
                    "address": "10.10.1.4"
                }
            }
        }
        
        test_measurement1 = {
            "$schema": "http://unis.crest.iu.edu/schema/20160630/measurement#",
            "selfRef": "http://dev.crest.iu.edu:8889/measurements/test_measurement1v",
            "service": "http://dev.crest.iu.edu:8889/services/56db9dc8e779895f5f68fddc",
            "eventTypes": [
                "ps:tools:blipp:linux:net:traceroute:hopip"
            ],
            "configuration": {
                "status": "ON",
                "regex": "^\\s*\\d+.*(?P<hopip>\\(.*\\))",
                "collection_schedule": "builtins.simple",
                "probe_module": "traceroute_probe",
                "src": "10.10.1.1",
                "use_ssl": False,
                "dst": "10.10.1.2",
                "reporting_params": 3,
                "reporting_tolerance": 10,
                "collection_ttl": 1500000,
                "command": "traceroute 72.36.65.65",
                "schedule_params": {
                    "every": 120
                },
                "collection_size": 100000,
                "ms_url": "http://dev.crest.iu.edu:8889",
                "eventTypes": {
                    "hopip": "ps:tools:blipp:linux:net:traceroute:hopip"
                },
                "unis_url": "http://dev.crest.iu.edu:8889",
                "reporting tolerance": 10,
                "name": "traceroute-72.36.65.65"
            },
            "id": "test_measurement1"
        }
        test_measurement2 = {
            "$schema": "http://unis.crest.iu.edu/schema/20160630/measurement#",
            "selfRef": "http://dev.crest.iu.edu:8889/measurements/test_measurement2v",
            "service": "http://dev.crest.iu.edu:8889/services/56db9dc8e779895f5f68fddc",
            "eventTypes": [
                "ps:tools:blipp:linux:net:traceroute:hopip"
            ],
            "configuration": {
                "status": "ON",
                "regex": "^\\s*\\d+.*(?P<hopip>\\(.*\\))",
                "collection_schedule": "builtins.simple",
                "probe_module": "traceroute_probe",
                "src": "10.10.2.1",
                "use_ssl": False,
                "dst": "10.10.2.2",
                "reporting_params": 3,
                "reporting_tolerance": 10,
                "collection_ttl": 1500000,
                "command": "traceroute 72.36.65.65",
                "schedule_params": {
                    "every": 120
                },
                "collection_size": 100000,
                "ms_url": "http://dev.crest.iu.edu:8889",
                "eventTypes": {
                    "hopip": "ps:tools:blipp:linux:net:traceroute:hopip"
                },
                "unis_url": "http://dev.crest.iu.edu:8889",
                "reporting tolerance": 10,
                "name": "traceroute-72.36.65.65"
            },
            "id": "test_measurement2"
        }
        test_measurement3 = {
            "$schema": "http://unis.crest.iu.edu/schema/20160630/measurement#",
            "selfRef": "http://dev.crest.iu.edu:8889/measurements/test_measurement3v",
            "service": "http://dev.crest.iu.edu:8889/services/56db9dc8e779895f5f68fddc",
            "eventTypes": [
                "ps:tools:blipp:linux:net:traceroute:hopip"
            ],
            "configuration": {
                "status": "ON",
                "regex": "^\\s*\\d+.*(?P<hopip>\\(.*\\))",
                "collection_schedule": "builtins.simple",
                "probe_module": "traceroute_probe",
                "src": "10.10.1.3",
                "use_ssl": False,
                "dst": "10.10.1.2",
                "reporting_params": 3,
                "reporting_tolerance": 10,
                "collection_ttl": 1500000,
                "command": "traceroute 72.36.65.65",
                "schedule_params": {
                    "every": 120
                },
                "collection_size": 100000,
                "ms_url": "http://dev.crest.iu.edu:8889",
                "eventTypes": {
                    "hopip": "ps:tools:blipp:linux:net:traceroute:hopip"
                },
                "unis_url": "http://dev.crest.iu.edu:8889",
                "reporting tolerance": 10,
                "name": "traceroute-72.36.65.65"
            },
            "id": "test_measurement3"
        }
        test_measurement4 = {
            "$schema": "http://unis.crest.iu.edu/schema/20160630/measurement#",
            "selfRef": "http://dev.crest.iu.edu:8889/measurements/test_measurement4v",
            "service": "http://dev.crest.iu.edu:8889/services/56db9dc8e779895f5f68fddc",
            "eventTypes": [
                "ps:tools:blipp:linux:net:traceroute:hopip"
            ],
            "configuration": {
                "status": "ON",
                "regex": "^\\s*\\d+.*(?P<hopip>\\(.*\\))",
                "collection_schedule": "builtins.simple",
                "probe_module": "traceroute_probe",
                "src": "10.10.1.4",
                "use_ssl": False,
                "dst": "10.10.1.1",
                "reporting_params": 3,
                "reporting_tolerance": 10,
                "collection_ttl": 1500000,
                "command": "traceroute 72.36.65.65",
                "schedule_params": {
                    "every": 120
                },
                "collection_size": 100000,
                "ms_url": "http://dev.crest.iu.edu:8889",
                "eventTypes": {
                    "hopip": "ps:tools:blipp:linux:net:traceroute:hopip"
                },
                "unis_url": "http://dev.crest.iu.edu:8889",
                "reporting tolerance": 10,
                "name": "traceroute-72.36.65.65"
            },
            "id": "test_measurement4"
        }

        test_path1 = {
            "directed": True,
            "src": "10.10.1.1",
            "selfRef": "http://dev.crest.iu.edu:8889/paths/test_port1",
            "$schema": "http://unis.crest.iu.edu/schema/20160630/path#",
            "dst": "10.10.1.2",
            "healthiness": "good",
            "status": "ON",
            "performance": "unknown",
            "id": "56e0af42e779895f5f9ca088"
        }
        test_path2 = {
            "directed": True,
            "src": "10.10.2.1",
            "selfRef": "http://dev.crest.iu.edu:8889/paths/test_port1",
            "$schema": "http://unis.crest.iu.edu/schema/20160630/path#",
            "dst": "10.10.2.2",
            "healthiness": "good",
            "status": "ON",
            "performance": "unknown",
            "id": "56e0af42e779895f5f9ca088"
        }
        test_path3 = {
            "directed": True,
            "src": "10.10.1.3",
            "selfRef": "http://dev.crest.iu.edu:8889/paths/test_port1",
            "$schema": "http://unis.crest.iu.edu/schema/20160630/path#",
            "dst": "10.10.1.2",
            "healthiness": "good",
            "status": "ON",
            "performance": "unknown",
            "id": "56e0af42e779895f5f9ca088"
        }
        test_path4 = {
            "directed": True,
            "src": "10.10.1.4",
            "selfRef": "http://dev.crest.iu.edu:8889/paths/test_port1",
            "$schema": "http://unis.crest.iu.edu/schema/20160630/path#",
            "dst": "10.10.1.1",
            "healthiness": "good",
            "status": "ON",
            "performance": "unknown",
            "id": "56e0af42e779895f5f9ca088"
        }
        from kernel.models import measurement
        if tmp == 1:
            port(test_port1, unisrt, False)
            port(test_port2, unisrt, False)
            port(test_port3, unisrt, False)
            port(test_port4, unisrt, False)
            port(test_port5, unisrt, False)
            port(test_port6, unisrt, False)
            
            path(test_path1, unisrt, False)
            path(test_path2, unisrt, False)
            path(test_path3, unisrt, False)
            path(test_path4, unisrt, False)
            
            measurements = [measurement(test_measurement1, unisrt, True)]
        elif tmp == 2:
            measurements = [measurement(test_measurement2, unisrt, True)]
        elif tmp == 3:
            measurements = [measurement(test_measurement3, unisrt, True)]
        elif tmp == 4:
            measurements = [measurement(test_measurement4, unisrt, True)]
    
        return [measurements, {'every': 120, 'duration': 10, 'num_tests': 1}]