#!/usr/bin/env python
'''
Created on Sep 27, 2013

@author: mzhang
'''
import pytz
import datetime, dateutil.parser
from copy import deepcopy

from kernel import models
from libnre.utils import *

import schedulers.adaptive
#import schedulers.graphcoloring as coloring
from test.test_support import args_from_interpreter_flags

BANDWIDTH = "ps:tools:blipp:linux:net:iperf:bandwidth"

class Helm(object):
    '''
    Helm is essentially a scheduler.
    It schedules all the newly posted raw measurements
    It uses UNISrt (UNIS runtime API).
    '''
    
    def __init__(self, unisrt):
        #with open(settings.HELMCONF) as f:
        #    self.conf = json.loads(f.read())
        
        self.unisrt = unisrt
        self._lastmtime = 0
        
    def _conflicting_measurements(self, resource_list, now):
        ret = []
        for m in filter(lambda x: x.eventTypes == [BANDWIDTH], self.unisrt.measurements['new'].values()):
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
        return schedulers.adaptive.build_basic_schedule(now,
                        datetime.timedelta(seconds = every),
                        datetime.timedelta(seconds = duration),
                        num_to_schedule,
                        conflicting_time)
        
    def schedule(self, paths=None, decomp_flag=False, test=False):
        '''
        periodically query UNISrt for new HELM-Measurements, and schedule them as conflict-free
        (with existing measurements taken into account?)
        note that, the specified node list may contains node(s) with no BLiPP service
        installed, in which case would be simply ignored
        '''
        if not paths:
            nodes = self.conf.get('nodes', {})
            if nodes:
                for runningOn in nodes:
                    service_index = '.'.join([runningOn, 'blipp'])
                    if not service_index in self.unisrt.services['existing']:
                        print "node " + runningOn + " doesn't have blipp service running on"
                    else:
                        blipps.extend([self.unisrt.services['existing'][service_index]])
            else:
                print "No nodes specified. A full mesh test shall be scheduled"
                for serv_inst in self.unisrt.services['existing'].values():
                    if serv_inst.name == 'blipp':
                        blipps.extend([serv_inst])

            HM = None
            self.unisrt.syncRuntime(resources = [models.service, models.measurement])
            for m in self.unisrt.measurements['existing'].values():
                if m.eventTypes == ["ps:tools:helm"] and m.ts > self._lastmtime:
                    # yes, I only look at the first found record, because it REALLY should use pub/sub
                    HM = m
                    self._lastmtime = m.ts
                    break

            if HM == None:
                return
        
        if not decomp_flag:
            #paths = self._getResourceLists(blipps)
            paths = self.getGENIResourceLists(paths)

        # Decision made: already-existing measurements won't be mapped to vertices, as:
        # 1. you don't re-color them
        # 2. not necessary, essentially you should compare time with the already-existing measurements
        ug, vprop_name = coloring.construct_graph(paths)
        
        vprop_order = [None] * ug.num_vertices()
        vprop_degree = ug.degree_property_map('total')
        vprop_marked = ug.new_vertex_property('int')
        bucketSorter = [[] for _ in range(ug.num_vertices())]
        coloring.smallest_last_vertex_ordering(ug, vprop_order, vprop_degree, vprop_marked, bucketSorter)

        vprop_color = ug.new_vertex_property('int')
        coloring.coloring(ug, vprop_order, vprop_color)
        
        #for i, v in enumerate(vprop_order):
        #    print 'order: ' + str(i) + ' name: ' + vprop_name[v] + ' color: ' + str(vprop_color[v]) + ' degree: ' + str(v.out_degree())
        #from graph_tool.draw import graph_draw
        #graph_draw(ug, vertex_fill_color = vprop_color)

        now = datetime.datetime.utcnow()
        now += datetime.timedelta(seconds = 300) # there will be certain time gap before blipp reads its schedule
        now = pytz.utc.localize(now)
        schedules = {}
        duration = HM.probe['test_duration']
        vlist = ug.vertices()
        for pair, path in paths.items():
            # TODO: investigate the consistent order of the iterators
            v = vlist.next()
            offset = duration * vprop_color[v]
            for repeat in range(HM.num_tests):
                round_offset = repeat * HM.every
                s = now + datetime.timedelta(seconds = offset) + datetime.timedelta(seconds = round_offset)
                e = s + datetime.timedelta(seconds = duration)
                schedules.setdefault(pair, []).append({"start": schedulers.adaptive.datetime_to_dtstring(s), "end": schedulers.adaptive.datetime_to_dtstring(e)})
#                schedules[pair] = self._calcTime(res_list,
#                                      self.conf['probes']['iperf']['probe_defaults']['schedule_params']['every'],
#                                      self.conf['probes']['iperf']['probe_defaults']['schedule_params']['duration'],
#                                      self.conf['probes']['iperf']['probe_defaults']['schedule_params']['num_to_schedule'])

            # make an iperf measurement
            measurement = build_measurement(self.unisrt, pair[0])
            measurement["eventTypes"] = [BANDWIDTH]
            measurement["type"] = "iperf"
            iperf_probe = {
                       "$schema": "http://unis.incntre.iu.edu/schema/tools/iperf",
                       "--client": pair[1] # dst name
            }
            self.unisrt.validate_add_defaults(iperf_probe)
            measurement["configuration"] = iperf_probe
            measurement["configuration"]["name"] = "iperf"
            measurement["configuration"]["collection_size"] = 10000000
            measurement["configuration"]["collection_ttl"] = 1500000
            measurement["configuration"]["collection_schedule"] = "builtins.scheduled"
            measurement["configuration"]["schedule_params"] = HM.probe['schedule_params']
            measurement["configuration"]["reporting_params"] = 1
            measurement["configuration"]["resources"] = path
            measurement["scheduled_times"] = schedules[pair]
            
            self.unisrt.updateRuntime([measurement], models.measurement, True)

        if test:
            self.unisrt.measurements['new'].clear()
        else:
            self.unisrt.syncRuntime(resources = [models.measurement])
            
        return schedules
    
def run(unisrt, args):
    '''
    all nre apps are required to have a run() function as the
    driver of this application
    '''
    helm = Helm(unisrt)
    helm.schedule(args)