#!/usr/bin/env python
'''
Created on Sep 27, 2013

@author: mzhang
'''
import pytz
import datetime, dateutil.parser

from libnre.utils import *
from libnre.resourcemgmt import *

import schedulers.adaptive
import schedulers.graphcoloring as coloring

BANDWIDTH_EVENTTYPE = "ps:tools:blipp:linux:net:iperf:bandwidth"
BANDWIDTH_TYPE = "iperf"

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
        return schedulers.adaptive.build_basic_schedule(now,
                        datetime.timedelta(seconds = every),
                        datetime.timedelta(seconds = duration),
                        num_to_schedule,
                        conflicting_time)
        
    def schedule(self, paths, schedule_params):
        '''
        input: paths --- schedule them as conflict-free
               schedule_params --- duration, repeat_num, frequency
        '''
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
        duration = schedule_params['duration']
        vlist = ug.vertices()
        for pair, path in paths.items():
            # TODO: investigate the consistent order of the iterators
            v = vlist.next()
            offset = duration * vprop_color[v]
            for repeat in range(schedule_params['num_tests']):
                round_offset = repeat * schedule_params['every']
                s = now + datetime.timedelta(seconds = offset) + datetime.timedelta(seconds = round_offset)
                e = s + datetime.timedelta(seconds = duration)
                schedules.setdefault(pair, []).append({"start": schedulers.adaptive.datetime_to_dtstring(s), "end": schedulers.adaptive.datetime_to_dtstring(e)})
            
        return schedules
    
    def post_measurements(self, paths, schedules, schedule_params, test_flag=False):
        '''
        make and post
        '''
        for pair, path in paths.items():
            measurement = build_measurement(self.unisrt, pair[0])
            measurement["eventTypes"] = [BANDWIDTH_EVENTTYPE]
            measurement["type"] = BANDWIDTH_TYPE
            probe = {
                "$schema": "http://unis.incntre.iu.edu/schema/tools/iperf",
                "--client": pair[1] # dst name
            }
            self.unisrt.validate_add_defaults(probe)
            measurement["configuration"] = probe
            measurement["configuration"]["name"] = "iperf"
            measurement["configuration"]["collection_size"] = 10000000
            measurement["configuration"]["collection_ttl"] = 1500000
            measurement["configuration"]["collection_schedule"] = "builtins.scheduled"
            measurement["configuration"]["schedule_params"] = schedule_params
            measurement["configuration"]["reporting_params"] = 1
            measurement["configuration"]["resources"] = path
            #measurement['configuration']['address'] = pair[1].ip
            measurement['configuration']['source'] = pair[0]
            measurement['configuration']['destination'] = pair[1]
            measurement["scheduled_times"] = schedules[pair]
        
            self.unisrt.updateRuntime([measurement], 'measurements', True)
            
        if test_flag:
            self.unisrt.measurements['new'].clear()
        else:
            self.unisrt.uploadRuntime('measurements')
    
def run(unisrt, args):
    '''
    all nre apps are required to have a run() function as the
    driver of this application
    '''
    pass

def main():
    import kernel.unisrt
    unisrt = kernel.unisrt.UNISrt()
    helm = Helm(unisrt)
    
    with open("/home/mzhang/workspace/nre/apps/helm/helm.conf") as f:
        conf = json.loads(f.read())
    pairs = map(lambda x: map(lambda y: unisrt.nodes['existing'][y].name, x), conf['pairs'])
    
    # TODO: getResourceLists take blipp service as input, whereas getGENIResourceLists takes nodes
    # it is all caused by the decision about which way is better to query the path, and further caused
    # by the association between node objects and service objects. May need to make them bi-directional
    # Here is a snippet to convert node to service, might be useful later         
    # for runningOn in nodes:
    #     service_index = '.'.join([runningOn, 'blipp'])
    #     if not service_index in self.unisrt.services['existing']:
    #         print "node " + runningOn + " doesn't have blipp service running on"
    #     else:
    #         blipps.extend([self.unisrt.services['existing'][service_index]])
    
    #paths = self._getResourceLists(blipps)
    paths = getGENIResourceLists(helm.unisrt, pairs)
    
    schedule_params = {'duration':10, 'num_tests':1, 'every':0}    
    schedules = helm.schedule(paths, schedule_params)
    helm.post_measurements(paths, schedules, schedule_params)
    
if __name__ == '__main__':
    main()