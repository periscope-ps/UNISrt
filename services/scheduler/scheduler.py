#!/usr/bin/env python
import re
import pytz
import datetime, calendar, dateutil.parser

import services.scheduler.schedalgorithms.adaptive as adaptive
import services.scheduler.schedalgorithms.graphcoloring as coloring
from libnre.utils import *
from services.pathmagnifier.pathmagnifier import *

BANDWIDTH_EVENTTYPE = "ps:tools:blipp:linux:net:iperf:bandwidth"
BANDWIDTH_TYPE = "iperf"

logger = settings.get_logger('scheduler')

class Scheduler(object):
    '''
    It schedules all the newly posted raw measurements (on demand)
    the contention resource is LINK, may expand it in future
    '''
    def __init__(self, unisrt, config_file=None):
        self.unisrt = unisrt
        unisrt.scheduler = self
        
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
        
    def schedule(self, paths, schedule_params):
        '''
        paths: {pair: consumed resources --> only LINK objects for now}
        schedule_params: duration, repeat_num, frequency
        '''
        # TODO: on-demand scheduling is a MUST, if scheduler is a system service
        
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
        now += datetime.timedelta(seconds = 300) # there will be certain time gap before BLiPP reads its schedule
        now = pytz.utc.localize(now)
        schedules = {}
        duration = schedule_params['duration']
        vlist = ug.vertices()
        for pair, path in paths.items():
            # TODO: need to make sure the order of the iterators remains consistent
            v = vlist.next()
            offset = duration * vprop_color[v]
            for repeat in range(schedule_params['num_tests']):
                round_offset = repeat * schedule_params['every']
                s = now + datetime.timedelta(seconds = offset) + datetime.timedelta(seconds = round_offset)
                e = s + datetime.timedelta(seconds = duration)
                
                # TODO: unified solution to bulk scheduling and on demand scheduling
                relative_start_ts = calendar.timegm(s.timetuple()) - self.unisrt.time_origin
                relative_end_ts = calendar.timegm(e.timetuple()) - self.unisrt.time_origin
                for link in path:
                    if link.booking[relative_start_ts : relative_end_ts].allzeros():
                        pass
                    else:
                        logger.warn("contention detected, shift to the next available time slot")
                        
                schedules.setdefault(pair, []).append({"start": adaptive.datetime_to_dtstring(s), "end": adaptive.datetime_to_dtstring(e)})
            
        return schedules
            
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
    
if __name__ == '__main__':
    import kernel.unisrt
    unisrt = kernel.unisrt.UNISrt()
    scheduler = Scheduler(unisrt)
    
    with open("/home/mzhang/workspace/nre/apps/helm/helm.conf") as f:
        conf = json.loads(f.read())
    pairs = map(lambda x: map(lambda y: unisrt.nodes['existing'][y].name, x), conf['pairs'])
    
    # TODO: getResourceLists take BLiPP service as input, whereas getGENIResourceLists takes nodes
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
    paths = getGENIResourceLists(scheduler.unisrt, pairs)
    
    schedule_params = {'duration':10, 'num_tests':10, 'every':3600}    
    schedules = scheduler.schedule(paths, schedule_params)
    scheduler.post_measurements(paths, schedules, schedule_params)