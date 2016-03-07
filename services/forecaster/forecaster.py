#!/usr/bin/env python
import re
from time import sleep, time
from netaddr import IPNetwork, IPAddress

from kernel.models import measurement
from libnre.utils import *

BANDWIDTH = "ps:tools:blipp:linux:net:iperf:bandwidth"
BLIPPSERVICE = 'ps:tools:blipp'

logger = settings.get_logger('forecaster')

class Forecaster(object):
    '''
    '''
    
    def __init__(self, unisrt):
        '''
        targets: list of measurements been launched/tracked, None indicates all known end hosts
        note that, it only does *bandwidth* forecasts at this moment, so only iperf measurements are used
        '''
        self.unisrt = unisrt
        self.targets = list()
            
    def follow(self, targets=None):
        '''
        keeps an eye on the given pairs
        '''
        if not targets:
            # no targets means all targets, get all services on this domain
            # TODO: implies all services provide some predict-able measurements -- true for now
            p = re.compile("urn:.*\+idms-ig-ill\+.*") # the slice that we are interested in
            slice_services = filter(lambda x: p.match(x.node.urn), self.unisrt.services['existing'].values())
    
            # same slice name may contain historical objects with same names, try to use the last set of objects...
            '''
            from cluster import HierarchicalClustering
            data = {n.data['ts']: n for n in slice_services}
            hc = HierarchicalClustering(data.keys(), lambda x,y: abs(x-y))
            clsts = hc.getlevel(100000000)
            big_value = 0
            big_index = 0
            for i, cl in enumerate(clsts):
                if cl[0] > big_value:
                    big_value = cl[0]
                    big_index = i
            tss = clsts[big_index]
            services = filter(lambda n: n.data['ts'] in tss, slice_services)
            '''
            
            # filter measurements on their services and extend targets
            self.targets.extend(filter(lambda m: m.service in slice_services, self.unisrt.measurements['existing'].values()))
            
        '''                        
        for pair in targets:
            iperf_probe = filter(lambda m: m.src == pair[0].name and\
                                           m.dst == pair[1].name and\
                                           BANDWIDTH in m.eventTypes, self.unisrt.measurements['existing'].values())
            
            if len(iperf_probe) == 0:
                bandwidth_meas = build_measurement(self.unisrt, pair[0].services[BANDWIDTH])
                bandwidth_meas['eventTypes'] = BANDWIDTH
                bandwidth_meas['probe_module'] = "json_probe"
                bandwidth_meas['command'] = "iperf3 -p 6001 --get-server-output -c " + pair[1].name
                
                # TODO: follow() is a long term, repeating function, it must be scheduled, and forecast()
                # must be aware of the schedules when query data
                # bandwidth_meas['scheduled_times'] = self.unisrt.scheduler.schedule['path between this pair']
                
                iperf_probe = measurement(bandwidth_meas, self.unisrt, True)
            
            elif len(iperf_probe) > 1:
                # TODO: need a proper error handling
                logger.warn("The same event type {et} between source-destination pair {src} and {dst} is found in multiple measurement instances. Which to use?".format(et = BANDWIDTH, src = pair[0].name, dst = pair[1].name))
                return None
            
            self.targets[pair] = iperf_probe
            
        self.unisrt.pushRuntime('measurements')
        '''
        
    def forecast(self, meas_obj, tolerance, fa='services.forecaster.forecalgorithms.slidingwnd', future=None):
        '''
        poll probe results from certain measurement, and apply statistical tool
        (user defined forecast algorithm) to predict the requested value
        
        meas_obj: requested measurement for performance forecasting
        tolerance: how long you want to wait until the forecasted value is generated
        fa: the chosen forecast algorithm
        future: a moment in future, default None indicates the "every" attribute of the measurement raw data
        '''
        # deliberately let the caller obey the rule of follow-and-then-forecast
        if meas_obj not in self.targets:
            return None
        
        start = time()
        while '%'.join([meas_obj.selfRef, BANDWIDTH]) not in self.unisrt.metadata['existing'] and time() - start < tolerance:
            logger.info("The measurement has not run yet")
            logger.info("waiting for {sec} more seconds...".format(max(0, start + tolerance - time())))
            sleep(5)
        
        remain = max(0, start + tolerance - time())
        if remain == 0:
            return None
        
        fa_module = __import__(fa, fromlist = [fa])
        meta_obj = self.unisrt.metadata['existing']['%'.join([meas_obj.selfRef, BANDWIDTH])]
        return fa_module.calc(self.unisrt, meas_obj, meta_obj, future, remain)

def run(unisrt, kwargs):
    forecaster = Forecaster(unisrt)
    setattr(unisrt, 'forecaster', forecaster)
    targets = kwargs.get('targets', None)
    forecaster.follow(targets)
    
if __name__ == '__main__':
    import kernel.unisrt
    unisrt = kernel.unisrt.UNISrt()
    forecaster = Forecaster(unisrt, 'args')