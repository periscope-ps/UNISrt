#!/usr/bin/env python
from time import sleep, time

from kernel.models import measurement
from libnre.utils import *

BANDWIDTH = "ps:tools:blipp:linux:net:iperf:bandwidth"

logger = settings.get_logger('forecaster')

class Forecaster(object):
    '''
    '''
    
    def __init__(self, unisrt, targets=None):
        '''
        targets: list of ends that need forecasts, None indicates all known end hosts
        note that, it does bandwidth forecasts only at this moment, so only iperf measurements are used
        '''
        self.unisrt = unisrt
        self.targets = dict()
        
        if targets != None:
            self.follow(targets)
        else:
            # TODO: should grab all known end hosts and pair them
            self.follow([])
            
    def follow(self, targets):
        '''
        keeps an eye on the given pairs
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
        
    def forecast(self, pair, tolerance, fa='services.forecaster.forecalgorithm.slidingwnd', future=None):
        '''
        poll probe results from certain measurement, and apply statistical tool
        (user defined forecast algorithm) to predict the requested value
        
        pair: requested source and destination for performance forecasting
        tolerance: how long you want to wait until the forecasted value is generated
        fa: the chosen forecast algorithm
        future: a moment in future, default None indicates the "every" attribute of the measurement raw data
        '''
        # deliberately let the caller obey the rule of follow-and-then-forecast
        if pair not in self.targets:
            return None
        
        meas_obj = self.targets[pair]
        start = time()
        while '%'.join([meas_obj.selfRef, BANDWIDTH]) not in self.unisrt.metadata['existing'] and time() - start < tolerance:
            logger.info("The measurement between {src} and {dst} has not run yet".format(src = pair[0], dst = pair[1]))
            logger.info("waiting for {sec} more seconds...".format(max(0, start + tolerance - time())))
            sleep(5)
        
        remain = max(0, start + tolerance - time())
        if remain == 0:
            return None
        
        fa_module = __import__(fa, fromlist = [fa])
        meta_obj = self.unisrt.metadata['existing']['%'.join([meas_obj.selfRef, BANDWIDTH])]
        return fa_module.calc(self.unisrt, meas_obj, meta_obj, future, remain)
    
if __name__ == '__main__':
    import kernel.unisrt
    unisrt = kernel.unisrt.UNISrt()
    forecaster = Forecaster(unisrt, 'args')