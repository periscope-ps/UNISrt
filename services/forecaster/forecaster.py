#!/usr/bin/env python
import re
import threading
import datetime, dateutil, pytz
from time import sleep, time
from requests.exceptions import ConnectionError

from kernel.models import measurement, metadata
from libnre.utils import *

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
            
    def follow(self, targets, followed_events, scheduler="builtins.scheduled", schedule_params={}):
        '''
        Keeps an eye on the given targets, a.k.a. to launch measurement tasks on targets
        '''
        if scheduler == "builtins.scheduled" and not getattr(self.unisrt, 'scheduler', None):
            logger.warn("NRE service scheduler needs to start first. Not following...")
            return []
        
        if not targets:
            # no targets means all targets, obtain all measurements already in this domain
            # TODO: implies all services provide some predict-able measurements -- true for now
            p = re.compile("urn:.*\+idms-ig-ill\+.*") # the slice that we are interested in
            slice_services = filter(lambda x: p.match(x.node.urn), self.unisrt.services['existing'].values())
    
            '''
            same slice name may contain historical objects with same names, try to use the last set of objects...
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
            return self.targets
            
        else:
            new_measurements = []
            for task in targets:
                #src_node = filter(lambda n: n.id == task['src-node'], self.unisrt.domains['existing'][task['src-domain']].nodes)[0]
                src_node = self.unisrt.nodes['existing'][task['src-node']]
                src_blipp = src_node.services['ps:tools:blipp']
                for followed_event in followed_events:
                    meas = build_measurement(self.unisrt, src_blipp.selfRef)
                    meas.update({
                        "eventTypes": get_eventtype_related(followed_event, 'eventtype_l'),
                        "configuration": {
                            "ms_url": self.unisrt.ms_url,
                            "collection_schedule": scheduler,
                            "schedule_params": schedule_params,
                            "reporting_params": 1,
                            "reporting_tolerance": 10,
                            "collection_size":100000,
                            "collection_ttl":1500000,
                            "unis_url": self.unisrt.unis_url,
                            "use_ssl": False,
                            "name": followed_event + "-" + task['dst-addr'],
                            "src": task['src-addr'],
                            "dst": task['dst-addr'],
                            "probe_module": get_eventtype_related(followed_event, 'probe_module'),
                            "command": get_eventtype_related(followed_event, 'command') % task['dst-addr'],
                            "regex": get_eventtype_related(followed_event, 'regex'),
                            "eventTypes": get_eventtype_related(followed_event, 'eventtype_d')
                        }
                    })
                    '''
                    TODO: should check to prevent duplicated measurements, ignored for now
                    if len(filter(lambda m: m.src == pair[0].name and\
                                           m.dst == pair[1].name and\
                                           BANDWIDTH in m.eventTypes, self.unisrt.measurements['existing'].values())) > 1:
                        logger.warn("The same event type {et} between source-destination pair {src} and {dst} is found in multiple measurement instances. Which to use?".format(et = BANDWIDTH, src = pair[0].name, dst = pair[1].name))
                    else:
                        measurement(meas, self.unisrt, True)
                    '''
                    # measurements are not pushed until scheduled
                    new_measurements.append(measurement(meas, self.unisrt, None, True))
                
            self.targets.extend([m.id for m in new_measurements])
            return self.unisrt.scheduler.schedule(new_measurements, schedule_params)
    
    def unfollow(self, del_meas):
        for dm in del_meas:
            if dm not in self.unisrt.measurements['existing']:
                return False
        for dm in del_meas:
            tmp = self.unisrt.measurements['existing'][dm]
            setattr(tmp, 'status', 'OFF')
            tmp.renew_local(dm)
            
        self.unisrt.pushRuntime('measurements')
            
        return True
    
    def forecast(self, meas_id, tolerance=60, fa='services.forecaster.forecalgorithms.slidingwnd', future=None, persistent=False):
        '''
        poll probe results from certain measurement, and apply statistical tool
        (user defined forecast algorithm) to predict the requested value
        
        meas_obj: requested measurement for performance forecasting
        tolerance: how long you want to wait until the forecasted value is generated
        fa: the chosen forecast algorithm
        future: a moment in future, default None indicates the "every" attribute of the measurement raw data
        persistent: whether a contiguous forecast should be performed
        
        return: actual data or all the metadata associated with the queried measurement
        '''
        def next(meas_obj, tolerance, fa, future):
            start = time()
            
            tmp = list(meas_obj.eventTypes)
            while tmp:
                map(lambda et: tmp.remove(et), meas_obj.metadata.keys())
                sleep(5)
            
            '''
            while '%'.join([meas_obj.selfRef, BANDWIDTH]) not in self.unisrt.metadata['existing'] and time() - start < tolerance:
                logger.info("The measurement has not run yet")
                logger.info("waiting for {sec} more seconds...".format(max(0, start + tolerance - time())))
                sleep(5)
            '''
            
            remain = max(0, start + tolerance - time())
            if remain == 0:
                return None
        
            ret = {}
            fa_module = __import__(fa, fromlist = [fa])
            for et, md in meas_obj.metadata.iteritems():
                while not md['historical']:
                    sleep(5)
                # to keep a unified return format
                ret[et] = {'forecasted': fa_module.calc(self.unisrt, meas_obj, md['historical'], future, remain)} 
            
            return ret
        
        def collect(meas_obj, tolerance, fa, future):
            if meas_obj.scheduled_times:
                # hold until measurement starts the very first scheduled activity
                while pytz.utc.localize(datetime.datetime.utcnow()) <\
                    dateutil.parser.parse(meas_obj.scheduled_times[0]['start']):
                    sleep(5)
                
            while True:
                next_vals = next(meas_obj, tolerance, fa, future)
                
                for et, md in meas_obj.metadata.iteritems():
                    post_data = {"mid": md['forecasted'].id,\
                                 "data": [next_vals[et]['forecasted']]}
                
                    try:
                        ms_ret = self.unisrt.post_data(post_data)
                        print json.dumps(post_data)
                    except ConnectionError:
                        logger.warning("cannot reach MS!")
                        return
                
                sleep(meas_obj.every)
        
        
        # deliberately let the caller obey the rule of follow-and-then-forecast
        if meas_id not in self.targets:
            return None
        
        tmp = []
        while len(tmp) != 1:
            sleep(5)
            tmp = filter(lambda m: m.id == meas_id, self.unisrt.measurements['existing'].values())
            
        meas_obj = filter(lambda m: m.id == meas_id, self.unisrt.measurements['existing'].values())[0]
        
        if persistent == True:
            # if persistent, no actual value is returned, instead the metadata of
            # the constant running measurement is returned for reference
            if hasattr(meas_obj, 'metadata'):
                return meas_obj.metadata

            for eventType in meas_obj.eventTypes:
                data = build_metadata(self.unisrt, meas_obj, eventType, isforecasted=True)
                metadata(data, self.unisrt, localnew=True)
            
            # TODO: ignore for today, Friday!!!!!!
            self.unisrt.pushRuntime('metadata')
            
            for eventType in meas_obj.eventTypes:
                threading.Thread(name=meas_obj.id, target=collect, args=(meas_obj, tolerance, fa, future,)).start()
                
            return meas_obj.metadata
        
        else:
            return next(meas_obj, tolerance, fa, future)

def run(unisrt, kwargs):
    forecaster = Forecaster(unisrt)
    setattr(unisrt, 'forecaster', forecaster)
    
if __name__ == '__main__':
    import kernel.unisrt
    unisrt = kernel.unisrt.UNISrt()
    forecaster = Forecaster(unisrt, 'args')
    setattr(unisrt, 'forecaster', forecaster)