'''
Created on Mar 2, 2015

@author: mzhang
'''
import calendar, time

def complain(data):
    return ("no ping returned, connectivity is down!", data)

def alarm(unisrt, measurement, metadata, no_look_back, tolerance):
    '''
    function alarm() and complain() should be implemented in all probe alarm modules e.g. pingalarm
    it applies specific analysis accordingly to the input probe data, and return a fault object or
    None, together with data
    '''
#    return "no ping returned, connectivity is down!"
    data = unisrt.poke_remote(str(metadata.id) + '?ts=gt=' + str(int(no_look_back)) + '000000')
    
    start = time.time()
    while time.time() - start < tolerance and len(data) < 2:
        data = unisrt.poke_remote(str(metadata.id) + '?ts=gt=' + str(int(no_look_back)) + '000000')
        time.sleep(5)
        
    if len(data) < 2:
        return complain(data)
            
    if max(1e6 * calendar.timegm(time.gmtime()) - data[0]['ts'],\
           data[0]['ts'] - data[1]['ts']) > 1e7 * measurement.every:
        return complain(data)
    else:
        return (None, data)