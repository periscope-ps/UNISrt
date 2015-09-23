'''
Created on Mar 2, 2015

@author: mzhang
'''

import calendar, time

def alarm(unisrt, measurement, metadata):
    '''
    function alarm should be implemented in all probe alarm modules e.g. pingalarm
    it applies specific analysis accordingly to the input probe data, and return
    a fault object or null
    '''
    data = []
    while len(data) < 2:
        data = unisrt.poke_remote(metadata.id)
        
    if max(1e6 * calendar.timegm(time.gmtime()) - data[0]['ts'],\
           data[0]['ts'] - data[1]['ts']) > 1e7 * measurement.every:
        return object()
    else:
        return None