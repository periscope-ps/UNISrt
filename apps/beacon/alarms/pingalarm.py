'''
Created on Mar 2, 2015

@author: mzhang
'''

import calendar, time

def alarm(measurement, data):
    '''
    function alarm should be implemented in all probe alarm modules e.g. pingalarm
    it applies specific analysis accordingly to the input probe data, and return
    a fault object or null
    '''
    print data
    if max(1e6 * calendar.timegm(time.gmtime()) - data[0]['ts'],\
           data[0]['ts'] - data[1]['ts']) > 1e7 * measurement.every:
        return object()
    else:
        return None