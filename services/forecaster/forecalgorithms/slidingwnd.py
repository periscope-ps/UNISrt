import time

WNDSIZE = 3600
def calc(unisrt, meas_obj, meta_obj, future, tolerance):
    '''
    calculate the next time point value by a sliding window average
    '''
    data = unisrt.poke_data(str(meta_obj.id) + '?ts=gt=' + str(int(time.time() - WNDSIZE)) + '000000')
    
    start = time.time()
    while time.time() - start < tolerance and len(data) == 0:
        data = unisrt.poke_data(str(meta_obj.id) + '?ts=gt=' + str(int(time.time() - WNDSIZE)) + '000000')
        time.sleep(5)
        
    if len(data) == 0:
        # metadata exists means the probe has been ran. no data within the window means some traffic problem
        # assume the probe agent is running correctly
        return {}
    else:
        count = 0
        addup = 0.0
        for value in data:
            count += 1
            val_str = value['value']
            val_list = val_str.split()
            val_flot = float(val_list[0]) * (val_list[1] == 'Mbits/sec' and 1000 or 1e+6) # unified to Kbps
            addup += val_flot
        
        return {'value': str(addup / count) + ' Kbits/sec',\
                'ts': (time.time() + meas_obj.every) * 1e+6} # roughly the next "every" time point