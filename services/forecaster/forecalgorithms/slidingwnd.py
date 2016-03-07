import time

WNDSIZE = 3600
def calc(unisrt, meas_obj, meta_obj, future, tolerance):
    '''
    calculate the next time point value by a sliding window average
    '''
    data = unisrt.poke_remote(str(meta_obj.id) + '?ts=gt=' + str(int(time.time() - WNDSIZE)) + '000000')
    
    start = time.time()
    while time.time() - start < tolerance and len(data) == 0:
        data = unisrt.poke_remote(str(meta_obj.id) + '?ts=gt=' + str(int(time.time() - WNDSIZE)) + '000000')
        time.sleep(5)
        
    if len(data) == 0:
        # metadata exists means the probe has been ran. no data within the window means some traffic problem
        # assume the probe agent is running correctly
        return 0
    else:
        count = 0
        addup = 0.0
        for value in data:
            count += 1
            addup += float(value['value'])
            
        return addup / count