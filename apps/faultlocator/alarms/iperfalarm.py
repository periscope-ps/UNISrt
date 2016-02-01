import time

def complain(data):
    return ("performance is too low!", data)

def alarm(unisrt, measurement, metadata, no_look_back, tolerance):
    '''
    function alarm() and complain() should be implemented in all probe alarm modules e.g. iperfalarm
    it applies specific analysis accordingly to the input probe data, and return a fault object or
    None, together with data
    '''
    
    data = []
    start = time.time()
    while not data and time.time() - start < tolerance:
        data = unisrt.poke_remote(metadata.id)
        time.sleep(5)
        
    if not data:
        return complain(data)
            
    if data[0]['value'] == 0:
        return complain(data)
    else:
        return (None, data)