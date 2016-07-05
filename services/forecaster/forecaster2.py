#!/usr/bin/env python
from kernel.models import measurement, metadata
from libnre.utils import *

logger = settings.get_logger('forecaster2')

class Forecaster2(object):
    '''
    '''
    
    def __init__(self, unisrt):
        '''
        '''
        self.unisrt = unisrt
        
    def data_query_assistant(self):
        pass

    def clustering(self):
        pass
    
    def cdf(self):
        pass
    
    def z_test_single_group_member(self):
        pass
    
    def special_or_fault_seg_locator(self):
        pass
    
def run(unisrt, kwargs):
    forecaster2 = Forecaster2(unisrt)
    setattr(unisrt, 'forecaster2', forecaster2)
    
if __name__ == '__main__':
    import kernel.unisrt
    unisrt = kernel.unisrt.UNISrt()
    forecaster2 = Forecaster2(unisrt, 'args')
    setattr(unisrt, 'forecaster2', forecaster2)