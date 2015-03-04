'''
Created on Oct 2, 2013

@author: mzhang
'''
import time
import validictory
from copy import deepcopy

import settings as UNISrt_settings
import unis_client
from models import *
from libnre.utils import *
from schema_cache import SchemaCache

logger = UNISrt_settings.get_logger('unisrt')

class UNISrt(object):
    '''
    This is the class represents UNIS in local runtime environment (local to the apps).
    All UNIS models defined in the periscope/settings.py will be represented as
    a corresponding item of the 'resources' list in this class.
    At the initialization phase, UNISrt will create an cache of the UNIS db, (and
    will maintain it consistent in a best-effort manner).
    '''
    
    # should move following to methods to utils
    def validate_add_defaults(self, data):
        if "$schema" not in data:
            return None
        schema = self._schemas.get(data["$schema"])
        validictory.validate(data, schema)
        utils.add_defaults(data, schema)
    
    def __init__(self):
        fconf = get_file_config(UNISrt_settings.CONFIGFILE)
        self.conf = deepcopy(UNISrt_settings.STANDALONE_DEFAULTS)
        utils.merge_dicts(self.conf, fconf)
        
        self._unis = unis_client.UNISInstance(self.conf)
        self.unis_url = str(self.conf['properties']['configurations']['unis_url'])
        self.ms_url = str(self.conf['properties']['configurations']['ms_url'])
        
        self._schemas = SchemaCache()
        
        # for now, only deals with the following list of elements
        self.domains = {'new': {}, 'existing': {}}
        self.nodes = {'new': {}, 'existing': {}}
        self.ports = {'new': {}, 'existing': {}}
        self.ipports = {'new': {}, 'existing': {}}
        self.links = {'new': {}, 'existing': {}}
        
        self.paths = {'new': {}, 'existing': {}}
        self.services = {'new': {}, 'existing': {}}
        
        self.measurements = {'new': {}, 'existing': {}}
        
        self.metadata = {'new': {}, 'existing': {}}
        self.data = {'new': {}, 'existing': {}}
        
        # time skew can cause disasters. set to 0 to initialize
        # I now rely on well synchronized client and server clocks in pulling
        self.timestamp = 0
        self.syncRuntime()
        
    def poke_remote(self, query):
        '''
        try to address this issue:
        - ms stores lots of data, and may be separated from unis
        - this data is accessible via /data url. They shouldn't be kept on runtime environment (too much)
        - however, sometimes they may be needed. e.g. HELM schedules traceroute measurement, and needs the
          results to schedule following iperf tests
        '''
        # use unis instance as a temporary solution
        ret = self._unis.get('/data/' + query)
        return ret
            
    def _uploadRuntime(self, element_name):
        '''
        it only upload the "local new born" objects, not the entire runtime environment
        it should only serve as a part of syncRuntime(), because calling this function
        without synchronizing with UNIS may cause inconsistency
        '''        
        while True:
            try:
                pair = getattr(self, element_name)['new'].popitem()
                url = '/' + element_name
                data = pair[1].prep_schema()
                self._unis.post(url, data)
            except KeyError:
                return
            
    def updateRuntime(self, data, model, localnew):
        '''
        this function should convert the input data into Python runtime objects
        data: a dictionary containing network element data
        model: the type of the object e.g. node, port...
        '''
        # sorting: in unisrt res dictionaries, a newer record of same index will be saved
        data.sort(key=lambda x: x.get('ts', 0), reverse=False)
        for v in data:
            model(v, self, localnew)
            
    def syncRuntime(self, resources=[domain, node, port, service, path, measurement, metadata]):
        '''
        synchronize the data with UNIS db:
        "local new born" objects will be uploaded to UNIS first
        then UNIS will be downloaded to local
        
        Note that, it is not an atomic operation, and there are various ways causing
        local-remote inconsistency; also, it assume UNIS does not remove records from
        its DB
        '''
        for element in resources:
            if element is metadata:
                plural = element.__name__
            else:
                plural = element.__name__ + 's'
                
            self._uploadRuntime(plural)
        for element in resources:
            if element is metadata:
                plural = element.__name__
            else:
                plural = element.__name__ + 's'
                
            self.updateRuntime(self._unis.get(plural + '?ts=gt=' + str(self.timestamp)), element, False)
            
        self.timestamp = int(time.time() * 10e5)
        
    def subscribeRuntime(self):
        '''
        someone, maybe the config file specify what model objects need be kept sync by pubsub
        '''
        pass
        
