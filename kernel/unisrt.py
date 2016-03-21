import threading
import validictory
from time import time
from copy import deepcopy
from graph_tool.all import Graph
from websocket import create_connection

import unis_client
import settings as nre_settings
from schema_cache import SchemaCache
from models import *
from libnre.utils import *

logger = nre_settings.get_logger('unisrt')

# map name strings to class objects
resources_classes = {
    "domains": domain,
    "nodes": node,
    "ports": port,
    "ipports": ipport,
    "links": link,
    "services": service,
    "paths": path,
    "measurements": measurement,
    "metadata": metadata
}

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
        add_defaults(data, schema)
    
    def __init__(self):
        logger.info("starting UNIS Network Runtime Environment...")
        fconf = get_file_config(nre_settings.CONFIGFILE)
        self.conf = deepcopy(nre_settings.STANDALONE_DEFAULTS)
        merge_dicts(self.conf, fconf)
        
        self.unis_url = str(self.conf['properties']['configurations']['unis_url'])
        self.ms_url = str(self.conf['properties']['configurations']['ms_url'])
        self._unis = unis_client.UNISInstance(self.conf)
        self.time_origin = int(time())
        
        self._schemas = SchemaCache()
        self._resources = self.conf['resources']
        
        for resource in self._resources:
            setattr(self, resource, {'new': {}, 'existing': {}})
        
        # construct the hierarchical representation of the network
        for resource in self._resources:
            # only pullRuntime once at the beginning, as pubsub will only update
            # them later when resources are modified on the server
            self.pullRuntime(self._unis.get(resource), resource, False)
            threading.Thread(name=resource, target=self.subscribeRuntime, args=(resource,)).start()
        
        # construct the graph representation of the network, of which this NRE is in charge
        self.g = Graph()
        self.nodebook = {}
        for key in self.nodes['existing'].keys():
            self.nodebook[key] = self.g.add_vertex()
        
        for key, link in self.links['existing'].iteritems():
            if hasattr(link, 'src') and hasattr(link, 'dst'):
                self.g.add_edge(self.nodebook[link.src.node.selfRef],\
                                self.nodebook[link.dst.node.selfRef], add_missing=False)
        
    def pullRuntime(self, data, resource_name, localnew):
        '''
        this function should convert the input data into Python runtime objects
        '''
        model = resources_classes[resource_name]
        
        # sorting: in unisrt res dictionaries, a newer record of same index will be saved
        if data and isinstance(data, list):
            data.sort(key=lambda x: x.get('ts', 0), reverse=False)
            for v in data:
                model(v, self, localnew)

    def pushRuntime(self, resource_name):
        '''
        this function upload specified resource to UNIS
        '''
        def pushEntry(k, entry):
            # TODO: use attribute "ts" to indicate an object downloaded from UNIS
            # for this sort of objects, only update their values.
            # this requires to remove "id" from all local created objects, not done yet
            if hasattr(entry, 'ts'):
                url = '/' + resource_name + '/' + getattr(entry, 'id')
                data = entry.prep_schema()
                self._unis.put(url, data)
                
                # the put() function may not do pubsub, so change to existing manually
#                if k in getattr(self, resource_name)['existing'] and isinstance(getattr(self, resource_name)['existing'][k], list):
#                    getattr(self, resource_name)['existing'][k].append(entry)
#                else:
#                    getattr(self, resource_name)['existing'][k] = entry
                
            else:
                url = '/' + resource_name
                data = entry.prep_schema()
                ret = self._unis.post(url, data)
                
        while True:
            try:
                key, value = getattr(self, resource_name)['new'].popitem()
                
                if not isinstance(value, list):
                    pushEntry(key, value)
                else:
                    for item in value:
                        pushEntry(key, item)
                    
            except KeyError:
                return
    
    def subscribeRuntime(self, resource_name):
        '''
        subscribe a channel(resource) to UNIS, and listen for any new updates on that channel
        '''
        #name = resources_subscription[resource_name]
        name = resource_name
        model = resources_classes[resource_name]
        url = self.unis_url.replace('http', 'ws', 1)
        url = url + '/subscribe/' + name
        ws = create_connection(url)
        data = ws.recv()
        while data:
            model(json.loads(data), self, False)
            data = ws.recv()
        ws.close()

    def poke_data(self, query):
        '''
        try to address this issue:
        - ms stores lots of data, and may be separated from unis
        - this data is accessible via /data url. They shouldn't be kept on runtime environment (too much)
        - however, sometimes they may be needed. e.g. HELM schedules traceroute measurement, and needs the
          results to schedule following iperf tests
        '''
        return self._unis.get('/data/' + query)
    
    def post_data(self, data):
        '''
        same as poke_data, the other way around
        '''
        #headers = self._def_headers("data")
        print data
        return self._unis.pc.do_req('post', '/data', data)#, headers)