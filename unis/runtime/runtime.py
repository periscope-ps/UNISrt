import asyncio
import atexit
import configparser
import copy
import signal
import sys

from functools import reduce
from lace.logging import trace
from lace import logging

from unis import settings
from unis.services import RuntimeService
from unis.runtime.oal import ObjectLayer

class Runtime(object):
    def build_settings(self):
        def _ls(v):
            for i, x in enumerate(v):
                try:
                    v[i] = int(x)
                except ValueError:
                    pass
            return v if len(v) > 1 else v[0]

        self.settings = copy.deepcopy(settings.DEFAULT_CONFIG)
        hasunis = self.settings.get('unis', False)
        if settings.CONFIGFILE:
            tys = { "true": True, "false": False, "none": None, "": None }
            tmpConfig = configparser.ConfigParser(allow_no_value=True)
            tmpConfig.read(settings.CONFIGFILE)
            
            for section in tmpConfig.sections():
                if section.startswith("unis"):
                    if not hasunis:
                        self.settings["unis"] = []
                        hasunis = True
                    self.settings["unis"].append({k:tys.get(v,v) for k,v in tmpConfig.items(section)})
                else:
                    self.settings[section] = {k:tys.get(v, _ls(v.split(','))) for k,v in tmpConfig.items(section)}
    
    @trace.debug("Runtime")
    def __init__(self, unis=None, **kwargs):
        def _unis_config(unis):
            if not isinstance(unis, dict):
                unis = { "url": unis, "default": False, "verify": False, "ssl": None, "enabled": True }
            return unis
        
        self.log = logging.getLogger()
        self.log.info("Starting Unis network Runtime Environment...")
        try:
            asyncio.get_event_loop()
        except:
            self.log.warn("No event loop found, creating event loop for runtime")
            asyncio.set_event_loop(asyncio.new_event_loop())
            
        self.build_settings()
        self._services = []
        
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)
        atexit.register(self.shutdown)
        
        if unis:
            unis = unis if isinstance(unis, list) else [unis]
            for new in unis:
                new = _unis_config(new)
                old = next((u for u in self.settings['unis'] if u['url'] == new['url']), None)
                if old:
                    old.update(**new)
                else:
                    self.settings['unis'].append(new)
        elif not self.settings['unis']:
            raise settings.ConfigurationError("Runtime configuration missing default UNIS instance")
        for k,v in kwargs.items():
            self.settings[k] = {**self.settings[k], **v} if isinstance(v, dict) else v 
        
        self.settings['default_source'] = reduce(lambda x,y: y if y['default'] else x, self.settings['unis'])['url']
        self._oal = ObjectLayer(self)
        
        self._oal.addSources(self.settings['unis'])
        [self.addService(s) for s in self.settings['services']]
        self._oal.preload()
        
    def __getattr__(self, n):
        try:
            return super(Runtime, self).__getattribute__(n)
        except AttributeError:
            return getattr(self._oal, n)
        
    @property
    @trace.info("Runtime")
    def collections(self):
        return [x.name for x in self._oal._cache.values()]
        
    @trace.info("Runtime")
    def find(self, href):
        return self._oal.find(href)
    
    @trace.info("Runtime")
    def insert(self, resource, commit=False, publish_to=None):
        if commit:
            self._oal.insert(resource).commit(publish_to=publish_to)
            return resource
        return self._oal.insert(resource)
    
    @trace.info("Runtime")
    def addService(self, service):
        instance = service
        if isinstance(service, str):
            import importlib
            path = service.split(".")[0]
            service = importlib.import_module(path[0])
            for comp in path[1:]:
                service = getattr(module, comp)
        if not issubclass(service, RuntimeService):
            raise ValueError("Service must by of type RuntimeService")
        if isinstance(service, type):
            instance = service()
        if type(instance) not in self._services:
            self._services.append(service)
            instance.attach(self)
    
    @trace.info("Runtime")
    def shutdown(self, sig=None, frame=None):
        self.log.info("Tearing down connection to UNIS...")
        if getattr(self, "_oal", None):
            self._oal.shutdown()
            self._oal = None
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        self.log.info("Teardown complete.")
        if sig:
            sys.exit(130)
    def __contains__(self, model):
        from unis.models.models import UnisObject
        if issubclass(model, UnisObject):
            return model in self._oal
        super(Runtime, self).__contains__(model)
    def __enter__(self):
        return self
    def __exit__(self, type, value, traceback):
        self.shutdown()
