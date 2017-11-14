import atexit
import configparser
import copy
import signal
import sys

from lace.logging import trace
from lace import logging

from unis.services import RuntimeService
from unis import settings
from unis.runtime.oal import ObjectLayer
from unis.models.models import UnisObject

class Runtime(object):
    @property
    def settings(self):
        if not hasattr(self, "_settings"):
            self._settings = copy.deepcopy(settings.DEFAULT_CONFIG)
            hasunis = False
            if settings.CONFIGFILE:
                tys = { "true": True, "false": False }
                tmpConfig = configparser.ConfigParser(allow_no_value=True)
                tmpConfig.read(settings.CONFIGFILE)
                
                for section in tmpConfig.sections():
                    if section.startswith("unis"):
                        if not hasunis:
                            self._settings["unis"] = []
                            hasunis = True
                        self._settings["unis"].append({k:tys.get(v,v) for k,v in tmpConfig.items(section)})
                    else:
                        self.settings[section] = {k:tys.get(v,v) for k,v in tmpConfig.items(section)}
                        
        return self._settings
    
    @trace.debug("Runtime")
    def __init__(self, url=None, defer_update=True, subscribe=None):
        self.log = logging.getLogger()
        self.log.info("Starting Unis network Runtime Environment...")
        
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)
        atexit.register(self.shutdown)
        
        self._services = []
        self.settings["defer_update"] = defer_update
        if isinstance(subscribe, bool):
            self.settings["proxy"]["subscribe"] = subscribe
        
        if url:
            urls = [u if isinstance(u,dict) else { 'url': u, 'verify': False, 'cert': None } for u in list(url)]
            urls[0]["default"] = True
            self.settings["unis"] = urls
        
        self._oal = ObjectLayer(self)
        map(self.addService, self.settings["services"])
        map(lambda x: getattr(self._oal, x).load(), self.settings["preload"])
        
    def __getattr__(self, n):
        if n != "_oal":
            return getattr(self._oal, n)
        return super(Runtime, self).__getattr__(n)
        
    @property
    @trace.info("Runtime")
    def collections(self):
        return [x.name for x in self._oal._cache]
        
    @trace.info("Runtime")
    def find(self, href):
        return self._oal.find(href)
    
    @trace.info("Runtime")
    def insert(self, resource, commit=False, publish_to=None):
        if commit:
            return self._oal.insert(resource).commit(publish_to=publish_to)
        return self._oal.insert(resource)
    
    @trace.info("Runtime")
    def addService(self, service):
        instance = service
        if isinstance(service, str):
            import importlib
            service = service.split(".")
            module = importlib.import_module(service[0])
            for comp in service[1:]:
                module = getattr(module, comp)
            instance = module()
        if isinstance(service, type):
            instance = service()
        if not isinstance(instance, RuntimeService):
            raise ValueError("Service must by of type RuntimeService")
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
        if issubclass(model, UnisObject):
            return model in self._oal
        super(Runtime, self).__contains__(model)
    def __enter__(self):
        return self
    def __exit__(self, type, value, traceback):
        self.shutdown()
