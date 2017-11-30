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

class _Singleton(type):
    instance = None
    def __call__(cls, urls=None, **kwargs):
        instance = instance or super(_Singleton, cls).__new__(cls)
        instance._rt_init(urls or ['http://localhost:8888'], **kwargs)
        return instance
        
class Runtime(metaclass=_Singleton):
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
    def __init__(self):
        self.log = logging.getLogger()
        self.log.info("Starting Unis network Runtime Environment...")
        self._services = []
        
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)
        atexit.register(self.shutdown)
        
        self._oal = ObjectLayer()
        
    def _rt_init(self, urls, **kwargs):
        for k,v in kwargs:
            self.settings[k] = {**self.settings[k], **v} if isinstance(v, dict) else v
        self._oal.addSources(urls)
        list(map(self.addService, self.settings['services']))
        list(map(lambda x: getattr(self._oal, x).load(), self.settings['preload']))
        
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
        if not issubclass(service, RuntimeService):
            raise ValueError("Service must by of type RuntimeService")
        if service not in self._services:
            self._services.append(service)
            instance.attach()
    
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
