import atexit
import configparser
import copy
import signal
import sys

from unis.services import RuntimeService
from unis.runtime.oal import ObjectLayer
from unis.utils.pubsub import Events
from unis.models.models import UnisObject
from unis import logging
from unis import settings

class RuntimeMeta(type):
    instances = {}
    def __call__(cls, *args, **kwargs):
        try:
            url = kwargs.get('url', args[0] if len(args) else None)
        except IndexError:
            raise AttributeError("Runtime constructor must contain url as first argument")
        if url not in cls.instances:
            cls.instances[url] = super(RuntimeMeta, cls).__call__(*args, **kwargs)
        return cls.instances[url]

class Runtime(object):
    @property
    def settings(self):
        if not hasattr(self, "_settings"):
            class DefaultDict(dict):
                def get(self, k, default=None):
                    val = super(DefaultDict, self).get(k, default)
                    return val or default
            
            self._settings = DefaultDict(copy.deepcopy(settings.DEFAULT_CONFIG))
            hasunis = False
            if settings.CONFIGFILE:
                tmpConfig = configparser.ConfigParser(allow_no_value=True)
                tmpConfig.read(settings.CONFIGFILE)
                
                for section in tmpConfig.sections():
                    if not section in self._settings:
                        self._settings[section] = {}
                    
                    if section.startswith("unis"):
                        if not hasunis:
                            self._settings["unis"] = []
                            hasunis = True
                        self._settings["unis"].append({})
                    for key, setting in tmpConfig.items(section):
                        if setting == "true":
                            setting = True
                        elif setting == "false":
                            setting = False
                        if section.startswith("unis"):
                            self._settings["unis"][-1][key] = setting
                        else:
                            self._settings[section][key] = setting
                        
        return self._settings
    
    @logging.debug("Runtime")
    def __init__(self, url=None, defer_update=False, subscribe=True, auto_sync=True, inline=False, **kwargs):
        self.log = settings.get_logger()
        self.log.info("Starting Unis network Runtime Environment...")
        
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)
        atexit.register(self.shutdown)
        
        self._services = []
        self.settings["defer_update"] = defer_update
        self.settings["subscribe"] = subscribe
        self.settings["auto_sync"] = auto_sync
        self.settings["inline"] = inline
        if "preload" in kwargs:
            self.settings["preload"] = kwargs["preload"]
        
        if url:
            url = url if isinstance(url, list) else [ url ]
            urls = []
            for unis in url:
                urls.append(unis if isinstance(unis, dict) else { 'url': unis, 'verify': False, 'cert': None })
            self.settings["unis"] = urls

        self._oal = ObjectLayer(self)
        
        for service in self.settings["services"]:
            self.addService(service)
            
        for collection in self.settings["preload"]:
            getattr(self._oal, collection).sync()
        
    def __getattr__(self, n):
        if "_oal" in self.__dict__:
            return getattr(self.__dict__["_oal"], n)
        else:
            raise AttributeError("_oal not found in Runtime")
        
    @property
    @logging.info("Runtime")
    def collections(self):
        return self._oal._cache.values()
        
    @logging.info("Runtime")
    def find(self, href):
        return self._oal.find(href)
    
    @logging.info("Runtime")
    def insert(self, resource, commit=False, publish_to=None):
        result = self._oal.insert(resource)
        if publish_to:
            resource.setSource(publish_to)
        if commit:
            resource.commit()
        return result
    
    @logging.info("Runtime")
    def addService(self, service):
        instance = service
        if isinstance(service, type):
            instance = service()
        if not isinstance(instance, RuntimeService):
            raise ValueError("Service must by of type RuntimeService")
        instance.attach(self)
    
    @logging.info("Runtime")
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
