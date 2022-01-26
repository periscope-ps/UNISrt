import asyncio, atexit, signal, sys, copy, configparser, threading

from functools import reduce
from lace.logging import trace
from lace import logging

from unis import settings
from unis.services import RuntimeService
from unis.runtime.oal import ObjectLayer
from unis.exceptions import ConnectionError

@trace("unis")
class Runtime(object):
    """
    :param unis: data store(s) to maintain.
    :param str name: The name of the current runtime for namespacing.
    :param dict runtime: Runtime configuration options.
    :param dict cache: Caching configuration options.
    :param dict proxy: Proxy configuration options.
    :param dict measurements: Measurement configuration options.
    
    The :class:`Runtime <unis.runtime.runtime.Runtime>` object maintains the configuration settings
    for the Unis Runime and acts as the container for the 
    :class:`ObjectAbstractionLayer <unis.runtime.oal.ObjectLayer>`.
    The :class:`Runtime <unis.runtime.runtime.Runtime>` also provides functionality for adding and removing
    :class:`RuntimeServices <unis.services.abstract.RuntimeService>`.
    
    **Configuration options**
    
    * **runtime**
        * **services:** List of service classes as strings to add to the runtime.
    * **cache**
        * **preload:** List of collections as strings to preload on startup.
        * **mode:** (*exponential*) Mode as string detemines how new resources are queried.
        * **growth:** (*2*) Value as integer determines how many new resources are queried per request.
    
    * **proxy**
        * **threads:** (*10*) Number of threads used by proxies.
        * **batch:** (*1000*) Batching size for remote requests.
        * **subscribe:** (*True*) Boolean indicates whether runtime should maintain a subscription to data stores.
        * **defer_update:** (*True*) Boolean switching runtime mode between *deferred mode* and *immediate mode*.
    * **measurements**
        * **read_history:** (*True*) Read in full history of measurements when measurement is added.
        * **subscribe:** (*True*) Subscribe to recieve measurements in realtime.
        * **batch_size:** (*0*) Specifies the number of new measurements to take before pushing to measurement store. (This takes precedence over **batch_until**)
        * **batch_until:** (*0*) Specifies the amount of time to wait for new measurements before pushing to measurement store.
    """
    def _build_settings(self):
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
                    if section not in self.settings:
                        self.settings[section] = {}
                    self.settings[section].update({k:tys.get(v, _ls(v.split(','))) for k,v in tmpConfig.items(section)})
    
    def __init__(self, unis=None, name="default", **kwargs):
        def _unis_config(unis):
            if not isinstance(unis, dict):
                return { "url": unis, "default": False, "verify": False, "ssl": None, "enabled": True }
            else:
                return {
                    "url": unis['url'],
                    "default": unis.get('default', False),
                    "verify": unis.get('verify', False),
                    "ssl": unis.get('ssl', None),
                    "enabled": unis.get('enabled', True)
                }
        
        self.log = logging.getLogger("unisrt")
        self.log.info("Starting Unis network Runtime Environment...")
        try:
            asyncio.get_event_loop()
        except:
            self.log.warn("No event loop found, creating event loop for runtime")
            asyncio.set_event_loop(asyncio.new_event_loop())
            
        self._build_settings()
        if unis:
            unis = unis if isinstance(unis, list) else [unis]
            self.settings['unis'] = [_unis_config(u) for u in unis]
        elif not self.settings['unis']:
            raise settings.ConfigurationError("Runtime configuration missing default UNIS instance")
        self.settings['namespace'] = name
        for k,v in kwargs.items():
            self.settings[k] = {**self.settings[k], **v} if isinstance(v, dict) else v

        default = None
        for u in self.settings['unis']:
            if u['default']:
                default = u['url']
                break
        if not default:
            self.settings['unis'][0]['default'] = True
        self.settings['default_source'] = default or self.settings['unis'][0]['url']
        self._oal = ObjectLayer(self.settings)
        
        try:
            self._oal.addSources(self.settings['unis'])
        
            if threading.current_thread() is threading.main_thread():
                try: signal.signal(signal.SIGINT, self._sig_close)
                except: pass
            atexit.register(self._exit_close)

            [self.addService(s) for s in self.settings['runtime']['services']]
            self._oal._preload()
        except Exception:
            self.shutdown()
            raise

        
    def __getattr__(self, n):
        try:
            return super(Runtime, self).__getattribute__(n)
        except AttributeError:
            if self.__dict__.get('_oal'):
                return getattr(self._oal, n)
            raise
    
    @property
    def collections(self):
        """
        :return: list of strings
        
        Collections lists the names of all collection types maintained by the runtime.
        """
        return [x.name for x in self._oal._cache.values()]
        
    def insert(self, resource, commit=False, publish_to=None, track=False):
        """
        :param resource: Resource to be added to the runtime for tracking.
        :param bool commit: (optional) Indicates whether the resource should be marked for insertion into a remote data store. Default ``False``. **Depreciated in version 1.1**
        :param bool track: (optional) Indicates whether the resource should be marked for insertion into a remote data store. Default ``False``. Alias for commit.
        :param str publish_to: (optional) If commit is ``True``, this indicates which remote data store to commit to.  If not provided, 
        :type resource: :class:`UnisObject <unis.models.models.UnisObject>`
        :return: :class:`UnisObject <unis.models.models.UnisObject>` the default store from the :class:`Runtime <unis.runtime.runtime.Runtime>` settings.
        
        ``insert`` adds an object that inherits from :class:`UnisObject <unis.models.models.UnisObject>` to a :class:`Runtime <unis.runtime.runtime.Runtime>` for tracking.
        This will use the inheritance chain from the underlying json schema describing the object to determine which collection to place the object.
        See :class:`UnisObject <unis.models.models.UnisObject>` for more information on resource typing.
        
        The behavior of ``commit`` depends on the ``defer_update`` setting.  In either mode, setting ``commit`` to True will mark the object for publication
        to a remote data store.  In ``immediate_mode``, the resource will be sent to the data store at this point.  In ``deferred_mode`` this will not take
        place until :meth:`flush <unis.runtime.oal.OAL.flush>` is called.
        """
        if commit or track:
            return self._oal._insert(resource).commit(publish_to=publish_to)
        return self._oal._insert(resource)

    def delete(self, resource):
        """
        :param resource: Resource to be removed from the runtime.

        Removes a resource from runtime tracking and management.

        note:: This operation will invalidate the object, preventing further modifications.
        """
        self._oal._remove(resource)
    
    def addService(self, service):
        """
        :param service: Serivce to be added to the runtime.
        :type service: str or :class:`RuntimeService <unis.services.abstract.RuntimeService>`
        
        ``addService`` takes a string path to a :class:`RuntimeService <unis.services.abstract.RuntimeService>` class,
        a :class:`RuntimeService <unis.services.abstract.RuntimeService>` class or object.  In the case that the ``service``
        is a string or class, a service object will be generated with no parameters.
        """
        instance = service
        if isinstance(service, str):
            import importlib
            path = service.split(".")
            module = importlib.import_module(".".join(path[:-1]))
            service = getattr(module, path[-1])
        if isinstance(service, type):
            if not issubclass(service, RuntimeService):
                raise ValueError("Service type must be of type RuntimeService")
            instance = service()
        if not isinstance(instance, RuntimeService):
            raise ValueError("Service object must be of type RuntimeService - {}".format(type(instance)))
        if type(instance) not in self._oal._services:
            self._oal._services.append(service)
            instance.setRuntime(self)
            instance.initialize()
            for target in instance.targets:
                instance.attach(self._oal._cache(target))

    def _sig_close(self, sig=None, frame=None):
        self.shutdown()
        raise KeyboardInterrupt
    def _exit_close(self):
        self.shutdown()
        
    def shutdown(self, sig=None, frame=None):
        """
        :param sig: (optional) This param is required for internal use and should not be used.
        :param frame: (optional) This param is required for internal use and should not be used.
        :return: None
        
        Shutdown the runtime, removing connections from remote instances.
        """
        self.log.info("Tearing down connection to UNIS...")
        if threading.current_thread() is threading.main_thread(): signal.signal(signal.SIGTERM, signal.SIG_DFL)
        if self.__dict__.get('_oal'):
            self._oal.shutdown()
        self.log.info("Teardown complete.")
    def __contains__(self, model):
        from unis.models.models import UnisObject
        if issubclass(model, UnisObject):
            return model in self._oal
        super(Runtime, self).__contains__(model)
    def __enter__(self):
        return self
    def __exit__(self, type, value, traceback):
        self.shutdown()
