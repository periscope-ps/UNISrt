import sys, yaml, os

from urllib.parse import urlparse

from unis import settings
from unis.models.models import _SchemaCache, _CACHE
schemaLoader = _SchemaCache()

def _prepare_cache():
    [schemaLoader.cache(r) for r,_ in {"http://json-schema.org/draft-07/schema#": 'draft7-schema',
                                       "http://json-schema.org/draft-07/hyper-schema#": 'draft7-hyper-schema',
                                       "http://json-schema.org/draft-07/links#": 'draft7-links'}.items()]
def _factory_args(uri):
    u = urlparse(uri)
    if u.scheme in ['http', 'https']: return (uri, False)
    if u.scheme == 'file': return (u.path, True)
    elif u.scheme == '': return (os.path.expanduser(uri), True)
    else:
        raise ValueError("Unknown model location scheme, must be one of - 'http', 'https', 'file', ''")
    
def load_model_archive(uri):
    with open(os.path.expanduser(uri)) as f:
        archive = yaml.safe_load(f)
    [schemaLoader.cache(*_factory_args(m)) for m in archive.get('cache', [])]
    _prepare_cache()
    return [load_model(m) for m in archive.get('models', [])]

def load_model(uri):
    uri, isfile = _factory_args(uri)
    model = schemaLoader.get_class(uri, None, isfile)
    setattr(sys.modules[__name__], model.__name__, model)
    return model

if settings.SCHEMA_PRELOAD_ARCHIVE is not None:
    load_model_archive(settings.SCHEMA_PRELOAD_ARCHIVE)
_prepare_cache()
