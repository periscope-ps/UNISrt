import sys, yaml, os

from urllib.parse import urlparse

from mundus import settings
from mundus.models.cache import _CACHE, _cache
from mundus.models.models import get_class, AbstractObject, DictObjectList, Entity

def _prepare_cache():
    [_cache(r) for r in {"http://json-schema.org/draft-07/schema#": 'draft7-schema',
                         "http://json-schema.org/draft-07/hyper-schema#": 'draft7-hyper-schema',
                         "http://json-schema.org/draft-07/links#": 'draft7-links'}.keys()]
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
    for m in archive.get('cache', []):
        _cache(*_factory_args(m))
    _prepare_cache()
    result = []
    for m in archive.get('models', []):
        result.append(load_model(m))
    return result

def load_model(uri):
    uri, isfile = _factory_args(uri)
    model = get_class(uri, is_file=isfile)
    setattr(sys.modules[__name__], model.__name__, model)
    return model

if settings.SCHEMA_PRELOAD_ARCHIVE is not None:
    load_model_archive(settings.SCHEMA_PRELOAD_ARCHIVE)
_prepare_cache()
