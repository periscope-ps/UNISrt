import sys

#import kernel.settings
import kernel.psobject.objects

from . import schemas
from . import factory
from .main import ObjectLayer
from .objects import UnisObject

#for name, schema in kernel.settings.SCHEMA.items():
#    cls = kernel.psobject.objects.get_class(schema)
#    setattr(sys.modules[__name__], name.title(), cls)
