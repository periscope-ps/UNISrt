import sys
import runtime.settings
import weakref

from .models import schemaLoader
for name, schema in runtime.settings.SCHEMAS.items():
    cls = schemaLoader.get_class(schema, name)
    setattr(sys.modules[__name__], name, cls)

from .main import ObjectLayer

def reference(obj):
    return weakref.ref(obj)()
