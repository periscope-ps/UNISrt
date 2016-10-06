import sys
import weakref

from unis.models import settings
from unis.models.models import schemaLoader
for name, schema in settings.SCHEMAS.items():
    cls = schemaLoader.get_class(schema)
    setattr(sys.modules[__name__], name, cls)
