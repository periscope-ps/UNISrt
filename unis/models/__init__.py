import os
import sys

from unis import settings

from unis.models.models import _SchemaCache
schemaLoader = _SchemaCache()
for name, schema in settings.SCHEMAS.items():
    setattr(sys.modules[__name__], name, schemaLoader.get_class(schema))
