import os
import sys

from unis import settings

from unis.models.models import _SchemaCache
schemaLoader = _SchemaCache()

# Ensure cache includes hyper-schema
schemaLoader.get_class("http://json-schema.org/draft-04/schema#", 'draft4-schema', True)
schemaLoader.get_class("http://json-schema.org/draft-04/hyper-schema#", 'draft4-hyper-schema', True)
schemaLoader.get_class("http://json-schema.org/draft-04/links#", 'draft4-links', True)

for name, schema in settings.SCHEMAS.items():
    setattr(sys.modules[__name__], name, schemaLoader.get_class(schema))
