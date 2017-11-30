import os
import sys

from unis import settings

from unis.models.models import _SchemaCache
schemaLoader = _SchemaCache()

# Ensure cache includes hyper-schema
schemaLoader.get_class("http://json-schema.org/draft-04/schema#", "draft-04-schema")
schemaLoader.get_class("http://json-schema.org/draft-04/hyper-schema#", "draft-04-hyper-schema")
schemaLoader.get_class("http://json-schema.org/draft-04/links#", "draft-04-links")

for name, schema in settings.SCHEMAS.items():
    setattr(sys.modules[__name__], name, schemaLoader.get_class(schema))
