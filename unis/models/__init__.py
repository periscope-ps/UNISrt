import os
import sys

from unis.models import settings
from unis.models.models import schemaLoader

try:
    os.makedirs(settings.JSON_SCHEMAS_ROOT)
except OSError as exp:
    pass

for name, schema in settings.SCHEMAS.items():
    cls = schemaLoader.get_class(schema)
    setattr(sys.modules[__name__], name, cls)
