import sys
import kernel.settings

from .objects import get_class
for name, schema in kernel.settings.SCHEMAS.items():
    cls = get_class(schema, name)
    setattr(sys.modules[__name__], name, cls)

from . import schemas
from . import factory
from .main import ObjectLayer
