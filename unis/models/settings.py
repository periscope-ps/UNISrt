import os

from unis.settings import UNISRT_ROOT

SCHEMA_HOST        = 'unis.crest.iu.edu'
SCHEMAS_LOCAL      = False

JSON_SCHEMAS_ROOT  = os.path.join(UNISRT_ROOT, "schemas")
SCHEMA_CACHE_DIR   = os.path.join(JSON_SCHEMAS_ROOT, ".cache")

JSON_SCHEMA_SCHEMA = "http://json-schema.org/draft-04/schema#"
JSON_SCHEMA_HYPER  = "http://json-schema.org/draft-04/hyper-schema#"
JSON_SCHEMA_LINKS  = "http://json-schema.org/draft-04/links#"

_schema = "http://{host}/schema/{directory}/{name}"
SCHEMAS = {
    'Manifest':        _schema.format(host = SCHEMA_HOST, directory = "20151104", name = "manifest#"),
    'Lifetime':        _schema.format(host = SCHEMA_HOST, directory = "20151104", name = "lifetime#"),
    'NetworkResource': _schema.format(host = SCHEMA_HOST, directory = "20151104", name = "networkresource#"),
    'Node':            _schema.format(host = SCHEMA_HOST, directory = "20151104", name = "node#"),
    'Domain':          _schema.format(host = SCHEMA_HOST, directory = "20151104", name = "domain#"),
    'Port':            _schema.format(host = SCHEMA_HOST, directory = "20151104", name = "port#"),
    'Link':            _schema.format(host = SCHEMA_HOST, directory = "20151104", name = "link#"),
    'Path':            _schema.format(host = SCHEMA_HOST, directory = "20151104", name = "path#"),
    'Network':         _schema.format(host = SCHEMA_HOST, directory = "20151104", name = "network#"),
    'Topology':        _schema.format(host = SCHEMA_HOST, directory = "20151104", name = "topology#"),
    'Service':         _schema.format(host = SCHEMA_HOST, directory = "20151104", name = "service#"),
    'Metadata':        _schema.format(host = SCHEMA_HOST, directory = "20151104", name = "metadata#"),
    'Data':            _schema.format(host = SCHEMA_HOST, directory = "20151104", name = "data#"),
    'Datum':           _schema.format(host = SCHEMA_HOST, directory = "20151104", name = "datum#"),
    'Measurement':     _schema.format(host = SCHEMA_HOST, directory = "20151104", name = "measurement#"),
    'Exnode':          _schema.format(host = SCHEMA_HOST, directory = "exnode/4", name = "exnode#"),
    'Extent':          _schema.format(host = SCHEMA_HOST, directory = "exnode/4", name = "extent#"),
}
