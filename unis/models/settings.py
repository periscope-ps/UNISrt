import os

from unis.settings import LOCAL_ROOT

SCHEMA_HOST        = 'unis.crest.iu.edu'
SCHEMAS_LOCAL      = False

JSON_SCHEMAS_ROOT  = os.path.join(LOCAL_ROOT, "schemas")
SCHEMA_CACHE_DIR   = os.path.join(JSON_SCHEMAS_ROOT, ".cache")

JSON_SCHEMA_SCHEMA = "http://json-schema.org/draft-04/schema#"
JSON_SCHEMA_HYPER  = "http://json-schema.org/draft-04/hyper-schema#"
JSON_SCHEMA_LINKS  = "http://json-schema.org/draft-04/links#"

_schema = "http://{host}/schema/{directory}/{name}"
SCHEMAS = {
    'Manifest':        _schema.format(host = SCHEMA_HOST, directory = "20160630", name = "manifest#"),
    'Lifetime':        _schema.format(host = SCHEMA_HOST, directory = "20160630", name = "lifetime#"),
    'NetworkResource': _schema.format(host = SCHEMA_HOST, directory = "20160630", name = "networkresource#"),
    'Node':            _schema.format(host = SCHEMA_HOST, directory = "20160630", name = "node#"),
    'Domain':          _schema.format(host = SCHEMA_HOST, directory = "20160630", name = "domain#"),
    'Port':            _schema.format(host = SCHEMA_HOST, directory = "20160630", name = "port#"),
    'Link':            _schema.format(host = SCHEMA_HOST, directory = "20160630", name = "link#"),
    'Path':            _schema.format(host = SCHEMA_HOST, directory = "20160630", name = "path#"),
    'Network':         _schema.format(host = SCHEMA_HOST, directory = "20160630", name = "network#"),
    'Topology':        _schema.format(host = SCHEMA_HOST, directory = "20160630", name = "topology#"),
    'Service':         _schema.format(host = SCHEMA_HOST, directory = "20160630", name = "service#"),
    'Metadata':        _schema.format(host = SCHEMA_HOST, directory = "20160630", name = "metadata#"),
    'Data':            _schema.format(host = SCHEMA_HOST, directory = "20160630", name = "data#"),
    'Datum':           _schema.format(host = SCHEMA_HOST, directory = "20160630", name = "datum#"),
    'Measurement':     _schema.format(host = SCHEMA_HOST, directory = "20160630", name = "measurement#"),
    'Exnode':          _schema.format(host = SCHEMA_HOST, directory = "exnode/6", name = "exnode#"),
    'Extent':          _schema.format(host = SCHEMA_HOST, directory = "exnode/6", name = "extent#"),
    'OFSwitch':        _schema.format(host = SCHEMA_HOST, directory = "ext/ofswitch/1", name = "ofswitch#"),
    'Flow':            _schema.format(host = SCHEMA_HOST, directory = "ext/flow/1", name = "flow#")
}
