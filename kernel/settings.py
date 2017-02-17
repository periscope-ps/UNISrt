
SCHEMA_HOST        = 'unis.crest.iu.edu'
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
}
