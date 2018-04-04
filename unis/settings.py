import os

class ConfigurationError(Exception): pass

##################################################################
# Schema definitions and locations
##################################################################
MIME = {
    'HTML': 'text/html',
    'JSON': 'application/json',
    'PLAIN': 'text/plain',
    'SSE': 'text/event-stream',
    'PSJSON': 'application/perfsonar+json',
    'PSBSON': 'application/perfsonar+bson',
    'PSXML': 'application/perfsonar+xml',
    }

PERISCOPE_ROOT = os.path.expandvars("$PERISCOPE_ROOT")
if PERISCOPE_ROOT == "$PERISCOPE_ROOT":
    PERISCOPE_ROOT = os.path.expanduser("~/.periscope")

SCHEMA_CACHE_DIR = os.path.join(PERISCOPE_ROOT, ".cache")
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
    'OFSwitch':        _schema.format(host = SCHEMA_HOST, directory = "ext/ofswitch/1", name = "ofswitch#"),
    'Flow':            _schema.format(host = SCHEMA_HOST, directory = "ext/flow/1", name = "flow#")
}

##################################################################
# UNISrt Configuration
##################################################################
from unis.services.data import DataService
from unis.services.graph import UnisGrapher

CONFIGFILE = os.path.expandvars("$RTUSER_CONFIG")
if CONFIGFILE == "$RTUSER_CONFIG":
    CONFIGFILE = os.path.join(PERISCOPE_ROOT, "rt.conf")

DEFAULT_CONFIG = {
    "unis": [],
    "services": [DataService, UnisGrapher],
    "cache": {
        "preload": [ "nodes", "links" ],
        "mode": "exponential",
        "growth": 2,
    },
    "proxy": {
        "threads": 10,
        "batch": 1000,
        "subscribe": True,
        "defer_update": True,
    },
    "measurements": {
        "read_history": True,
        "subscribe": True
    }
}

