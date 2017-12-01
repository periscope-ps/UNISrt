import os
import sys

##################################################################
# UNISrt Configuration
##################################################################
CONFIGFILE = None

DEFAULT_CONFIG = {
    "unis": [
        {
            "url": "http://localhost:8888",
            "verify": False,
            "cert": None,
            "default": True
        },
        {
            "url": "http://localhost:8889",
            "verify": False,
            "cert": None,
            "enabled": False
        }
    ],
    "services": [], # UnisGrapher ],
    "subscribe": False,
    "preload": [ "nodes", "links" ],
    "cache": {
        "mode": "exponential",
        "growth": 2,
    },
    "proxy": {
        "threads": 10,
        "batch": 1000,
        "subscribe": False
    },
    "measurements": {
        "read_history": False,
        "subscribe": False
    }
}

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

RTUSER_ROOT = os.path.expandvars("$RTUSER_ROOT")
if RTUSER_ROOT is "$RTUSER_ROOT":
    RTUSER_ROOT=os.path.expanduser("~/.unis")

SCHEMA_CACHE_DIR = os.path.join(RTUSER_ROOT, ".cache")


### XXX: below to be removed and replaced with lace in future merge

##################################################################
# Logging configuration
##################################################################
import logging

DEBUG = False
TRACE = False

LOGGER_NAMESPACE = "unisrt"

def config_logger():
    log = logging.getLogger(LOGGER_NAMESPACE)
    if log.handlers:
        return log
    
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    log.addHandler(handler)
    # set level
    if TRACE:
        log_level = logging.TRACE
    elif DEBUG:
        log_level = logging.DEBUG
    else:
        log_level = logging.WARN
        log.setLevel(log_level)
        
def get_logger(namespace=LOGGER_NAMESPACE):
    """Return logger object"""
    config_logger()
    return logging.getLogger(namespace)
