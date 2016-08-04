import os
import sys

##################################################################
# UNISrt Configuration
##################################################################
CONFIGFILE = None

STANDALONE_DEFAULTS = {
    "properties": {
        "configurations": {
            "unis_url": "http://localhost:8888",
            "ms_url": "http://localhost:8888",
            "use_ssl": False,
            "ssl_cert": "client.pem",
            "ssl_key": "client.key",
            "ssl_cafile": None
            }
        }
}

UNISRT_ROOT = os.path.dirname(os.path.abspath(__file__)) + os.sep
sys.path.append(os.path.dirname(os.path.dirname(UNISRT_ROOT)))

##################################################################
# Schema definitions and locations
##################################################################
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

MIME = {
    'HTML': 'text/html',
    'JSON': 'application/json',
    'PLAIN': 'text/plain',
    'SSE': 'text/event-stream',
    'PSJSON': 'application/perfsonar+json',
    'PSBSON': 'application/perfsonar+bson',
    'PSXML': 'application/perfsonar+xml',
    }

##################################################################
# Logging configuration
##################################################################
import logging

DEBUG = True
TRACE = False

LOGGER_NAMESPACE = "unisrt"

def config_logger():
    log = logging.getLogger(LOGGER_NAMESPACE)
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
