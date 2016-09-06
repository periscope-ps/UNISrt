from unis.settings import UNISRT_ROOT

##################################################################
# UNISrt Configuration
##################################################################
CONFIGFILE = None

DEFAULT_CONFIG = {
    "unis": {
        "url": "http://localhost:8888",
        "use_ssl": False,
        "cert": None,
    },
    "ms": {
        "url": "http://localhost:8888",
        "use_ssl": False,
        "cert": None
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
