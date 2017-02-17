import logging
import socket

HOSTNAME = socket.gethostname() ### this needs to get the fqdn for DOMAIN to be right down below
CONFIGFILE = "/home/mzhang/workspace/nre/kernel/nre.conf"

SCHEMAS = {
    'networkresources': 'http://unis.crest.iu.edu/schema/20160630/networkresource#',
    'nodes': 'http://unis.crest.iu.edu/schema/20160630/node#',
    'domains': 'http://unis.crest.iu.edu/schema/20160630/domain#',
    'ports': 'http://unis.crest.iu.edu/schema/20160630/port#',
    'links': 'http://unis.crest.iu.edu/schema/20160630/link#',
    'paths': 'http://unis.crest.iu.edu/schema/20160630/path#',
    'networks': 'http://unis.crest.iu.edu/schema/20160630/network#',
    'topologies': 'http://unis.crest.iu.edu/schema/20160630/topology#',
    'services': 'http://unis.crest.iu.edu/schema/20160630/service#',
    'blipp': 'http://unis.crest.iu.edu/schema/20160630/blipp#',
    'metadata': 'http://unis.crest.iu.edu/schema/20160630/metadata#',
    'datum': 'http://unis.crest.iu.edu/schema/20160630/datum#',
    'data': 'http://unis.crest.iu.edu/schema/20160630/data#',
    'ipports': 'http://unis.crest.iu.edu/schema/ext/ipport/1/ipport#'
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

STANDALONE_DEFAULTS = {
}

DEBUG = True
NETLOGGER_NAMESPACE = "urt"

def config_logger(logger):
    """Configures netlogger"""
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    return logger


def get_logger(namespace=NETLOGGER_NAMESPACE):
    """Return logger object"""
    logger = logging.getLogger(namespace)
    if DEBUG:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    
    if logger.handlers:
        return logger
    else:
        return config_logger(logger)
    return nllog.get_logger(namespace)

