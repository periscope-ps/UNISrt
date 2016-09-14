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

##################################################################
# Netlogger stuff... pasted from Ahmed's peri-tornado
##################################################################
import logging
from netlogger import nllog

DEBUG = False
TRACE = False
NETLOGGER_NAMESPACE = "nre"

def config_logger():
    """Configures netlogger"""
    nllog.PROJECT_NAMESPACE = NETLOGGER_NAMESPACE
    #logging.setLoggerClass(nllog.PrettyBPLogger)
    logging.setLoggerClass(nllog.BPLogger)
    log = logging.getLogger(nllog.PROJECT_NAMESPACE)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    log.addHandler(handler)
    # set level
    if TRACE:
        log_level = (logging.WARN, logging.INFO, logging.DEBUG,
                     nllog.TRACE)[3]
    elif DEBUG:
        log_level = (logging.WARN, logging.INFO, logging.DEBUG,
                     nllog.TRACE)[2]

    else:
        log_level = (logging.WARN, logging.INFO, logging.DEBUG,
                     nllog.TRACE)[1]
    log.setLevel(log_level)


def get_logger(namespace=NETLOGGER_NAMESPACE):
    """Return logger object"""
    # Test if netlogger is initialized
    if nllog.PROJECT_NAMESPACE != NETLOGGER_NAMESPACE:
        config_logger()
    return nllog.get_logger(namespace)

