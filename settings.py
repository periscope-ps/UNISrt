import socket

HOSTNAME = socket.gethostname() ### this needs to get the fqdn for DOMAIN to be right down below
CONFIGFILE = "/home/mzhang/workspace/nre/kernel/nre.conf"

SCHEMAS = {
    'networkresources': 'http://unis.incntre.iu.edu/schema/20140214/networkresource#',
    'nodes': 'http://unis.incntre.iu.edu/schema/20140214/node#',
    'domains': 'http://unis.incntre.iu.edu/schema/20140214/domain#',
    'ports': 'http://unis.incntre.iu.edu/schema/20140214/port#',
    'links': 'http://unis.incntre.iu.edu/schema/20140214/link#',
    'paths': 'http://unis.incntre.iu.edu/schema/20140214/path#',
    'networks': 'http://unis.incntre.iu.edu/schema/20140214/network#',
    'topologies': 'http://unis.incntre.iu.edu/schema/20140214/topology#',
    'services': 'http://unis.incntre.iu.edu/schema/20140214/service#',
    'blipp': 'http://unis.incntre.iu.edu/schema/20140214/blipp#',
    'metadata': 'http://unis.incntre.iu.edu/schema/20140214/metadata#',
    'datum': 'http://unis.incntre.iu.edu/schema/20140214/datum#',
    'data': 'http://unis.incntre.iu.edu/schema/20140214/data#',
    'ipports': 'http://unis.incntre.iu.edu/schema/ext/ipport/1/ipport#'
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

DEBUG = True
TRACE = False
NETLOGGER_NAMESPACE = "blipp"

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

