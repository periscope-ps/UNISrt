import os, configparser, logging

def expandvar(x):
    v = os.path.expandvars(x)
    return None if v == x else v

ID_FIELD, TS_FIELD = ':id', ':ts'

#PERISCOPE_ROOT = expandvar("$PERISCOPE_ROOT") or os.path.expanduser("~/.periscope")
#SCHEMA_CACHE_DIR = os.path.join(PERISCOPE_ROOT, ".rtcache")
#SCHEMA_AUTH = 'unis.crest.iu.edu'
SCHEMA_PRELOAD_ARCHIVE = expandvar("$MUNDUS_SCHEMA_ARCHIVE")

# TODO
#   Switch to mundus schema using local cache
#   Proposal:
#     Use cache directory to build object cache with optional schema loc file (see mundd)

#_SCHEMA_REFS = [('Manifest',        '20160630',       'manifest#'),
#                ('Lifetime',        '20160630',       'lifetime#'),
#                ('NetworkResource', '20160630',       'networkresource#'),
#                ('Node',            '20160630',       'node#'),
#                ('Domain',          '20160630',       'domain#'),
#                ('Port',            '20160630',       'port#'),
#                ('Link',            '20160630',       'link#'),
#                ('Path',            '20160630',       'path#'),
#                ('Network',         '20160630',       'network#'),
#                ('Topology',        '20160630',       'topology#'),
#                ('Service',         '20160630',       'service#'),
#                ('Metadata',        '20160630',       'metadata#'),
#                ('Data',            '20160630',       'data#'),
#                ('Datum',           '20160630',       'datum#'),
#                ('Measurement',     '20160630',       'measurement#'),
#                ('Exnode',          'exnode/6',       'exnode#'),
#                ('Extent',          'exnode/6',       'extent#'),
#                ('OFSwitch',        'ext/ofswitch/1', 'ofswitch#'),
#                ('Flow',            'ext/flow/1',     'flow#')]
#SCHEMAS = {n: f"http://{SCHEMA_AUTH}/schema/{d}/{p}" for n, d, p in _SCHEMA_REFS}
#CONFIGFILE = expandvar("$RTUSER_CONFIG") or os.path.join(PERISCOPE_ROOT, 'rt.conf')

# options: NAME, DESCRIPTION, NOTES, EXAMPLE, DEFAULT
OPTIONS = {
    'threadpool_size': ('Sets the number of threads maintained for handling asynchronous tasks.', 'Changing thread count will not stop threads', '10', 10),
    'preload_models': ('Preload all instances of a list of models when connecting to remotes.', 'Does not load instances for existing connections', '[Node, Link]', []),
    'batch_size': ('Sets the number of records queried per remote request.', 'Intended for tuning remote query performance', '1000', 1000),
    'auto_persist': ('Automatically establish persistent connection to remote clients.', 'For event driven clients', 'False', False),
    'auto_push': ('Automatically push changes to remote.', 'More responsive updates, results in more overhead', 'False', False),
    'meas_read_hist': ('Load full measurement history when instancing a record measurement.', '', 'False', False),
    'meas_batch_post': ('Sets the size of the new measurement batch before submission.', 'Measurements will not post until batch is full', '0', 0),
    'meas_live': ('Enable asynchronous subscription to measurement updates.', 'This can also be enabled per measurement', 'False', False)
}

#_parser = configparser.ConfigParser(allow_no_value=True)
#_parser.read(CONFIGFILE)
_log = logging.getLogger('unis.config')
_tys = { "true": True, "false": False, "none": None, "": None }

#def _ls(v):
#    for i,x in enumerate(v):
#        try: v[i] = int(x)
#        except ValueError: pass
#    return v if len(v) > 1 else v[0]

#for section in _parser.sections():
#    if section.startswith('unis'):
#        CONFIG['unis'].append({k:_tys.get(v,v) for k,v in _parser.items(section)})
#    else:
#        if section not in CONFIG: _log.warn(f"Bad configuration - {section}")
#        CONFIG[section].update({k:_tys.get(v, _ls(v.split("."))) for k,v in _parser.items(section)})
