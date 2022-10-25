import os
from unis.config import Argument

def expandvar(x):
    v = os.path.expandvars(x)
    return None if v == x else v

ID_FIELD, TS_FIELD = ':id', ':ts'

CONFIG_PATH = "./mundus.conf"
SCHEMA_PRELOAD_ARCHIVE = expandvar("$MUNDUS_SCHEMA_ARCHIVE")

# TODO
#   Switch to mundus schema using local cache
#   Proposal:
#     Use cache directory to build object cache with optional schema loc file (see mundd)

OPTIONS = [
    Argument('', 'runtime.threadpool_size', 1, int, 'Set the number of threads maintained for handling asynchronous tasks.  Reducing threadcount will close threads as they become available.'),
    Argument('', 'conn.preload_models', [], list, 'Preload all instances of the provided types of models by model name when connecting to a new remote.'),
    Argument('', 'conn.batch_size', 0, int, 'Set the number of records queried per remote request.  Intended for tuning remote query perforamce.'),
    Argument('', 'conn.auto_persist', False, bool, 'Automatically establish persisent connection to remote clients. For event driven clients.'),
    Argument('', 'conn.auto_push', False, bool, 'Automatically push changes to remote. More responsive updates at the expense of greater overhead.'),
    Argument('', 'measurements.read_hist', False, bool, 'Load full measurement history when instancing a record measurement.'),
    Argument('', 'measurements.batch_post', 0, int, 'Set the size of the new measurement cache before sending data to the remote.'),
    Argument('', 'measurements.live', False, bool, 'Enable asynchronous subscription to measurement updates.  This can also be enabled on a per measurement basis.'),
    Argument('', 'models.archive_path', SCHEMA_PRELOAD_ARCHIVE, str, 'Path to the model schema archive.')
]
