import os
import sys

UNISRT_ROOT = os.path.dirname(os.path.abspath(__file__)) + os.sep
LOCAL_ROOT = os.path.expanduser('/var/unis') + os.sep
sys.path.append(os.path.dirname(os.path.dirname(UNISRT_ROOT)))