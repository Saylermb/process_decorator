import sys
if sys.version_info[1] < 8:
    raise NotImplementedError
from .base import SharedObjectBase
from .dict import Dict
from .queue import Queue
