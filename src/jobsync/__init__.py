__version__ = '1.2.0'

from jobsync.client import Job as Job
from jobsync.client import Task as Task
from jobsync.config import CoordinationConfig as CoordinationConfig
from jobsync.schema import \
    get_table_names_for_appname as get_table_names_for_appname
