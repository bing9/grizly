from .utils.functions import set_cwd, get_path, file_extension, read_config, retry
from .store import Store
from .config import Config, config

# from .tools.base import copy_df_to_excel
from .drivers.frames_factory import QFrame
from .drivers.sql import union, join
from .tools.crosstab import Crosstab
from .tools.email import Email
from .drivers.sfdc import SFDCDriver
from .sources.filesystem.old_s3 import S3
from .sources.rdbms.rdbms_factory import SQLDB, RDBMS
from .drivers.github import GitHubDriver

# from .scheduling.orchestrate import Workflow, Listener, EmailListener, Schedule, Runner
from .scheduling.registry import SchedulerDB, Job, Trigger
from .tools.extract import Extract, SimpleExtract, SFDCExtract


import os
from sys import platform

if platform.startswith("linux"):
    home_env = "HOME"
else:
    home_env = "USERPROFILE"

home_path = os.getenv(home_env) or "/root"
try:
    cwd = home_path
except KeyError:
    pass
