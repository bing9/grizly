from .utils import set_cwd, get_path, file_extension, read_config
from .store import Store
from .config import Config

from .ui.start import UI

from .tools.extract import copy_df_to_excel
from .tools.qframe import QFrame, union, join, initiate
from .tools.crosstab import Crosstab
from .tools.email import Email
from .tools.sfdc import SFDC
from .tools.s3 import S3, s3_to_csv, csv_to_s3, df_to_s3, s3_to_rds
from .tools.github import GitHub
from .tools.sqldb import SQLDB, check_if_exists, delete_where, get_columns, copy_table
from .scheduling.orchestrate import Workflow, Listener, EmailListener, Schedule, Runner, retry


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
