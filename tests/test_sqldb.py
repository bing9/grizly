import os
from ..grizly.tools.sqldb import SQLDB
from ..grizly.utils import get_path


def write_out(out):
    with open(get_path("output.sql", from_where="here"), "w",) as f:
        f.write(out)


def test_check_if_exists():
    sqldb = SQLDB(db="redshift", engine_str="mssql+pyodbc://redshift_acoe")
    assert sqldb.check_if_exists("fiscal_calendar_weeks", "base_views") == True


def test_pyodbc_interface():
    sqldb = SQLDB(db="redshift", engine_str="mssql+pyodbc://redshift_acoe", interface="pyodbc")
    assert sqldb.check_if_exists("fiscal_calendar_weeks", "base_views") == True
