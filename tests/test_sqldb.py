import os
from ..grizly.tools.sqldb import SQLDB, check_if_valid_type
from ..grizly.utils import get_path


def write_out(out):
    with open(get_path("output.sql", from_where="here"), "w",) as f:
        f.write(out)


def test_check_if_exists():
    sqldb = SQLDB(db="redshift", engine_str="mssql+pyodbc://redshift_acoe")
    assert sqldb.check_if_exists("fiscal_calendar_weeks", "base_views") == True


def test_check_if_valid_type():
    assert check_if_valid_type("INT")
    assert not check_if_valid_type("string")
    assert check_if_valid_type("varchar(30)")

