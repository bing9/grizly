import os
from ..grizly.tools.sqldb import SQLDB
from ..grizly.utils import get_path
import pytest


def write_out(out):
    with open(get_path("output.sql", from_where="here"), "w",) as f:
        f.write(out)


def test_check_if_exists():
    sqldb = SQLDB(dsn="redshift_acoe")
    assert sqldb.check_if_exists("fiscal_calendar_weeks", "base_views") == True


def test_pyodbc_interface():
    sqldb = SQLDB(dsn="redshift_acoe")
    assert sqldb.check_if_exists("fiscal_calendar_weeks", "base_views") == True


def test_create_external_table():
    sqldb = SQLDB(dsn="redshift_acoe")
    table = "test_create_external_table"
    schema = "acoe_spectrum"

    sqldb = sqldb.create_table(
        type="external_table",
        table=table,
        columns=["col1", "col2"],
        types=["varchar(100)", "int"],
        schema=schema,
        s3_key="bulk/",
        bucket="acoe-s3",
        if_exists="drop",
    )
    assert sqldb.check_if_exists(table=table, schema=schema, external=True)

    with pytest.raises(ValueError):
        sqldb = sqldb.create_table(
            type="external_table",
            table=table,
            columns=["col1", "col2"],
            types=["varchar(100)", "int"],
            schema=schema,
            s3_key="bulk/",
            bucket="acoe-s3",
            if_exists="fail",
        )

    con = sqldb.get_connection(autocommit=True)
    con.execute(f"DROP TABLE {schema}.{table}")
    con.close()

    assert not sqldb.check_if_exists(table=table, schema=schema, external=True)
