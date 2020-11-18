import os
from ..grizly.sources.sources_factory import Source
from ..grizly.utils.functions import get_path
import pytest


def write_out(out):
    with open(get_path("output.sql", from_where="here"), "w",) as f:
        f.write(out)


def test_check_if_exists():
    db = Source(dsn="redshift_acoe")
    assert db.check_if_exists("fiscal_calendar_weeks", "base_views")


def test_pyodbc_interface():
    db = Source(dsn="redshift_acoe")
    assert db.check_if_exists("fiscal_calendar_weeks", "base_views")


def test_create_external_table():
    db = Source(dsn="redshift_acoe")
    table = "test_create_external_table"
    schema = "acoe_spectrum"

    db = db.create_table(
        table_type="external",
        table=table,
        columns=["col1", "col2"],
        types=["varchar(100)", "int"],
        schema=schema,
        s3_key="bulk/",
        bucket="acoe-s3",
        if_exists="drop",
    )
    assert db.check_if_exists(table=table, schema=schema)

    with pytest.raises(ValueError):
        db = db.create_table(
            table_type="external",
            table=table,
            columns=["col1", "col2"],
            types=["varchar(100)", "int"],
            schema=schema,
            s3_key="bulk/",
            bucket="acoe-s3",
            if_exists="fail",
        )

    db._run_query(sql=f"DROP TABLE {schema}.{table}", autocommit=True)

    assert not db.check_if_exists(table=table, schema=schema)
