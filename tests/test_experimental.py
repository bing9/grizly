import os
import pytest
import dask
import sys
from pandas import DataFrame
from grizly import Extract, QFrame, get_path, config

# from grizly import get_path
# from ..grizly.dangerous.experimental import Extract
# from ..grizly.tools.qframe import QFrame


"""NOTE:
When experimental.py is changing, grizly needs to be re-installed
on the 'tests_dask_worker_1' as well as on the 'pytest_grizly_notebook'
(maybe also with 'dask_scheduler') to execute the pytest test correctly."""


"""test data"""

dsn = get_path("Chinook.sqlite", from_where="here")

"""fixtures"""
cwd = os.getcwd()


@pytest.fixture(scope="session")
def extract_fixture():
    qf = QFrame(dsn=dsn, db="sqlite", dialect="mysql").from_table(table="Track")
    extract_fixture = Extract(
        name="extract_fixture", driver=qf, store_backend="local", store_path="store.json"
    )
    return extract_fixture


"""test functions"""


# def test_get_existing_partitions(extract_fixture):
#     # TODO
#     client_str = "dask_scheduler:8786"
#     # extract_fixture.submit(client_str=client_str)
#     pass


# def test_get_distinct_values():
#     # TODO
#     pass


# def test_get_partitions_to_download():
#     # TODO
#     pass
