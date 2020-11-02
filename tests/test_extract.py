import logging
import os

import pytest
from grizly import get_path
from grizly.config import config

from ..grizly.drivers.frames_factory import QFrame
from ..grizly.tools.extract import Extract

"""NOTE:
When experimental.py is changing, grizly needs to be re-installed
on the 'tests_dask_worker_1' as well as on the 'pytest_grizly_notebook'
(maybe also with 'dask_scheduler') to execute the pytest test correctly."""


cwd = os.getcwd()

sqlite_dsn = os.path.join(cwd, "Chinook.sqlite")
denodo_dsn = "DenodoODBC"
sfdc_dsn = "sfdc"
output_dsn = "redshift_acoe"
scheduler_address = "dask_scheduler:8786"

s3_bucket = config.get_service("s3").get("bucket")

logger = logging.getLogger(__name__)


"""fixtures"""


@pytest.fixture(scope="session")
def simple_extract():
    qf = QFrame(dsn=sqlite_dsn, db="sqlite", dialect="mysql", logger=logger).from_table(
        table="Track"
    )
    simple_extract = Extract(
        name="Simple Extract Test",
        qf=qf,
        output_dsn=output_dsn,
        scheduler_address=scheduler_address,
    )
    return simple_extract


@pytest.fixture(scope="session")
def denodo_extract():
    qf = QFrame(dsn=denodo_dsn, logger=logger).from_json(f"s3://{s3_bucket}/test/qframe_store.json")
    qf.limit(5)
    denodo_extract = Extract(
        name="Denodo Extract Test",
        qf=qf,
        store_path=f"s3://{s3_bucket}/test/store.json",
        store_backend="s3",
        output_dsn=output_dsn,
        scheduler_address=scheduler_address,
        if_exists="replace",
    )
    return denodo_extract


@pytest.fixture(scope="session")
def sfdc_extract():
    table = "User"
    qf = QFrame(dsn=sfdc_dsn, table=table, logger=logger)
    qf.limit(5)
    sfdc_extract = Extract(
        name="SFDC Extract Test", qf=qf, output_dsn=output_dsn, scheduler_address=scheduler_address,
    )
    return sfdc_extract


@pytest.fixture(scope="session", params=["simple_extract", "denodo_extract", "sfdc_extract"])
def extract(simple_extract, denodo_extract, sfdc_extract, request):
    return eval(request.param)


"""test functions"""


def test_connection(extract):
    extract._get_client(scheduler_address)


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
