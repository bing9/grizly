import logging
import os
from time import sleep

import pytest
from grizly.config import config
from grizly.drivers.frames_factory import QFrame
from grizly.scheduling.registry import Job, SchedulerDB
from grizly.tools.extract import Extract

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
registry = SchedulerDB(redis_host="pytest_redis")

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
        if_exists="replace",
    )
    return simple_extract


@pytest.fixture(scope="session")
def denodo_extract():
    qf = QFrame(dsn=denodo_dsn, logger=logger).from_json(
        f"s3://{s3_bucket}/test/denodo_extract_store.json"
    )
    denodo_extract = Extract(
        name="Denodo Extract Test",
        qf=qf,
        store_backend="s3",
        scheduler_address=scheduler_address,
        if_exists="replace",
    )
    return denodo_extract


@pytest.fixture(scope="session")
def sfdc_extract():
    table = "Account"
    qf = QFrame(dsn=sfdc_dsn, table=table, columns=["Id", "Name"], logger=logger)
    qf.limit(1000)
    sfdc_extract = Extract(
        name="SFDC Extract Test",
        qf=qf,
        output_dsn=output_dsn,
        scheduler_address=scheduler_address,
        if_exists="replace",
    )
    return sfdc_extract


@pytest.fixture(scope="session", params=["simple_extract", "denodo_extract", "sfdc_extract"])
def extract(simple_extract, denodo_extract, sfdc_extract, request):
    return eval(request.param)


"""test functions"""


# def test_connection(extract):
#     extract._get_client(scheduler_address)


# def test_connection_fail(extract):
#     with pytest.raises(OSError):
#         extract._get_client("123:4")


def test_simple_extract_e2e(simple_extract):
    # TODO
    result = simple_extract.submit(registry=registry)
    assert result is True

    spectrum_table = QFrame(dsn=output_dsn, schema="acoe_spectrum", table="simple_extract_test")
    assert spectrum_table.nrows > 0


def test_denodo_extract_e2e(denodo_extract):
    partition_jobs_result = denodo_extract.submit(registry=registry)
    assert partition_jobs_result is True
    sleep(5)
    extract_job_result = Job("Denodo Extract Test", db=registry).last_run.status
    while not extract_job_result == "success":
        sleep(0.1)
        extract_job_result = Job("Denodo Extract Test", db=registry).last_run.status

    spectrum_table = QFrame(dsn=output_dsn, schema="acoe_spectrum", table="simple_extract_test")
    assert spectrum_table.nrows > 0


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
