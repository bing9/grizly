import logging
import os

import pytest
import s3fs
from grizly.config import config
from grizly.drivers.frames_factory import QFrame
from grizly.scheduling.registry import Job, SchedulerDB
from grizly.tools.extract import Extract


cwd = os.getcwd()

sqlite_dsn = os.path.join(cwd, "Chinook.sqlite")
denodo_dsn = "DenodoODBC"
sfdc_dsn = "sfdc"
output_dsn = "redshift_acoe"
scheduler_address = "dask_scheduler:8786"
registry = SchedulerDB(redis_host="pytest_redis")

s3_bucket = config.get_service("s3").get("bucket")
s3 = s3fs.S3FileSystem()

logger = logging.getLogger(__name__)


"""fixtures"""


# @pytest.fixture(scope="session")
# def simple_extract():
#     qf = QFrame(dsn=sqlite_dsn, db="sqlite", dialect="mysql", logger=logger).from_table(
#         table="Track"
#     )
#     simple_extract = Extract(
#         qf=qf,
#         name="Simple Extract Test",
#         output_dsn=output_dsn,
#         scheduler_address=scheduler_address,
#         if_exists="replace",
#     )
#     yield simple_extract
#     simple_extract.unregister(remove_job_runs=True)
#     s3.rm(simple_extract.s3_root_url, recursive=True)


# def test_read_extract_store():
#     qf = QFrame(dsn="DenodoODBC", logger=logger).from_json(
#         f"s3://{s3_bucket}/test/denodo_extract_store.json"
#     )
#     assert qf.extract_store is not None


@pytest.fixture(scope="session")
def denodo_extract():
    denodo_extract = Extract().from_json(f"s3://{s3_bucket}/test/denodo_extract_store.json")
    yield denodo_extract
    denodo_extract.unregister(remove_job_runs=True)
    s3.rm(denodo_extract.s3_root_url, recursive=True)
    # TODO: drop table as well


# @pytest.fixture(scope="session")
# def sfdc_extract():
#     sfdc_extract = Extract().from_json(f"s3://{s3_bucket}/test/sfdc_extract_store.json")
#     yield sfdc_extract
#     sfdc_extract.unregister(remove_job_runs=True)
#     s3.rm(sfdc_extract.s3_root_url, recursive=True)


# @pytest.fixture(scope="session")
# def sfdc_extract():
#     table = "Account"
#     qf = QFrame(dsn=sfdc_dsn, table=table, columns=["Id", "Name"], logger=logger)
#     qf.limit(1000)
#     sfdc_extract = Extract(
#         qf=qf,
#         name="SFDC Extract Test",
#         output_dsn=output_dsn,
#         scheduler_address=scheduler_address,
#         if_exists="replace",
#     )
#     yield sfdc_extract
#     sfdc_extract.unregister(remove_job_runs=True)
#     s3.rm(sfdc_extract.s3_root_url, recursive=True)


# @pytest.fixture(scope="session", params=["simple_extract", "denodo_extract", "sfdc_extract"])
# def extract(simple_extract, denodo_extract, sfdc_extract, request):
#     return eval(request.param)


#########
# Tests #
#########


# def test_connection(extract):
#     extract._get_client(scheduler_address)


# def test_connection_fail(extract):
#     with pytest.raises(OSError):
#         extract._get_client("123:4")


# def test_simple_extract_e2e(simple_extract):
#     # TODO
#     result = simple_extract.submit(registry=registry)
#     assert result is True

#     spectrum_table = QFrame(dsn=output_dsn, schema="acoe_spectrum", table="simple_extract_test")
#     assert spectrum_table.nrows > 0


##########
# Denodo #
##########


# def test_denodo_extract_e2e(denodo_extract):
#     result = denodo_extract.submit(registry=registry)  # reregister=True
#     assert result is True
#     assert Job("Denodo Extract Test", db=registry).last_run.status == "success"
#     spectrum_table = QFrame(dsn=output_dsn, schema="acoe_spectrum", table="denodo_extract_test")
#     assert spectrum_table.nrows > 0


# def test_get_distinct_values(denodo_extract):
#     values = denodo_extract.get_distinct_values().compute()
#     assert values == [
#         "Poland|Krakow",
#         "Poland|Tarnow",
#         "Poland|Nowy Sacz",
#         "Poland|Zakopane",
#         "Poland|Warszawa",
#         "Poland|Plock",
#         "Poland|Radom",
#         "Poland|Sieradz",
#         "USA|Los Angeles",
#         "USA|San Francisco",
#         "USA|San Jose",
#         "USA|Miami",
#         "USA|Tampa",
#         "USA|Orlando",
#     ]


# def test_get_existing_partitions(denodo_extract):
#     denodo_extract.submit(registry=registry)
#     # existing_partitions = denodo_extract.get_existing_partitions().compute()
#     # logger.info(existing_partitions)
#     pass


# def test_get_partitions_to_download():
#     pass


# # TODO
# def test_if_exists_append():
#     # - should not create an external table if it already exists
#     # Denodo - should not remove files from s3_staging_url
#     # SFDC - as per 0.4.0rc1, should remove all files from s3_staging_url
#     pass
