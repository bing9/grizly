import datetime
import gc
import json
import os
from abc import abstractmethod
from typing import Any, List, Optional, Union

import dask
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
from dask.delayed import Delayed
from distributed import Client
from pyarrow import Table

from ..config import config
from ..drivers.frames_factory import QFrame
from ..scheduling.registry import Job, SchedulerDB
from ..sources.filesystem.old_s3 import S3
from ..utils.functions import chunker

WORKING_DIR = os.getcwd()


class BaseExtract:
    """
    Common functions for all extracts. The logic for parallelization
    must be defined in the subclass"""

    def __init__(
        self,
        name: str,
        qf,
        s3_root_url: str = None,
        output_external_table: str = None,
        output_external_schema: str = None,
        output_dsn: str = None,
        scheduler_address: str = None,
        output_table_type: str = "external",
        if_exists: str = "append",
    ):
        self.bucket = config.get_service("s3")["bucket"]
        self.name = name
        self.name_snake_case = self._to_snake_case(name)
        self.qf = qf
        self.output_table_type = output_table_type
        self.s3_root_url = s3_root_url or f"s3://{self.bucket}/extracts/{self.name_snake_case}/"
        self.s3_staging_url = os.path.join(self.s3_root_url, "data", "staging")
        self.output_external_table = output_external_table or self.name_snake_case
        self.output_external_schema = output_external_schema or os.getenv(
            "GRIZLY_EXTRACT_STAGING_EXTERNAL_SCHEMA"
        )
        self.output_dsn = output_dsn
        self.scheduler_address = scheduler_address or os.getenv("DASK_SCHEDULER_ADDRESS")
        self.if_exists = if_exists
        self.table_if_exists = self._map_if_exists(if_exists)
        self.priority = 0
        self.logger = self.qf.logger

    @staticmethod
    def _to_snake_case(text):
        return text.lower().replace(" - ", "_").replace(" ", "_")

    @staticmethod
    def _map_if_exists(if_exists):
        """ Map data-related if_exists to table-related commands """
        mapping = {"skip": "skip", "append": "skip", "replace": "drop"}
        return mapping[if_exists]

    def _get_client(self):
        return Client(self.scheduler_address)

    @dask.delayed
    def to_arrow(self):
        self.logger.info("Writing data to arrow...")
        pa = self.qf.to_arrow()
        gc.collect()
        return pa

    @dask.delayed
    def arrow_to_s3(self, arrow_table: Table, file_name: str = None):
        def _arrow_to_s3(arrow_table):
            def give_name(_):
                return file_name

            s3 = s3fs.S3FileSystem()
            self.logger.info(f"Uploading {file_name} to {self.s3_staging_url}...")
            pq.write_to_dataset(
                arrow_table,
                root_path=self.s3_staging_url,
                filesystem=s3,
                use_dictionary=True,
                compression="snappy",
                partition_filename_cb=give_name,
            )
            self.logger.info(f"Successfully uploaded {file_name} to {self.s3_staging_url}")

        _arrow_to_s3(arrow_table)

    @dask.delayed
    def to_s3(self, path):
        self.logger.info(f"Downloading data to {path}...")
        self.qf.to_parquet(path)

    @dask.delayed
    def wipe_staging(self):
        s3 = s3fs.S3FileSystem()
        s3.rm(self.s3_staging_url, recursive=True)

    @dask.delayed
    def create_external_table(self, upstream: Delayed = None):
        self.qf.create_external_table(
            schema=self.output_external_schema,
            table=self.output_external_table,
            dsn=self.output_dsn,
            s3_url=self.s3_staging_url,
            if_exists=self.table_if_exists,
        )

    @dask.delayed
    def create_table(self, upstream: Delayed = None):
        qf = QFrame(dsn=self.output_dsn, dialect="mysql", logger=self.logger).from_table(
            schema=self.output_external_schema, table=self.output_external_table
        )
        qf.to_table(
            schema=self.output_schema_prod,
            table=self.output_table_prod,
            if_exists=self.if_exists,  # here we use the unmapped if_exists
        )

    @abstractmethod
    def generate_tasks(self):
        pass

    def register(
        self,
        registry: Optional[SchedulerDB] = None,
        redis_host: Optional[str] = None,
        redis_port: Optional[int] = None,
        **kwargs,
    ):
        """Submit the partitions and/or extract job

        Parameters
        ----------
        kwargs: arguments to pass to Job.register()

        Examples
        ----------
        dev_registry = SchedulerDB(dev_scheduler_addr)
        Extract().register(registry=dev_registry, crons="0 12 * * MON", if_exists="replace")  # Mondays 12 AM
        """
        registry = registry or SchedulerDB(
            logger=self.logger, redis_host=redis_host, redis_port=redis_port
        )
        self.extract_job = Job(self.name, logger=self.logger, db=registry)
        self.extract_job.register(tasks=self.generate_tasks(), **kwargs)

    def submit(self, registry, **kwargs):
        """Submit the extract job

        Parameters
        ----------
        kwargs: arguments to pass to Job.register() and Job.submit()

        """
        self.register(registry=registry, if_exists=self.if_exists)
        self.extract_job.submit(scheduler_address=self.scheduler_address, **kwargs)
        return True


class SimpleExtract(BaseExtract):
    """"Extract data in single piece"""

    def generate_tasks(self):
        file_name = self.name_snake_case + ".parquet"
        path = os.path.join(self.s3_root_url, file_name)
        s3 = self.to_s3(path)
        external_table = self.create_external_table(upstream=s3)
        base_table = self.create_table(upstream=external_table)

        if self.output_table_type == "base":
            final_task = base_table
        else:
            final_task = external_table

        self.logger.debug("Tasks generated successfully")

        return [final_task]


class SFDCExtract(BaseExtract):
    """
    Exctract data from SFDC. The data is extracted using the standard API
    in batches.

    We partition data by querying SFDC, calculating the number of URLs with chunks of
    response data, and grouping them into bigger chunks of eg. 50 URLs (with the typical SFDC
    limit per URL being 250-500 rows). For example, a 125k row table could be split into 10
    parallel branches, each worker being responsible for processing a chunk containing 12.5k rows.

    Note that partitions are not guaranteed to match, so instead of just overwriting, like in
    DenodoExtract, we remove all files before loading the new ones. This is because
    Spectrum reads all files in the S3 folder into the table, so if we had files with
    overlapping content, it would load the duplicated rows.
    """

    def generate_tasks(self) -> List[Delayed]:
        self.logger.info("Generating tasks...")

        urls = self.get_urls()
        url_chunks = self.chunk(urls, chunksize=50)
        client = self._get_client()
        chunks = client.compute(url_chunks).result()

        wipe_staging = self.wipe_staging() if self.if_exists == "replace" else None

        s3_uploads = []
        batch_no = 1
        for url_chunk in chunks:
            first_url_pos = url_chunk[0].split("-")[1]
            last_url_pos = url_chunk[-1].split("-")[1]
            file_name = f"{first_url_pos}-{last_url_pos}.parquet"
            arrow_table = self.urls_to_arrow(url_chunk, batch_no=batch_no, upstream=wipe_staging)
            to_s3 = self.arrow_to_s3(arrow_table, file_name=file_name)
            s3_uploads.append(to_s3)
            batch_no += 1

        external_table = self.create_external_table(upstream=s3_uploads)
        base_table = self.create_table(upstream=external_table)

        if self.output_table_type == "base":
            final_task = base_table
        else:
            final_task = external_table

        self.logger.info("Tasks generated successfully")

        return [final_task]

    @dask.delayed
    def get_urls(self):
        return self.qf.source._get_urls_from_response(query=self.qf.get_sql())

    @staticmethod
    @dask.delayed
    def chunk(iterable, chunksize):
        return chunker(iterable, size=chunksize)

    @dask.delayed
    def urls_to_arrow(
        self, urls: List[str], batch_no: int, upstream: dask.delayed = None
    ) -> pa.Table:
        self.logger.info(f"Converting batch no. {batch_no} into a pyarrow table...")
        start = datetime.datetime.now()
        records = []
        for url in urls:
            records_chunk: List[tuple] = self.qf.source._fetch_records_url(url)
            redords_chunk_processed: List[tuple] = self.qf._cast_records(records_chunk)
            records.extend(redords_chunk_processed)

        cols = self.qf.get_fields(aliased=True)

        _dict = {col: [] for col in cols}
        for record in records:
            for i, col_val in enumerate(record):
                col_name = cols[i]
                _dict[col_name].append(col_val)

        arrow_table = self.qf._dict_to_arrow(_dict)

        duration = datetime.datetime.now() - start
        seconds = duration.seconds
        msg = f"Pyarrow table for batch no. {batch_no} has been successfully generated in {seconds} second(s)."
        self.logger.info(msg)
        gc.collect()
        return arrow_table


class DenodoExtract(BaseExtract):
    def __init__(
        self,
        name,
        qf,
        *args,
        store_backend: str = "local",
        store_path: str = None,
        store: dict = None,
        **kwargs,
    ):
        super().__init__(name, qf, *args, **kwargs)
        self.store_backend = store_backend.lower()
        self.store_path = store_path or self._get_default_store_path()
        self.store = store or qf.store["extract"]  # TODO: add store validation
        self._load_attrs_from_store(self.store)

    def _get_default_store_path(self) -> str:
        if self.store_backend == "local":
            path = os.path.join(WORKING_DIR, "store.json")
        elif self.store_backend == "s3":
            path = os.path.join(self.s3_root_url, "store.json")
        else:
            raise NotImplementedError
        return path

    def _validate_store(self, store):
        pass

    def _load_attrs_from_store(self, store):
        """Load values defined in store into attributes"""
        self.partition_cols = store["partition_cols"]
        self.output_dsn = store["output"].get("dsn") or self.qf.source.dsn
        self.output_external_schema = store["output"].get("external_schema") or os.getenv(
            "GRIZLY_EXTRACT_STAGING_EXTERNAL_SCHEMA"
        )
        self.output_schema_prod = store["output"].get("schema") or os.getenv(
            "GRIZLY_EXTRACT_STAGING_SCHEMA"
        )
        self.output_external_table = store["output"].get("external_table") or self.name_snake_case
        self.output_table_prod = store["output"].get("table") or self.name_snake_case
        self.output_table_type = "base" if store["output"].get("table") else "external"

    @dask.delayed
    def get_distinct_values(self):
        def _validate_columns(columns: Union[str, List[str]], existing_columns: List[str]):
            """ Check whether columns exist within the table """
            if isinstance(columns, str):
                column = [columns]
            for column in columns:
                # column = column.replace("sq.", "")
                if column not in existing_columns:
                    raise ValueError(f"Driver does not contain {column}")

        columns = self.partition_cols
        existing_columns = self.qf.get_fields(aliased=True)
        _validate_columns(columns, existing_columns)

        self.logger.info(f"Obtaining the list of unique values in {columns}...")

        try:
            # faster method, but this will fail if user is working on multiple schemas/tables
            schema = self.qf.store["select"]["schema"]
            table = self.qf.store["select"]["table"]
            where = self.qf.store["select"]["where"]

            partitions_qf = (
                self.qf.copy()
                .from_table(table=table, schema=schema, columns=columns)
                .where(where)
                .groupby()
            )
        except KeyError:
            # source qf queries multiple tables; use source qf
            partitions_qf = self.qf.copy().select(columns).groupby()

        records = partitions_qf.to_records()
        if isinstance(columns, list):
            values = ["|".join(str(val) for val in row) for row in records]
        else:
            values = [row[0] for row in records]

        self.logger.info(f"Successfully obtained the list of unique values in {columns}")
        self.logger.debug(f"Unique values in {columns}: {values}")

        return values

    @dask.delayed
    def get_existing_partitions(self, upstream: Delayed = None):
        """ Returns partitions already uploaded to S3 """

        self.logger.info("Starting the extract process...")

        s3 = S3(url=self.s3_staging_url)
        existing_partitions = []
        for file_name in s3.list():
            extension = file_name.split(".")[-1]
            if extension == "parquet":
                existing_partitions.append(file_name.replace(".parquet", ""))

        self.logger.info(f"Successfully obtained the list of existing partitions")
        self.logger.debug(f"Existing partitions: {existing_partitions}")

        return existing_partitions

    @dask.delayed
    def get_partitions_to_download(
        self, all_partitions, existing_partitions, upstream: Delayed = None
    ):
        self.logger.debug(f"All partitions: {all_partitions}")
        self.logger.debug(f"Existing partitions: {existing_partitions}")
        partitions_to_download = [
            partition for partition in all_partitions if partition not in existing_partitions
        ]
        self.logger.debug(
            f"Partitions to download: {len(partitions_to_download)}, {partitions_to_download}"
        )
        self.logger.info(f"Downloading {len(partitions_to_download)} partitions...")
        return partitions_to_download

    @dask.delayed
    def to_store_backend(self, serializable: Any, file_name: str):
        if self.store_backend == "s3":
            url = os.path.join(self.s3_root_url, file_name)
            s3 = S3(url=url)
            self.logger.info(f"Copying {file_name} from memory to {url}...")
            s3.from_serializable(serializable)
        elif self.store_backend == "local":
            self.logger.info(f"Copying {file_name} from memory to {self.store_path}...")
            with open(self.store_path, "w") as f:
                json.dump(serializable, f)
        else:
            raise NotImplementedError

    @dask.delayed
    def get_cached_distinct_values(self, file_name: str):
        if self.store_backend == "s3":
            s3 = S3(url=os.path.join(self.root_url, file_name))
            values = s3.to_serializable()
        elif self.store_backend == "local":
            with open(self.store_path) as f:
                values = json.load(f)
        else:
            raise NotImplementedError
        return values

    @dask.delayed
    def filter_qf(self, query: str):
        queried = self.qf.copy().where(query, if_exists="append")
        return queried

    @dask.delayed
    def remove_table(self, schema, table):
        pass

    @staticmethod
    @dask.delayed
    def to_parquet(qf, path):
        qf.to_parquet(path)

    def generate_tasks(
        self,
        refresh_partitions_list: bool = True,
        download_if_older_than: int = 0,
        cache_distinct_values: bool = True,
        **kwargs,
    ):

        if refresh_partitions_list:
            all_partitions = self.get_distinct_values()
        else:
            all_partitions = self.get_cached_distinct_values(file_name="all_partitions.json")

        # by default, always cache the list of distinct values in backend for future use
        if cache_distinct_values:
            cache_distinct_values_in_backend = self.to_store_backend(
                all_partitions, file_name="all_partitions.json"
            )
        else:
            cache_distinct_values_in_backend = None

        if self.if_exists == "replace":
            partitions_to_download = all_partitions
        else:
            existing_partitions = self.get_existing_partitions()
            partitions_to_download = self.get_partitions_to_download(
                all_partitions, existing_partitions, upstream=cache_distinct_values_in_backend
            )

        # compute partitions on the cluster
        client = self._get_client()
        partitions = []
        try:
            partitions = client.compute(partitions_to_download).result()
        except Exception:
            self.logger.exception("Something went wrong")
            raise
        finally:
            client.close()
        if not partitions:
            self.logger.warning("No partitions to download")
        self.partition_tasks = [partitions_to_download]

        if len(self.partition_cols) > 1:
            partition_cols_casted = [
                f"CAST({partition_col} AS VARCHAR)" for partition_col in self.partition_cols
            ]
            partition_cols = "CONCAT(" + ", ".join(partition_cols_casted) + ")"
        else:
            partition_cols = self.partition_cols[0]

        if self.if_exists == "skip":
            self.logger.info("Skipping...")
            return

        uploads = []
        for partition in partitions:
            partition_concatenated = partition.replace("|", "")
            file_name = f"{partition}.parquet"
            where = f"{partition_cols}='{partition_concatenated}'"
            processed_qf = self.filter_qf(query=where)
            s3 = self.to_parquet(processed_qf, os.path.join(self.s3_staging_url, file_name))
            uploads.append(s3)

        external_table = self.create_external_table(upstream=uploads)
        if self.output_table_type == "base":
            regular_table = self.create_table(upstream=external_table)
            # remove Spectrum table to avoid duplication
            # clear_spectrum = self.remove_table(
            #     self.output_schema, self.output_table, upstream=regular_table
            # )
            final_task = regular_table
        else:
            final_task = external_table
        self.extract_tasks = [final_task]

    def register(self, registry=None, crons=None, **kwargs):
        """Submit the partitions and/or extract job

        Parameters
        ----------
        kwargs: arguments to pass to Job.register()

        Examples
        ----------
        dev_registry = SchedulerDB(dev_scheduler_addr)
        Extract().register(registry=dev_registry, crons="0 12 * * MON", if_exists="replace")  # Mondays 12 AM
        """
        partitions_job_name = self.name + " - partitions"
        self.partitions_job = Job(partitions_job_name, logger=self.logger, db=registry)
        self.extract_job = Job(self.name, logger=self.logger, db=registry)
        self.generate_tasks()  # calculate partition tasks
        self.partitions_job.register(tasks=self.partition_tasks, crons=crons or [], **kwargs)
        self.extract_job.register(tasks=self.extract_tasks, upstream=partitions_job_name, **kwargs)

    def submit(
        self, registry, **kwargs,
    ):
        """Submit the extract job

        Parameters
        ----------
        kwargs: arguments to pass to Job.register() and Job.submit()

        """
        self.register(registry=registry, if_exists=self.if_exists, **kwargs)
        self.partitions_job.submit(
            scheduler_address=self.scheduler_address,
            priority=kwargs.get("priority"),
            to_dask=kwargs.get("to_dask"),
        )
        return True

    def validate(self):
        pass


def Extract(qf, *args, **kwargs):

    if "sqlite" in qf.source.dsn:
        return SimpleExtract(qf=qf, *args, **kwargs)

    db = config.get_service("sources")[qf.source.dsn]["db"]

    if db == "sfdc":
        return SFDCExtract(qf=qf, *args, **kwargs)
    elif db == "denodo":
        return DenodoExtract(qf=qf, *args, **kwargs)
    else:
        return SimpleExtract(qf=qf, *args, **kwargs)
