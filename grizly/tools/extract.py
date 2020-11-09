import datetime
import gc
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
from ..utils.functions import chunker, retry

WORKING_DIR = os.getcwd()
s3 = s3fs.S3FileSystem()


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
    def begin_extract(self):
        self.logger.info("Starting the extract process...")

    @dask.delayed
    def arrow_to_s3(self, arrow_table: Table, file_name: str = None):
        def _arrow_to_s3(arrow_table):
            def give_name(_):
                return file_name

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
    def wipe_staging(self):
        try:
            s3.rm(self.s3_staging_url, recursive=True)
        except FileNotFoundError:
            self.logger.debug(f"Couldn't wipe out {self.s3_staging_url} as it doesn't exist")

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
        qf = QFrame(
            dsn=self.output_dsn,
            dialect="mysql",
            schema=self.output_external_schema,
            table=self.output_external_table,
            logger=self.logger,
        )
        qf.to_table(
            schema=self.output_schema_prod,
            table=self.output_table_prod,
            if_exists="replace",  # always re-create from the external table
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

    def unregister(self, registry: SchedulerDB = None, remove_job_runs: bool = False) -> bool:
        self.extract_job.unregister(remove_job_runs=remove_job_runs)
        return True

    def submit(self, registry, **kwargs):
        """Submit the extract job

        Parameters
        ----------
        kwargs: arguments to pass to Job.register() and Job.submit()

        """
        self.register(registry=registry, if_exists="skip")
        self.extract_job.submit(scheduler_address=self.scheduler_address, **kwargs)
        return True


class SimpleExtract(BaseExtract):
    """"Extract data in single piece"""

    def generate_tasks(self):
        file_name = self.name_snake_case + ".parquet"
        path = os.path.join(self.s3_root_url, file_name)

        start = self.begin_extract()
        s3 = self.to_parquet(qf=self.qf, path=path, upstream=start)
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

        start = self.begin_extract()

        urls = self.get_urls(upstream=start)
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
    def get_urls(self, upstream=None):
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
        self, name, qf, *args, **kwargs,
    ):
        super().__init__(name, qf, *args, **kwargs)
        self.store = qf.store["extract"]  # TODO: add store validation
        self._load_attrs_from_store(self.store)

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
    def __get_source_partitions(self, upstream=None):
        def _validate_columns(columns: Union[str, List[str]], existing_columns: List[str]):
            """ Check whether columns exist within the table """
            if isinstance(columns, str):
                column = [columns]
            for column in columns:
                # column = column.replace("sq.", "")
                if column not in existing_columns:
                    raise ValueError(f"QFrame does not contain {column}")

        columns = self.partition_cols
        existing_columns = self.qf.get_fields(aliased=True)
        _validate_columns(columns, existing_columns)

        self.logger.debug("Retrieving source partitions..")

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
            source_partitions = ["|".join(str(val) for val in row) for row in records]
        else:
            source_partitions = [row[0] for row in records]

        self.logger.info(
            f"Successfully retrieved the list of {len(source_partitions)} source partitions"
        )
        self.logger.debug(f"Source partitions: \n{source_partitions}")

        return source_partitions

    def _get_parquet_files(self, path: str = None) -> List[str]:
        s3_filepaths = s3.ls(path)
        return [os.path.basename(path) for path in s3_filepaths if path.endswith(".parquet")]

    @dask.delayed
    def _get_existing_partitions(self, upstream: Delayed = None):
        """ Returns partitions already uploaded to S3 """

        self.logger.debug("Retrieving existing partitions..")

        parquet_files = self._get_parquet_files(self.s3_staging_url)
        existing_partitions = [fname.replace(".parquet", "") for fname in parquet_files]

        self.logger.info(
            f"Successfully retrieved the list of {len(existing_partitions)} existing partition(s)"
        )
        self.logger.debug(f"Existing partitions: \n{existing_partitions}")

        return existing_partitions

    @dask.delayed
    def _get_new_partitions(self, source_partitions, existing_partitions, upstream: Delayed = None):

        self.logger.debug("Calculating new partitions..")

        new_partitions = [p for p in source_partitions if p not in existing_partitions]

        self.logger.info(
            f"Successfully calculated the list of {len(new_partitions)} new partition(s)"
        )
        self.logger.debug(f"New partitions: \n{new_partitions}")

        return new_partitions

    @dask.delayed
    def _cache_partitions(self, serializable: Any, file_name: str = "partitions.json"):
        url = os.path.join(self.s3_root_url, file_name)
        s3 = S3(url=url)

        self.logger.info(f"Copying {file_name} from memory to {url}...")

        s3.from_serializable(serializable)

    @dask.delayed
    def _get_cached_partitions(self, file_name: str):
        url = os.path.join(self.s3_root_url, file_name)
        s3 = S3(url=url)

        self.logger.debug("Retrieving cached partitions...")

        cached_partitions = s3.to_serializable()

        self.logger.info(
            f"Successfully retrieved the list of {len(cached_partitions)} cached partition(s)"
        )
        self.logger.debug(f"Cached partitions: \n{cached_partitions}")

        return cached_partitions

    @dask.delayed
    def filter_qf(self, query: str):
        queried = self.qf.copy().where(query, if_exists="append")
        return queried

    @dask.delayed
    def remove_table(self, schema, table):
        pass

    @dask.delayed
    @retry(Exception, tries=5, delay=5)
    def to_parquet(self, qf, path):
        # need to raise here as well for retries to work
        try:
            qf.to_parquet(path)
        except Exception:
            raise

    def _get_source_partitions(self, cache: str = "off", upstream=None) -> Delayed:
        """Generate a task that retrieves the list of partitions from source data"""
        if cache == "on":
            partitions = self._get_cached_partitions(file_name="partitions.json")
        else:
            partitions = self.__get_source_partitions()
        return partitions

    def _get_partition_cols_expr(self):
        if len(self.partition_cols) == 1:
            expr = self.partition_cols[0]
        else:
            partition_cols_casted = [f"CAST({col} AS VARCHAR)" for col in self.partition_cols]
            expr = "CONCAT(" + ", ".join(partition_cols_casted) + ")"
        return expr

    def _generate_partition_task(self, cache: str = "off") -> List[Delayed]:

        start = self.begin_extract()

        source_partitions = self._get_source_partitions(cache=cache, upstream=start)
        to_cache = self._cache_partitions(source_partitions) if cache == "on" else None

        if self.if_exists == "replace":
            partitions_to_download = source_partitions
        else:
            existing_partitions = self._get_existing_partitions()
            partitions_to_download = self._get_new_partitions(
                source_partitions, existing_partitions, upstream=to_cache
            )
        return partitions_to_download

    def _generate_extract_tasks(self, partitions) -> List[Delayed]:
        partition_cols_expr = self._get_partition_cols_expr()
        uploads = []
        for partition in partitions:
            partition_concatenated = partition.replace("|", "")
            file_name = f"{partition}.parquet"
            where = f"{partition_cols_expr}='{partition_concatenated}'"
            processed_qf = self.filter_qf(query=where)
            s3 = self.to_parquet(processed_qf, os.path.join(self.s3_staging_url, file_name))
            uploads.append(s3)

        external_table = self.create_external_table(upstream=uploads)

        if self.output_table_type == "base":
            regular_table = self.create_table(upstream=external_table)
            final_task = regular_table
        else:
            # TODO: do not create this task if the table already exists and if_exists != 'replace'
            final_task = external_table

        return [final_task]

    def generate_tasks(self, partitions_cache: str = "off", **kwargs,) -> List[List[Delayed]]:
        """Generate partitioning and extraction tasks

        Parameters
        ----------
        cache : bool, optional, by default True
            Whether to use caching for partitions list. This can be useful if partitions don't
            change often and the computation is expensive.
        Returns
        -------
        List[List[Delayed]]
            Two lists: one with tasks for generating the list of partitions and the other with
            tasks for extracting these partitions into 'self.output_dsn'
        """
        partition_task = self._generate_partition_task(cache=partitions_cache)
        # compute partitions on the cluster
        client = self._get_client()
        partitions = client.compute(partition_task).result()
        client.close()
        extract_tasks = self._generate_extract_tasks(partitions=partitions)

        return extract_tasks

    def register(
        self,
        registry: SchedulerDB = None,
        crons: List[str] = None,
        partitions_cache: str = "off",
        if_exists: str = "skip",
        **kwargs,
    ) -> bool:
        """Submit the partitions and/or extract job

        Parameters
        ----------
        kwargs: arguments to pass to Job.register()

        Examples
        ----------
        dev_registry = SchedulerDB(dev_scheduler_addr)
        Extract().register(registry=dev_registry, crons=["0 12 * * MON"], if_exists="replace")
        """
        tasks = self.generate_tasks(partitions_cache=partitions_cache)

        self.extract_job = Job(self.name, logger=self.logger, db=registry)
        self.extract_job.register(
            tasks=tasks, crons=crons, if_exists=if_exists, **kwargs,
        )
        return True

    def unregister(self, registry: SchedulerDB = None, remove_job_runs: bool = False) -> bool:
        self.extract_job.unregister(remove_job_runs=remove_job_runs)
        return True

    def submit(
        self, registry: SchedulerDB, reregister: bool = False, **kwargs,
    ):
        """Submit the extract job

        Parameters
        ----------
        kwargs: arguments to pass to Job.register() and Job.submit()

        """
        if reregister:
            if_exists = "replace"
        else:
            if_exists = "skip"
        self.register(registry=registry, if_exists=if_exists, **kwargs)
        self.extract_job.submit(
            scheduler_address=self.scheduler_address, priority=kwargs.get("priority")
        )
        return True

    def validate(self):
        pass


def Extract(name, qf, *args, **kwargs):

    if "sqlite" in qf.source.dsn:
        return SimpleExtract(name, qf, *args, **kwargs)

    db = config.get_service("sources")[qf.source.dsn]["db"]

    if db == "sfdc":
        return SFDCExtract(name, qf, *args, **kwargs)
    elif db == "denodo":
        return DenodoExtract(name, qf, *args, **kwargs)
    else:
        return SimpleExtract(name, qf, *args, **kwargs)
