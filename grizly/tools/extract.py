import datetime
import gc
import logging
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
from ..sources.sources_factory import Source
from ..store import Store
from ..utils.functions import chunker, retry
from ..utils.type_mappers import spectrum_to_redshift

s3 = s3fs.S3FileSystem()


class BaseExtract:
    """
    Common functions for all extracts. The logic for parallelization
    must be defined in the subclass
    """

    def __init__(
        self,
        qf: QFrame = None,
        name: str = None,
        staging_schema: str = None,
        staging_table: str = None,
        prod_schema: str = None,
        prod_table: str = None,
        output_dsn: str = None,
        output_dialect: str = None,
        output_source_name: str = None,
        output_table_type: str = "external",
        s3_root_url: str = None,
        s3_bucket: str = None,
        dask_scheduler_address: str = None,
        if_exists: str = "append",
        priority: int = 0,
        **kwargs,
    ):
        self.qf = qf
        self.name = name
        self.staging_schema = staging_schema
        self.staging_table = staging_table
        self.prod_schema = prod_schema
        self.prod_table = prod_table
        self.output_dsn = output_dsn
        self.output_dialect = output_dialect
        self.output_source_name = output_source_name
        self.s3_root_url = s3_root_url
        self.s3_bucket = s3_bucket
        self.dask_scheduler_address = dask_scheduler_address
        self.output_table_type = output_table_type
        self.if_exists = if_exists
        self.priority = priority

        self._load_attrs()

    @classmethod
    def from_json(cls, path, key="extract"):
        store = Store.from_json(json_path=path, key=key)
        dsn = store.qframe.select.source.get("dsn")
        name = store.get("name")
        logger = logging.getLogger("distributed.worker").getChild(name)
        qf = QFrame(dsn=dsn, logger=logger).from_dict(store.qframe)
        extract_params = {key: val for key, val in store.items() if key != "qframe" and val != ""}

        return cls(qf=qf, **extract_params)

    def __str__(self):
        attrs = {k: val for k, val in self.__dict__.items() if not str(hex(id(val))) in str(val)}
        _repr = ""
        for attr, value in attrs.items():
            line_len = len(attr)
            attr_val = self._get_attr_val(attr, init_val=value)
            if attr == "qf":
                attr_val_str = self.qf.__class__.__name__
            else:
                attr_val_str = str(attr_val)
            if attr_val_str == "None":
                from termcolor import colored

                line_len -= 9

                attr_val_str = colored(attr_val_str, "red")
                attr = colored(attr, "red")

            _repr += attr + " " + attr_val_str.rjust(80 - line_len) + "\n"
        return _repr

    def _get_attr_val(self, attr, init_val):
        attr_env = f"GRIZLY_EXTRACT_{attr.upper()}"
        # attr_val = (
        #     init_val or self.store.get(attr) or config.get_service(attr) or os.getenv(attr_env)
        # )
        attr_val = (
            init_val
            # or self.store.get(attr)
            # or config.get_service(attr)
            or os.getenv(attr_env)
        )
        # print(attr, attr_val)
        return attr_val

    def _load_attrs(self):
        """Load attributes. Looking in init->store->config->env variables"""

        attrs = {k: val for k, val in self.__dict__.items() if not str(hex(id(val))) in str(val)}
        for attr, value in attrs.items():
            if attr != "qf":
                setattr(self, attr, self._get_attr_val(attr, init_val=value))

        # use automated defaults
        self.name_snake_case = self._to_snake_case(self.name)
        self.staging_table = self.staging_table or self.name_snake_case
        # TODO: remove -- should be read automatically above once the structure is improved
        self.s3_bucket = self.s3_bucket or config.get_service("s3").get("bucket")
        self.s3_root_url = (
            self.s3_root_url or f"s3://{self.s3_bucket}/extracts/{self.name_snake_case}/"
        )
        self.output_source = Source(
            dsn=self.output_dsn, dialect=self.output_dialect, source=self.output_source_name
        )
        self.s3_staging_url = os.path.join(self.s3_root_url, "data", "staging")
        self.table_if_exists = self._map_if_exists(self.if_exists)
        self.logger = logging.getLogger("distributed.worker").getChild(self.name_snake_case)
        self.qf.logger = self.logger

    @staticmethod
    def _to_snake_case(text):
        return text.lower().replace(" - ", "_").replace(" ", "_")

    @staticmethod
    def _map_if_exists(if_exists):
        """ Map data-related if_exists to table-related commands """
        mapping = {"skip": "skip", "append": "skip", "replace": "drop"}
        return mapping[if_exists]

    def _get_client(self):
        return Client(self.dask_scheduler_address)

    @dask.delayed
    def begin_extract(self):
        self.logger.info("Starting the extract process...")

    @staticmethod
    @dask.delayed
    def fix_qf_types(qf, upstream=None):
        qf_c = qf.copy()
        return qf_c.fix_types()

    @dask.delayed
    @retry(Exception, tries=5, delay=5)
    def to_parquet(self, qf, path, upstream=None):
        # need to raise here as well for retries to work
        try:
            qf.to_parquet(path)
        except Exception:
            raise
        return qf

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
    def create_external_table(self, qf, upstream: Delayed = None):
        """Use processed QF rather than self.qf, as the types can be modified by to_arrow()"""
        qf.create_external_table(
            schema=self.staging_schema,
            table=self.staging_table,
            output_source=self.output_source,
            s3_url=self.s3_staging_url,
            if_exists=self.table_if_exists,
        )

    @dask.delayed
    def create_table(self, upstream: Delayed = None):
        """Create and populate a table"""
        qf = QFrame(
            source=self.output_source,
            schema=self.staging_schema,
            table=self.staging_table,
            logger=self.logger,
        )
        mapped_types = [spectrum_to_redshift(dtype) for dtype in qf.dtypes]
        qf.source.create_table(
            schema=self.prod_schema,
            table=self.prod_table,
            columns=qf.get_fields(aliased=True),
            types=mapped_types,
            if_exists="drop",  # always re-create from the external table
        )
        qf.source.write_to(
            schema=self.output_schema_prod,
            table=self.output_table_prod,
            columns=qf.get_fields(aliased=True),
            sql=qf.get_sql(),
            if_exists="append",
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

    def submit(self, registry, reregister: bool = False, **kwargs):
        """Submit the extract job

        Parameters
        ----------
        kwargs: arguments to pass to Job.register() and Job.submit()

        """
        if_exists = "replace" if reregister else "skip"
        self.register(registry=registry, if_exists=if_exists)
        self.extract_job.submit(scheduler_address=self.dask_scheduler_address, **kwargs)
        return True


class SimpleExtract(BaseExtract):
    """"Extract data in single piece"""

    def generate_tasks(self):
        file_name = self.name_snake_case + ".parquet"
        path = os.path.join(self.s3_root_url, file_name)

        start = self.begin_extract()
        qf_fixed = self.fix_qf_types(self.qf, upstream=start)
        s3 = self.to_parquet(qf=qf_fixed, path=path)
        external_table = self.create_external_table(qf=qf_fixed, upstream=s3)
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

        qf_fixed = self.fix_qf_types(self.qf, upstream=wipe_staging)

        s3_uploads = []
        batch_no = 1
        for url_chunk in chunks:
            first_url_pos = url_chunk[0].split("-")[1]
            last_url_pos = url_chunk[-1].split("-")[1]
            file_name = f"{first_url_pos}-{last_url_pos}.parquet"
            arrow_table = self.urls_to_arrow(
                qf=qf_fixed, urls=url_chunk, batch_no=batch_no, upstream=wipe_staging
            )
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
        self, qf: QFrame, urls: List[str], batch_no: int, upstream: dask.delayed = None
    ) -> pa.Table:
        self.logger.info(f"Converting batch no. {batch_no} into a pyarrow table...")
        start = datetime.datetime.now()
        records = []
        for url in urls:
            records_chunk: List[tuple] = qf.source._fetch_records_url(url)
            redords_chunk_processed: List[tuple] = qf._cast_records(records_chunk)
            records.extend(redords_chunk_processed)

        cols = qf.get_fields(aliased=True)

        _dict = {col: [] for col in cols}
        for record in records:
            for i, col_val in enumerate(record):
                col_name = cols[i]
                _dict[col_name].append(col_val)

        arrow_table = qf._dict_to_arrow(_dict)

        duration = datetime.datetime.now() - start
        seconds = duration.seconds
        msg = f"Pyarrow table for batch no. {batch_no} has been successfully generated in {seconds} second(s)."
        self.logger.info(msg)
        gc.collect()
        return arrow_table


class DenodoExtract(BaseExtract):
    def __init__(
        self, qf, partition_cols, *args, **kwargs,
    ):
        super().__init__(qf, *args, **kwargs)
        self.partition_cols = partition_cols

    def _validate_store(self, store):
        pass

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

    @staticmethod
    @dask.delayed
    def filter_qf(qf: QFrame, query: str) -> QFrame:
        queried = qf.copy().where(query, if_exists="append")
        return queried

    @dask.delayed
    def remove_table(self, schema, table):
        pass

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
        qf_fixed = self.fix_qf_types(self.qf)
        for partition in partitions:
            partition_concatenated = partition.replace("|", "")
            file_name = f"{partition}.parquet"
            where = f"{partition_cols_expr}='{partition_concatenated}'"
            processed_qf = self.filter_qf(qf=qf_fixed, query=where)
            s3 = self.to_parquet(processed_qf, os.path.join(self.s3_staging_url, file_name))
            uploads.append(s3)

        external_table = self.create_external_table(qf=qf_fixed, upstream=uploads)

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
        remove_job_runs: bool = False,
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
            tasks=tasks,
            crons=crons,
            if_exists=if_exists,
            remove_job_runs=remove_job_runs,
            **kwargs,
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
            scheduler_address=self.dask_scheduler_address, priority=kwargs.get("priority")
        )
        return True

    def validate(self):
        pass


class Extract:
    def __new__(cls, **kwargs):
        source_name = kwargs.get("qf").source.__class__.__name__.lower()
        if source_name == "sfdb":
            return SFDCExtract(**kwargs)
        elif source_name == "denodo":
            return DenodoExtract(**kwargs)
        else:
            return SimpleExtract(**kwargs)

    @classmethod
    def from_json(self, store_path, key="extract"):

        store = Store.from_json(store_path, key=key)
        source_name = store.qframe.select.source.get("source_name")

        if source_name == "sfdb":
            return SFDCExtract.from_json(store_path, key=key)
        elif source_name == "denodo":
            return DenodoExtract.from_json(store_path, key=key)
        else:
            return SimpleExtract.from_json(store_path, key=key)
