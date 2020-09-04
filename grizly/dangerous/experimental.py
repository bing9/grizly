import json
import os
import dask
from ..tools.qframe import QFrame
from ..tools.s3 import S3
from ..scheduling.orchestrate import Workflow
import s3fs
import pyarrow.parquet as pq
from pyarrow import Table
import logging
from distributed import Client
from typing import List, Any, Union
from dask.delayed import Delayed
import gc
from ..config import config

JOBS_HOME = os.getenv("GRIZLY_WORKFLOWS_HOME", "home")

class Extract:
    def __init__(
        self,
        name: str,
        driver: QFrame,  # BaseDriver?
        store_backend: str = "local",
        store_path: str = None,
        data_backend: str = "s3",
        s3_bucket: str = None,
        s3_key: str = None,
        client_str: str = None,
        if_exists: str = "replace",
        reload_data: bool = True,
        output_external_schema: str = None,
        logger: logging.Logger = None,
        **kwargs,
    ):
        self.name = name
        self.module_name = self._to_snake_case(name)
        self.driver = driver
        self.store_backend = store_backend
        self.bucket = s3_bucket or config.get_service("s3")["bucket"]
        self.store_path = store_path
        self.s3_key = s3_key or f"extracts/{self._to_snake_case(name)}/"
        self.data_backend = data_backend
        self.client_str = client_str
        self.if_exists = if_exists
        self.data_if_exists = if_exists
        self.table_if_exists = self._map_if_exists(if_exists)
        self.reload_data = reload_data
        self.priority = 0
        self.output_external_schema = output_external_schema or os.getenv(
            "GRIZLY_EXTRACT_STAGING_EXTERNAL_SCHEMA"
        )
        self.logger = logger or self.driver.logger
        self.store = self.load_store()
        self._load_attrs_from_store(self.store)

    @staticmethod
    def _to_snake_case(text):
        return text.lower().replace(" - ", "_").replace(" ", "_")

    def _map_if_exists(self, if_exists):
        """ Map data-related if_exists to table-related commands """
        mapping = {"skip": "skip", "append": "skip", "replace": "drop"}
        return mapping[if_exists]

    def _get_client(self, client_str: Union[str, None] = None):
        if not client_str:
            client_str = self.client_str
        return Client(client_str)

    def _validate_store(self, store):
        pass

    def _load_attrs_from_store(self, store):
        self.partition_cols = store["partition_cols"]
        self.output_dsn = (
            store["output"].get("dsn") or self.driver.sqldb.dsn
        )  # this will only work for SQL drivers
        self.output_external_schema = self.output_external_schema or store["output"].get("external_schema")
        self.output_schema_prod = store["output"].get("schema") or os.getenv(
            "GRIZLY_EXTRACT_STAGING_SCHEMA"
        )
        self.output_external_table = store["output"].get("external_table") or self.module_name
        self.output_table_prod = store["output"].get("table") or self.module_name

    def load_store(self):
        if self.store_backend == "local":
            DEFAULT_STORE_PATH = os.path.join(JOBS_HOME, "workflows", self.module_name, "store.json")
            store_path = self.store_path or DEFAULT_STORE_PATH
            with open(store_path) as f:
                store = json.load(f)
        elif self.store_backend == "s3":
            DEFAULT_STORE_PATH = "s3://" + os.path.join(self.bucket, self.s3_key, "store.json")
            s3 = S3(url=DEFAULT_STORE_PATH)
            store = s3.to_serializable()
        else:
            raise NotImplementedError

        self._validate_store(store)

        return store

    @dask.delayed
    def get_distinct_values(self):
        def _validate_columns(columns: Union[str, List[str]], existing_columns: List[str]):
            """ Check whether the provided columns exist within the table """
            if isinstance(columns, str):
                column = [columns]
            for column in columns:
                # column = column.replace("sq.", "")
                if column not in existing_columns:
                    raise ValueError(f"QFrame does not contain {column}")

        columns = self.partition_cols
        existing_columns = self.driver.get_fields(aliased=True)
        _validate_columns(columns, existing_columns)

        self.logger.info(f"Obtaining the list of unique values in {columns}...")

        where = self.driver.data["select"]["where"]
        try:
            # faster method, but this will fail if user is working on multiple schemas/tables
            schema = self.driver.data["select"]["schema"]
            table = self.driver.data["select"]["table"]

            partitions_qf = (
                QFrame(dsn=self.driver.sqldb.dsn, logger=self.logger)
                .from_table(table=table, schema=schema, columns=columns)
                .query(where)
                .groupby()
            )
        except KeyError:
            qf_copy = self.driver.copy()
            partitions_qf = qf_copy.select(columns).groupby()

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

        s3 = S3(s3_key=self.s3_key + "data/staging/")
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
            s3 = S3(s3_key=self.s3_key, file_name=file_name)
            self.logger.info(f"Copying {file_name} from memory to {self.s3_key}...")
            s3.from_serializable(serializable)
        elif self.store_backend == "local":
            self.logger.info(f"Copying {file_name} from memory to {self.store_file_dir}...")
            with open(os.path.join(self.store_file_dir, file_name), "w") as f:
                json.dump(serializable, f)
        else:
            raise NotImplementedError

    @dask.delayed
    def get_cached_distinct_values(self, file_name: str):
        if self.store_backend == "s3":
            s3 = S3(s3_key=self.s3_key, file_name=file_name)
            values = s3.to_serializable()
        elif self.store_backend == "local":
            with open(os.path.join(self.store_file_dir, file_name)) as f:
                values = json.load(f)
        else:
            raise NotImplementedError
        return values

    @dask.delayed
    def query_driver(self, query: str):
        queried = self.driver.copy().query(query, if_exists="append")
        return queried

    @dask.delayed
    def to_arrow(self, driver: QFrame, partition: str = None):
        self.logger.info(f"Downloading data for partition {partition}...")
        pa = driver.to_arrow()
        self.logger.info(f"Data for partition {partition} has been successfully downloaded")
        gc.collect()
        return pa

    @dask.delayed
    def arrow_to_data_backend(self, arrow_table: Table, s3_key: str = None, file_name: str = None):
        def arrow_to_s3(arrow_table):
            def give_name(_):
                return file_name

            s3 = s3fs.S3FileSystem()
            file_name = s3_key.split("/")[-1]
            s3_key_root = (
                "s3://acoe-s3/" + s3_key[: s3_key.index(file_name) - 1]
            )  # -1 removes the last slash
            self.logger.info(f"Uploading {file_name} to {s3_key_root}...")
            pq.write_to_dataset(
                arrow_table,
                root_path=s3_key_root,
                filesystem=s3,
                use_dictionary=True,
                compression="snappy",
                partition_filename_cb=give_name,
            )
            self.logger.info(f"Successfully uploaded {file_name} to {s3_key}")

        if self.data_backend == "s3":
            arrow_to_s3(arrow_table)
        elif self.data_backend == "local":
            pq.write_table(arrow_table, os.path.join(self.store_file_dir, file_name))
        else:
            raise NotImplementedError

    @dask.delayed
    def create_external_table(self, upstream: Delayed = None):
        s3_key = self.s3_key + "data/staging/"
        if self.data_backend == "s3":
            self.driver.create_external_table(
                schema=self.output_external_schema,
                table=self.output_external_table,
                dsn=self.output_dsn,
                bucket=self.bucket,
                s3_key=s3_key,
                if_exists=self.table_if_exists,
            )
        else:
            raise ValueError("Exteral tables are only supported for S3 backend")

    @dask.delayed
    def create_table(self, upstream: Delayed = None):
        if self.data_backend == "s3":
            qf = QFrame(dsn=self.output_dsn, dialect="mysql", logger=self.logger).from_table(
                schema=self.output_external_schema, table=self.output_external_table
            )
            # qf.create_table(
            #     dsn=self.output_dsn,
            #     dialect="postgresql",
            #     schema=self.output_schema_prod,
            #     table=self.output_table_prod,
            #     if_exists=self.table_if_exists,
            # )
            qf.to_table(
                schema=self.output_schema_prod,
                table=self.output_table_prod,
                if_exists=self.data_if_exists,
            )
        else:
            # qf.to_table()
            raise NotImplementedError

    @dask.delayed
    def remove_table(self, schema, table):
        pass

    def generate_workflow(
        self,
        refresh_partitions_list: bool = True,
        if_exists: str = None,
        download_if_older_than: int = 0,
        cache_distinct_values: bool = True,
        output_table_type: str = "external",
        client_str: str = None,
        **kwargs,
    ):
        if if_exists is None:
            if_exists = self.if_exists
        if if_exists == "skip":
            raise NotImplementedError("Please choose one of ('append', 'replace')")

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

        if if_exists == "replace":
            if self.reload_data:
                partitions_to_download = all_partitions
            else:
                existing_partitions = self.get_existing_partitions()
                partitions_to_download = self.get_partitions_to_download(
                all_partitions, existing_partitions, upstream=cache_distinct_values_in_backend
            )
        else:
            existing_partitions = self.get_existing_partitions()
            partitions_to_download = self.get_partitions_to_download(
                all_partitions, existing_partitions, upstream=cache_distinct_values_in_backend
            )  # should return json obj

        # compute partitions on the cluster
        client = self._get_client(client_str)
        try:
            partitions = client.compute(partitions_to_download).result()
        except Exception:
            self.logger.exception("Something went wrong")
        finally:
            client.close()
        if not partitions:
            self.logger.warning("No partitions to download")

        if len(self.partition_cols) > 1:
            partition_cols_casted = [
                f"CAST({partition_col} AS VARCHAR)" for partition_col in self.partition_cols
            ]
            partition_cols = "CONCAT(" + ", ".join(partition_cols_casted) + ")"
        else:
            partition_cols = self.partition_cols[0]

        # create the workflow
        uploads = []
        for partition in partitions:
            partition_concatenated = partition.replace("|", "")
            s3_key = self.s3_key + "data/staging/" + f"{partition}.parquet"
            where = f"{partition_cols}='{partition_concatenated}'"
            # where_with_null = f"{partition_cols} IS NULL"
            # where = regular_where if partition_cols is not None else where_with_null
            processed_driver = self.query_driver(query=where)
            arrow_table = self.to_arrow(driver=processed_driver, partition=partition)
            push_to_backend = self.arrow_to_data_backend(arrow_table, s3_key=s3_key)
            uploads.append(push_to_backend)
        external_table = self.create_external_table(upstream=uploads)
        if output_table_type == "base":
            regular_table = self.create_table(upstream=external_table)
            # clear_spectrum = self.remove_table(self.output_schema, self.output_table, upstream=regular_table)
            final_task = regular_table
        else:
            final_task = external_table
        wf = Workflow(name=self.name, tasks=[final_task])
        self.workflow = wf
        return wf

    def submit(self, **kwargs):
        """Submit to cluster

        Parameters
        ----------
        kwargs: arguments to pass to Workflow.submit()

        Examples
        --------
        Extract().submit(scheduler_address="grizly_scheduler:8786")
        """
        client_str = kwargs.get("client_str")
        client = self._get_client(client_str)
        wf = self.generate_workflow(**kwargs)
        wf.submit(client, **kwargs)
        client.close()


    def validate(self):
        json1 = gen_json()
        qf1 = load_qf(json_path="")

    @dask.delayed
    def load_qf(self, dsn, json_path, subquery, upstream=None):
        qf = QFrame(dsn=self.input_dsn).from_json(json_path=json_path, subquery=subquery)
        return qf

        