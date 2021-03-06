import datetime
import gc
import logging
import os
from abc import abstractmethod
from typing import Any, List, Optional, Union

import dask
import pandas as pd
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

s3 = s3fs.S3FileSystem()
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)


class BaseExtract:
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
        autocast: bool = True,
        s3_root_url: str = None,
        s3_bucket: str = None,
        dask_scheduler_address: str = None,
        if_exists: str = "append",
        priority: int = -1,
        validation: dict = None,
        **kwargs,
    ):
        """Base Extract class. The logic for parallelization must be defined in the subclass.

        Parameters
        ----------
        qf : QFrame, optional
            The QFrame containing a query on the source table, by default None
        name : str, optional
            The name of the extract, by default None
        staging_schema : str, optional
            Staging schema, by default None
        staging_table : str, optional
            Staging table, by default None
        prod_schema : str, optional
            The schema where data will be moved after passing validations, by default None
        prod_table : str, optional
            The table where data will be moved after passing validations, by default None
        output_dsn : str, optional
            The DSN used for connecting to the output Source, by default None
        output_dialect : str, optional
            The dialect used for connecting to the output Source, by default None
        output_source_name : str, optional
            The name used for connecting to the output Source, by default None
        autocast : bool, optional
            Whether to automatically infer data types from parquet files
            (this works similar to a Glue crawler). This is required because:
            1) in some cases, original data types must be casted when dumping to parquet,
            2) spectrum is incompatible with itself, ie. the Spectrum data types cannot be used
            when creating spectrum tables, by default True
        s3_root_url : str, optional
            The root URL of the project, eg. s3://my_bucket/extracts, by default None
        s3_bucket : str, optional
            S3 bucket, by default None
        dask_scheduler_address : str, optional
            The address of the Dask scheduler where the computation should be executed,
            by default None
        if_exists : str, optional
            What to do with the files and tables if they exist, by default "append"
        priority : int, optional
            Priority of the computation on the Dask cluster, by default -1
        store : Store, optional
            A store to create the store from. Typically you'd use Exract.from_json(),
            by default None
        validation: dict, optional
            Definition of the validation to perform after a successful run.
            Required keys: 'groupby', 'sum'. Optional key: 'max_allowed_diff_percent', by default 1.
        """
        self.qf = qf
        self.name = name
        self.staging_schema = staging_schema
        self.staging_table = staging_table
        self.prod_schema = prod_schema
        self.prod_table = prod_table
        self.output_dsn = output_dsn
        self.output_dialect = output_dialect
        self.output_source_name = output_source_name
        self.autocast = autocast
        self.s3_root_url = s3_root_url
        self.s3_bucket = s3_bucket
        self.dask_scheduler_address = dask_scheduler_address
        self.if_exists = if_exists
        self.priority = priority
        self.validation = validation

        self._load_attrs()

    @classmethod
    def from_json(cls, path, key="extract"):
        store = Store.from_json(json_path=path, key=key)
        dsn = store.qframe.select.source.get("dsn")
        name = store.get("name")
        logger = logging.getLogger(name)
        qf = QFrame(dsn=dsn, logger=logger).from_dict(store.qframe)
        extract_params = {
            key: val for key, val in store.items() if key != "qframe" and val is not None
        }
        return cls(qf=qf, store=store, **extract_params)

    def __str__(self):

        from termcolor import colored  # change to _repr_html?

        attrs = {k: val for k, val in self.__dict__.items() if not str(hex(id(val))) in str(val)}
        _repr = ""
        for attr, value in attrs.items():
            line_len = len(attr)
            attr_val = self._get_attr_val(attr, init_val=value)
            if attr == "qf":
                attr_val_str = self.qf.__class__.__name__
            elif attr == "partitions" and self.partitions:
                attr_val_str = str(self.partitions)
            else:
                attr_val_str = str(attr_val)
            # trim output to 50 chars
            attr_val_str = attr_val_str[:50] + "..." if len(attr_val_str) > 50 else attr_val_str
            if attr_val_str == "None":

                line_len -= 9

                attr_val_str = colored(attr_val_str, "yellow")
                attr = colored(attr, "yellow")

            _repr += attr + " " + attr_val_str.rjust(80 - line_len) + "\n"
        return _repr

    def _get_attr_val(self, attr, init_val):

        NON_ENV_PARAMS = ("qf", "store")
        if attr in NON_ENV_PARAMS:
            return init_val

        attr_env = f"GRIZLY_EXTRACT_{attr.upper()}"
        attr_val = init_val if init_val is not None else os.getenv(attr_env)

        return attr_val

    def _load_attrs(self):
        """Load attributes from init and env variables."""

        attrs = {k: val for k, val in self.__dict__.items() if not str(hex(id(val))) in str(val)}
        for attr, value in attrs.items():
            if attr != "qf":
                setattr(self, attr, self._get_attr_val(attr, init_val=value))

        # use automated defaults
        self.name_snake_case = self._to_snake_case(self.name)
        self.staging_table = self.staging_table or self.name_snake_case
        self.prod_table = self.prod_table or self.name_snake_case
        # TODO: remove below line -- should be read by _get_attr_val()
        self.s3_bucket = self.s3_bucket or config.get_service("s3").get("bucket")
        self.s3_root_url = (
            self.s3_root_url or f"s3://{self.s3_bucket}/extracts/{self.name_snake_case}/"
        )
        self.output_source = Source(
            dsn=self.output_dsn, dialect=self.output_dialect, source=self.output_source_name
        )
        self.s3_staging_url = os.path.join(self.s3_root_url, "data", "staging")
        self.s3_prod_url = os.path.join(self.s3_root_url, "data", "prod")
        self.table_if_exists = self._map_if_exists(self.if_exists)
        # self.logger = logging.getLogger("distributed.worker").getChild(self.name_snake_case)
        self.logger = logging.getLogger(self.name_snake_case)
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

    @dask.delayed
    def fix_qf_types(self, upstream=None):
        qf_fixed = self.qf.copy().fix_types()
        return qf_fixed

    @dask.delayed
    @retry(Exception, tries=5, delay=5)
    def to_parquet(self, qf, file_name=None, upstream=None):

        self.logger.info("Downloading data to parquet...")

        path = os.path.join(self.s3_staging_url, file_name)

        # need to raise here as well for retries to work
        try:
            qf.to_parquet(path)
        except Exception:
            raise

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
    def empty_s3_dir(self, url, upstream=None):

        if upstream is False:
            self.logger.warning("Received SKIP signal. Skipping 'empty_s3_dir()'...")
            return False

        self.logger.info(f"Emptying {url}...")

        try:
            s3.rm(url, recursive=True)
            self.logger.info(f"{url} has been successfully emptied.")
        except FileNotFoundError:
            self.logger.warning(f"Couldn't empty {url} as it doesn't exist")

    @dask.delayed
    def create_external_table(self, qf, schema, table, s3_url, upstream: Delayed = None):
        """Use processed QF rather than self.qf, as the types can be modified by to_arrow()"""
        qf.create_external_table(
            schema=schema,
            table=table,
            output_source=self.output_source,
            s3_url=s3_url,
            if_exists=self.table_if_exists,
        )

    @abstractmethod
    def extract(self):
        pass

    def register(
        self,
        registry: Optional[SchedulerDB] = None,
        redis_host: Optional[str] = None,
        redis_port: Optional[int] = 6379,
        remove_job_runs=False,
        **kwargs,
    ):
        """Submit the partitions and/or extract job

        Parameters
        ----------
        kwargs: arguments to pass to extract_job.register()

        Examples
        ----------
        dev_registry = SchedulerDB(dev_scheduler_addr)
        e = Extract.from_json("store.json")
        e.register(registry=dev_registry, crons="0 12 * * MON", if_exists="replace")  # Mondays 12 AM
        """
        registry = registry or SchedulerDB(
            logger=self.logger, redis_host=redis_host, redis_port=redis_port
        )
        self.extract_job = Job(self.name, logger=self.logger, db=registry)
        self.extract_job.register(func=self.extract, remove_job_runs=remove_job_runs, **kwargs)

        self.validation_job = Job(self.name + " - validation", logger=self.logger, db=registry)
        self.validation_job.register(
            func=self.validate,
            owner="system",
            description=f"{self.name} extract validation",
            if_exists="replace",
            remove_job_runs=False,
            upstream={self.name: "success"},
        )

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
        self.register(registry=registry, if_exists=if_exists, **kwargs)
        self.extract_job.submit(scheduler_address=self.dask_scheduler_address)
        return True

    def run(self, **kwargs):
        self.extract_job.submit(scheduler_address=self.dask_scheduler_address, **kwargs)
        return True

    @dask.delayed
    def _compare_dfs(
        self,
        df1: pd.DataFrame,
        df2: pd.DataFrame,
        sort_by: Union[str, List[str]] = None,
        max_allowed_diff_percent: float = 1,
    ) -> pd.DataFrame:
        """Compare the sums of all numeric columns of df1 and df2 and show mismatches

        Parameters
        ----------
        df1 : DataFrame
            First df
        df2 : DataFrame
            Second df
        sort_by: Union[str, List[str]], optional
            The column(s) by which to sort the rows for comparison
        max_allowed_diff_percent : float, optional
            Allowed percentage difference per row, by default 1%
        """

        self.logger.info("Validating data...")

        if (df1.sum() == df2.sum()).all():
            self.logger.info("Amounts match.")
            return

        df1 = df1.copy()
        df2 = df2.copy()

        df1 = df1.sort_values(by=sort_by).reset_index(drop=True)
        df2 = df2.sort_values(by=sort_by).reset_index(drop=True)

        # make sure rows are in the same order, so we compare apples to apples
        assert df1[sort_by].values.tolist() == df2[sort_by].values.tolist()

        df1_numeric = df1.select_dtypes("number")
        df2_numeric = df2.select_dtypes("number")

        show_diffs = False
        for col in df1_numeric:
            col_diff = abs(df1_numeric[col] - df2_numeric[col])
            for row_no, row_diff in enumerate(col_diff):
                row_value = df1_numeric.loc[row_no, col]
                if (row_diff / row_value) > (max_allowed_diff_percent / 100):
                    show_diffs = True

        if show_diffs:
            non_numeric_cols = [
                col for col in df1.columns if col not in df1.select_dtypes("number").columns
            ]
            df_non_numeric = df1[non_numeric_cols]
            diffs = abs(df1_numeric - df2_numeric)
            total = df1_numeric.sum().sum() + df2_numeric.sum().sum()
            total_str = f"${(total/1e6):.2f}"
            total_diff = diffs.sum().sum()
            total_diff_str = f"${(total_diff/1e6):.2f}"
            diffs_pretty = diffs.applymap("${0:,.0f}".format)
            diff_df = pd.concat([df_non_numeric, diffs_pretty], axis=1)
            total_col_diffs_str = ""
            for col, val in zip(diffs.columns, diffs.sum()):
                col_diff_str = f"{col}: ${(val/1e6):.2f} M"
                total_col_diffs_str += col_diff_str + "\n"
            msg = (
                f"Amounts are different by a total of {total_diff_str} M, out of {total_str} M."
                + "\n\n"
            )
            msg += "Differences per column:" + "\n"
            msg += f"{total_col_diffs_str}" + "\n"
            msg += "Differences per row:" + "\n"
            msg += f"{diff_df.to_markdown(index=False)}"
            self.logger.error(msg)
            is_valid = False
        else:
            self.logger.info("Data has been successfully validated.")
            is_valid = True
        return is_valid

    def _process_validation_qf(self, qf, where, groupby_cols, sum_cols):
        qf_filtered = qf.copy().where(where)
        qf_grouped = qf_filtered.groupby(groupby_cols)
        qf_final = qf_grouped[sum_cols].agg("sum")
        return qf_final

    @dask.delayed
    @retry(Exception, tries=3, delay=10)
    def get_df(self, qf):
        try:
            df = qf.to_df()
        except Exception:
            # trigger retry
            raise
        return df

    @dask.delayed
    def staging_to_prod(self, upstream=None):

        if upstream is False:
            self.logger.warning("Received SKIP signal. Skipping 'staging_to_prod()'...")
            return False

        self.logger.info("Moving data to prod...")

        urls = s3.ls(self.s3_staging_url)
        for url in urls[1:]:  # [0] is the dir itself
            prod_url = url.replace("staging", "prod")
            s3.cp(url, prod_url)

        self.logger.info("Parquet files have been successfully moved to prod.")
        qf = QFrame(
            source=Source(dsn=self.output_dsn, dialect="spectrum"),
            schema=self.staging_schema,
            table=self.staging_table,
            logger=self.logger,
        )
        qf.create_external_table(
            schema=self.prod_schema,
            table=self.prod_table,
            output_source=self.output_source,
            s3_url=self.s3_prod_url,
            if_exists=self.table_if_exists,
        )
        return True

    @dask.delayed
    def finish_validation(self, upstream=None):
        if upstream is False:
            self.logger.warning("Validation finished with the status fail.")
            return False
        self.logger.info("Validation finished with the status success.")

    def validate(self, max_allowed_diff_percent=1):

        if not self.validation:
            self.logger.warning(f"No validation strategy specified for {self.name}.")
            return

        max_allowed_diff_percent = (
            self.validation.get("max_allowed_diff_percent") or max_allowed_diff_percent
        )

        groupby_cols = self.validation.get("groupby")
        sort_by = groupby_cols if len(groupby_cols) > 1 else groupby_cols[0]

        sum_cols = self.validation.get("sum")
        cols = groupby_cols + sum_cols
        where = self.qf.store.select.get("where")

        in_schema = self.qf.store.select.get("schema")
        in_table = self.qf.store.select.get("table")

        out_schema = self.staging_schema
        out_table = self.staging_table

        in_qf = QFrame(
            dsn=self.qf.source.dsn,
            schema=in_schema,
            table=in_table,
            columns=cols,
            logger=self.logger,
        )
        out_qf = QFrame(
            dsn=self.output_dsn,
            schema=out_schema,
            table=out_table,
            columns=cols,
            logger=self.logger,
        )

        in_qf_processed = self._process_validation_qf(in_qf, where, groupby_cols, sum_cols)
        out_qf_processed = self._process_validation_qf(out_qf, where, groupby_cols, sum_cols)

        # build dask graph
        in_df = self.get_df(in_qf_processed)
        out_df = self.get_df(out_qf_processed)

        validate = self._compare_dfs(
            in_df, out_df, sort_by=sort_by, max_allowed_diff_percent=max_allowed_diff_percent
        )

        empty_prod = self.empty_s3_dir(self.s3_prod_url, upstream=validate)
        to_prod = self.staging_to_prod(upstream=empty_prod)

        end = self.finish_validation(upstream=to_prod)

        return dask.delayed()([end], name=self.name + " (validation)").compute()


class SimpleExtract(BaseExtract):
    """"Extract data in single piece"""

    def extract(self) -> None:
        file_name = self.name_snake_case + ".parquet"

        self.logger.info("Generating tasks...")

        start = self.begin_extract()
        qf_fixed = self.fix_qf_types(upstream=start)
        s3 = self.to_parquet(qf=qf_fixed, file_name=file_name)
        external_table_staging = self.create_external_table(
            qf=qf_fixed,
            schema=self.staging_schema,
            table=self.staging_table,
            s3_url=self.s3_staging_url,
            upstream=s3,
        )

        self.logger.info("Tasks have been successfully generated.")

        self.logger.info("Submitting to Dask...")
        graph = dask.delayed()([external_table_staging], name=self.name)
        client = self._get_client()
        client.compute(graph)
        client.close()
        self.logger.info("Tasks have been successfully submitted to Dask.")


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

    def extract(self):

        self.logger.info("Generating tasks...")

        start = self.begin_extract()

        urls = self.get_urls(upstream=start)
        url_chunks = self.chunk(urls, chunksize=50)
        client = self._get_client()
        chunks = client.compute(url_chunks).result()
        client.close()

        if self.if_exists == "replace":
            wipe_staging = self.empty_s3_dir(url=self.s3_staging_url)
        else:
            wipe_staging = None

        qf_fixed = self.fix_qf_types(upstream=wipe_staging)

        if chunks:
            # extract `chunksize` URLs at a time
            s3_uploads = []
            batch_no = 1
            for url_chunk in chunks:
                first_url_pos = url_chunk[0].split("-")[1]
                last_url_pos = url_chunk[-1].split("-")[1]
                file_name = f"{first_url_pos}-{last_url_pos}.parquet"
                arrow_table = self.urls_to_arrow(qf=qf_fixed, urls=url_chunk, batch_no=batch_no)
                to_s3 = self.arrow_to_s3(arrow_table, file_name=file_name)
                s3_uploads.append(to_s3)
                batch_no += 1
        else:
            # extract the table into a single parquet file
            to_s3 = self.to_parquet(qf_fixed, file_name="1.parquet")
            s3_uploads = [to_s3]

        external_table_staging = self.create_external_table(
            qf=qf_fixed,
            schema=self.staging_schema,
            table=self.staging_table,
            s3_url=self.s3_staging_url,
            upstream=s3_uploads,
        )

        self.logger.info("Tasks have been successfully generated.")

        self.logger.info("Submitting to Dask...")
        return dask.delayed()([external_table_staging], name=self.name + " graph").compute()

    @dask.delayed
    def get_urls(self, upstream=None):
        self.logger.info("Obtaining the list of URLs...")
        return self.qf.source._get_urls_from_response(query=self.qf.get_sql())

    @dask.delayed
    def chunk(self, iterable, chunksize):
        self.logger.info("Generating URL batches...")
        chunks = chunker(iterable, size=chunksize)
        self.logger.info("URL batches have been successfully generated.")
        return chunks

    @dask.delayed
    def qf_to_arrow(qf: QFrame) -> pa.Table:
        return qf.to_arrow()

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
        self, qf, partition_cols, partitions: List[Any] = None, *args, **kwargs,
    ):
        super().__init__(qf, *args, **kwargs)
        self.partition_cols = partition_cols
        self.partitions = partitions

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

        schema = self.qf.store["select"]["schema"]
        table = self.qf.store["select"]["table"]
        where = self.qf.store["select"]["where"]

        partitions_qf = (
            QFrame(dsn=self.qf.source.dsn, table=table, schema=schema, columns=columns)
            .where(where)
            .groupby()
        )
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

        wipe_staging = (
            self.empty_s3_dir(self.s3_staging_url) if self.if_exists == "replace" else None
        )

        if self.autocast:
            qf_fixed = self.fix_qf_types(upstream=wipe_staging)
        else:
            qf_fixed = self.qf

        uploads = []
        for partition in partitions:
            partition_concatenated = partition.replace("|", "")
            file_name = f"{partition}.parquet"
            if partition == "None":
                where = f"{partition_cols_expr} IS NULL"
            else:
                where = f"{partition_cols_expr}='{partition_concatenated}'"
            processed_qf = self.filter_qf(qf=qf_fixed, query=where)
            s3 = self.to_parquet(processed_qf, file_name=file_name)
            uploads.append(s3)

        # TODO: do not create this task if the table already exists and if_exists != 'replace'
        external_table_staging = self.create_external_table(
            qf=qf_fixed,
            schema=self.staging_schema,
            table=self.staging_table,
            s3_url=self.s3_staging_url,
            upstream=uploads,
        )

        return [external_table_staging]

    def extract(self, partitions_cache: str = "off") -> None:
        """Generate partitioning and extraction tasks

        Parameters
        ----------
        partitions_cache : str, optional, by default "off"
            Whether to use caching for partitions list. This can be useful if partitions don't
            change often and the computation is expensive.
        Returns
        -------
        List[List[Delayed]]
            Two lists: one with tasks for generating the list of partitions and the other with
            tasks for extracting these partitions into 'self.output_dsn'
        """

        self.logger.info("Generating tasks...")

        if not self.partitions:
            partition_task = self._generate_partition_task(cache=partitions_cache)
            # compute partitions on the cluster
            client = self._get_client()
            partitions = client.compute(partition_task).result()
            client.close()
        else:
            partitions = self.partitions
        extract_tasks = self._generate_extract_tasks(partitions=partitions)

        self.logger.info("Tasks have been successfully generated.")

        self.logger.info("Submitting to Dask...")
        return dask.delayed()([extract_tasks], name=self.name + " graph").compute()


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
