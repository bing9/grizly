import json
import os
import dask
from grizly import S3, Workflow
import s3fs
import pyarrow.parquet as pq


class Extract:
    __allowed = ("store_file_dir", "store_file_name", "s3_key")

    def __init__(self, tool, backend="local", logger=None, **kwargs):
        self.tool = tool
        self.backend = backend
        self.module_name = "".join(os.path.basename(__file__).split(".")[:-1])
        self.priority = 0
        self.scheduler_address = "grizly_scheduler:8786"
        self.client = None
        self.logger = logger or logging.getLogger("distributed.worker").getChild(self.module_name)
        for k, v in kwargs.items():
            if not (k in self.__class__.__allowed):
                raise ValueError(f"{k} parameter is not allowed")
            setattr(self, k, v)
        self.load_store()

    def _validate_store(self, store):
        pass

    def load_store(self):
        if getattr(self, "store_file_name", None) is None:
            self.store_file_name = "store.json"
            self.logger.warning("'store_file_name' was not provided.\n" f"Attempting to load from {self.store_file_name}...")

        if self.backend == "local":
            if getattr(self, "store_file_dir", None) is None:
                self.store_file_dir = os.path.dirname(__file__)
                self.logger.warning(
                    "'store_file_dir' was not provided but backend is set to 'local'.\n"
                    f"Attempting to load {self.store_file_name} from {self.store_file_dir or 'current directory'}..."
                )
            file_path = os.path.join(self.store_file_dir, self.store_file_name)
            with open(file_path) as f:
                store = json.load(f)

        elif self.backend == "s3":
            if getattr(self, "s3_key", None) is None:
                self.s3_key = f"extracts/{self.module_name}/"
                self.logger.warning(
                    "'s3_key' was not provided but backend is set to 's3'.\n"
                    f"Attempting to load {self.store_file_name} from {self.s3_key}..."
                )
            s3 = S3(s3_key=self.s3_key, file_name=self.store_file_name)
            store = s3.to_json()
        else:
            raise NotImplementedError

        self._validate_store(store)
        self.name = store["metadata"]["name"]
        self.partition_cols = store["partition_cols"]

        return store

    @dask.delayed
    def get_distinct_values(self):
        def _validate_columns(columns, existing_columnns):
            """ Check whether the provided columns exist within the table """
            if isinstance(columns, str):
                column = columns
                if column not in existing_columns:
                    raise ValueError(f"QFrame does not contain {column}")
            elif isinstance(columns, list):
                for column in columns:
                    if column not in existing_columns:
                        raise ValueError(f"QFrame does not contain {column}")

        columns = self.partition_cols
        existing_columns = self.tool.get_fields()
        _validate_columns(columns, existing_columns)
        qf_copy = self.tool.copy()

        self.logger.info(f"Obtaining the list of unique values in {columns}...")

        if isinstance(columns, str):
            column = columns
            to_select = column
        elif isinstance(columns, list):
            partition_col = "CONCAT(" + ", ".join(columns) + ")"
            qf_copy.assign(partition_column=partition_col)
            to_select = "partition_column"
        else:
            raise ValueError(f"columns must be one of: str, list")

        records = qf_copy.select(to_select).distinct().to_records()
        values = [row[0] for row in records]

        self.logger.info(f"Successfully obtained the list of unique values in {columns}")
        self.logger.debug(f"Unique values in {columns}: {values}")

        return values

    @dask.delayed
    def get_existing_partitions(self, upstream=None):
        """ Returns partitions already uploaded to S3 """

        self.logger.info("Starting the extract process...")

        s3 = S3(s3_key=self.s3_key)
        existing_partitions = []
        for file_name in s3.list():
            extension = file_name.split(".")[-1]
            if extension == "parquet":
                existing_partitions.append(file_name.replace(".parquet", ""))

        self.logger.info(f"Successfully obtained the list of existing partitions")
        self.logger.debug(f"Existing partitions: {existing_partitions}")

        return existing_partitions

    @dask.delayed
    def get_partitions_to_download(self, all_partitions, existing_partitions):
        existing_partitons_normalized = [partition.replace(".", "") for partition in existing_partitions]
        self.logger.debug(f"All partitions: {all_partitions}")
        self.logger.debug(f"Existing partitions: {existing_partitons_normalized}")
        partitions_to_download = [partition for partition in all_partitions if partition not in existing_partitons_normalized]
        self.logger.debug(f"Partitions to download: {len(partitions_to_download)}, {partitions_to_download}")
        return partitions_to_download

    @dask.delayed
    def to_backend(self, serializable, file_name):
        if self.backend == "s3":
            s3 = S3(s3_key=self.s3_key, file_name=file_name)
            s3.from_serializable(serializable)
        else:
            raise NotImplementedError

    @dask.delayed
    def get_cached_distinct_values(self, file_name):
        if self.backend == "s3":
            s3 = S3(s3_key=self.s3_key, file_name=file_name)
            _dict = s3.to_json()
        else:
            raise NotImplementedError
        return _dict

    @dask.delayed
    def query_tool(self, query):
        tool_copy = self.tool.copy()
        return tool_copy.query(query)

    @dask.delayed
    def to_arrow(self, processed_tool):
        return processed_tool.to_arrow()  # qf.to_arrow(), sfdc.to_arrow()

    @dask.delayed
    def arrow_to_backend(self, arrow_table, s3_key):
        def arrow_to_s3(arrow_table):
            def give_name(_):
                return file_name

            s3 = s3fs.S3FileSystem()
            file_name = s3_key.split("/")[-1]
            s3_key_root = "s3://acoe-s3/" + s3_key[: s3_key.index(file_name) - 1]  # -1 removes the last slash
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

        if self.backend == "s3":
            arrow_to_s3(arrow_table)
        else:
            raise NotImplementedError

    def generate_workflow(
        self, refresh_partitions_list=True, if_exists="append", download_if_older_than=0, cache_distinct_values=True
    ):
        client = self.client or Client(self.scheduler_address)

        if refresh_partitions_list:
            all_partitions = self.get_distinct_values()
        else:
            all_partitions = self.get_cached_distinct_values(file_name="all_partitions.json")

        # by default, always cache the list of distinct values in backend for future use
        if cache_distinct_values:
            cache_distinct_values_in_backend = self.to_backend(all_partitions, file_name="all_partitions.json")

        if if_exists == "replace":
            partitions_to_download = all_partitions
        else:
            existing_partitions = self.get_existing_partitions()
            partitions_to_download = self.get_partitions_to_download(
                all_partitions, existing_partitions
            )  # should return json obj

        # compute partitions on the cluster
        partitions = client.compute(partitions_to_download).result()

        if isinstance(self.partition_cols, list):
            self.partition_cols = "CONCAT(" + ", ".join(self.partition_cols) + ")"

        # create the workflow
        uploads = []
        for partition in partitions:
            s3_key = self.s3_key + f"{partition}.parquet"
            where = f"{self.partition_cols}='{partition}'"
            processed_tool = self.query_tool(query=where)
            arrow_table = self.to_arrow(processed_tool)
            push_to_backend = self.arrow_to_backend(arrow_table, s3_key=s3_key)
            uploads.append(push_to_backend)
        wf = Workflow(name=self.name, tasks=uploads)
        return wf


# testing
# from grizly import QFrame
# import logging
# from distributed import Client
# logger = logging.getLogger("distributed.worker").getChild("extract_test")


# def load_qf(dsn):
#     grizly_wf_dir = os.getenv("GRIZLY_WORKFLOWS_HOME") or "/home/acoe_workflows/workflows"
#     json_path = os.path.join(grizly_wf_dir, "workflows", "historical_backlog_parquet", "historical_backlog_is.json")
#     qf = QFrame(dsn=dsn, logger=logger).from_json(json_path, subquery="historical_backlog_is")
#     return qf


# def query_qf(qf, query):
#     qf_copy = qf.copy()
#     return qf_copy.query(query)


# __file__ = "historical_backlog_is_parquet.py"
# #  AND business_unit_group = 'MEDG'
# where = """
#         segment in ( 101, 105 )
#         AND snapshot_weekly_indicator = 1
#         AND current_near_real_time_data_indicator = '0'
#         AND business_unit_group IN ('AERG', 'ENGG', 'ICTG', 'INDG', 'MEDG')
#         """
# qf_raw = load_qf(dsn="DenodoPROD")
# qf = query_qf(qf_raw, query=where)
# wf = Extract(tool=qf, backend="s3").generate_workflow(refresh_partitions_list=True)
# local_client = Client("grizly_scheduler:8786")
# wf.submit(client=local_client)
