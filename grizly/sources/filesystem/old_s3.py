import csv
from datetime import datetime, timezone
from functools import partial, wraps
from io import BytesIO, StringIO
from itertools import count
import json
import logging
import os
from typing import List, Union, Literal, Optional, Tuple

from boto3 import resource
from botocore.exceptions import ClientError
import deprecation
from pandas import DataFrame, read_csv, read_excel, read_parquet
import pyarrow.parquet as pq

from ..utils import clean, clean_colnames, file_extension, get_path
from .dialects import pyarrow_to_rds_type
from .sqldb import SQLDB

deprecation.deprecated = partial(deprecation.deprecated, deprecated_in="0.3", removed_in="0.4")

logger = logging.getLogger(__name__)
# TODO: change to a single logger accross the who grizly library, so that it
# can be set in one place


def _check_if_s3_exists(f):
    @wraps(f)
    def wrapped(self, *args, **kwargs):
        try:
            resource("s3").Object(self.bucket, self.full_s3_key).load()
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                raise FileNotFoundError(f"{self.full_s3_key} file not found")

        return f(self, *args, **kwargs)

    return wrapped


class S3:

    """Class that represents a file in S3.

        Parameters
        ----------
        file_name : str
            Name of the file
        s3_key : str
            Name of s3 key
        bucket : str, optional
            Bucket name, if None then 'acoe-s3'
        file_dir : str, optional
            Path to local folder to store the file, if None then '%UserProfile%/s3_loads'
        """

    _ids = count(0)

    def __init__(
        self,
        file_name: Optional[str] = None,
        s3_key: Optional[str] = None,
        bucket: Optional[str] = None,
        url: Optional[str] = None,
        file_dir: Optional[str] = None,
        min_time_window: int = 0,
        logger=None,
        **kwargs,
    ):
        self.id = next(self._ids)

        if url:
            self.url = url
            _, _, self.bucket, *s3_key_list = url.split("/")[:-1]
            self.s3_key = "/".join(s3_key_list) + "/"
            self.file_name = url.split("/")[-1]
        else:
            self.file_name = file_name or f"s3_tmp_{self.id}.csv"
            self.s3_key = s3_key or ""
            if not self.s3_key.endswith("/") and self.s3_key != "":
                self.s3_key += "/"
            self.bucket = bucket or "acoe-s3"
            self.url = f"s3://{self.bucket}/{self.s3_key}{self.file_name}"

        self.file_ext = file_extension(self.file_name)
        self.full_s3_key = self.s3_key + self.file_name
        self.file_dir = file_dir or get_path("s3_loads")
        os.makedirs(self.file_dir, exist_ok=True)
        self.file_path = os.path.join(self.file_dir, self.file_name)

        self.min_time_window = min_time_window
        self.logger = logger or logging.getLogger(__name__)
        self.status = "initiated"

        self.__set_proxy()

        if kwargs.get("redshift_str") is not None:
            self.logger.warning(
                "Parameter redshift_str will be ignored. If you are going to use "
                "S3.to_rds method, please specify dsn parameter there.",
            )

    def __repr__(self):
        return f"{self.__class__.__name__}(url='{self.url}')"

    @property
    def aws_credentials(self):
        from configparser import ConfigParser

        config = ConfigParser()
        config.read(get_path(".aws", "credentials"))

        return config["default"] or dict()

    @property
    def aws_config(self):
        from configparser import ConfigParser

        config = ConfigParser()
        config.read(get_path(".aws", "config"))

        return config["default"] or dict()

    def info(self):
        """Print a concise summary of a S3

        Examples
        --------
        >>> S3('test_grizly.csv', 'test/').info()
        file_name: test_grizly.csv
        s3_key: test/
        bucket: acoe-s3
        """
        s = f"file_name: {self.file_name}\n" f"s3_key: {self.s3_key}\n" f"bucket: {self.bucket}"
        print(s)

    def list(self) -> List[str]:
        """Returns the list of files that are in S3.s3_key"""
        files = []
        key_list = self.s3_key.split("/")[:-1]
        data = resource("s3").meta.client.list_objects(Bucket=self.bucket, Prefix=self.s3_key)
        if "Contents" in data:
            for file in data["Contents"]:
                file_list = file["Key"].split("/")
                for _ in key_list:
                    file_list.pop(0)
                if len(file_list) == 1:
                    files.append(file_list[0])
        else:
            files = []
        return files

    def exists(self):
        """Check if the S3 file exists"""
        return self.file_name in self.list()

    @_check_if_s3_exists
    def delete(self):
        """Remove S3 file"""
        resource("s3").Object(self.bucket, self.full_s3_key).delete()
        self.logger.info(f"'{self.full_s3_key}' has been successfully removed")

    @_check_if_s3_exists
    def copy_to(
        self,
        file_name: Optional[str] = None,
        s3_key: Optional[str] = None,
        bucket: Optional[str] = None,
        keep_file: bool = True,
        if_exists: Literal["fail", "skip", "replace", "archive"] = "replace",
        versioning: bool = True,
    ):
        """Copies S3 file to another S3 file.

        Parameters
        ----------
        file_name : str, optional
            New file name, if None then the same as in class
        s3_key : str, optional
            New S3 key, if None then the same as in class
        bucket : str, optional
            New bucket, if None then the same as in class
        keep_file : bool, optional
            Whether to keep the original S3 file after copying it to another
            S3 file, by default True
        if_exists : str, optional
            How to behave if the output S3 already exists.

            * fail: Raise ValueError
            * skip: Abort without throwing an error
            * replace: Overwrite existing file
            * archive: Move old S3 to archive/s3_key/file_name(version)

        Examples
        --------
        >>> s3 = S3('test_grizly.csv', 'test/test/', file_dir=r'/home/analyst/')
        >>> s3 = s3.copy_to('test_old.csv', s3_key='test/')

        Returns
        -------
        S3
            S3 class with new parameters
        """
        if if_exists not in ("fail", "skip", "replace", "archive"):
            raise ValueError(f"'{if_exists}' is not valid for if_exists")
        file_name = file_name or self.file_name
        s3_key = s3_key or self.s3_key
        bucket = bucket or self.bucket

        out_s3 = S3(file_name=file_name, s3_key=s3_key, bucket=bucket, file_dir=self.file_dir,)
        exists = self.exists()

        if exists:
            if if_exists == "fail":
                raise ValueError(f"{s3_key + file_name} already exists")
            elif if_exists == "skip":
                return self
            elif if_exists == "archive":
                out_s3.archive(versioning=versioning)

        s3_file = resource("s3").Object(bucket, s3_key + file_name)

        source_s3_key = self.full_s3_key
        copy_source = {"Key": source_s3_key, "Bucket": self.bucket}

        s3_file.copy(copy_source)
        self.logger.info(
            f"'{source_s3_key}' copied from '{self.bucket}' to '{bucket}' bucket "
            f"as '{s3_key + file_name}'"
        )

        if not keep_file:
            self.delete()

        return out_s3

    def from_df(
        self,
        df: DataFrame,
        sep: str = "\t",
        clean_df: bool = True,
        if_exists: Literal["fail", "skip", "replace", "archive"] = "replace",
        to_file: bool = False,
        index: bool = False,
        **kwargs,
    ):
        r"""Saves DataFrame in S3.

        Examples
        --------
        >>> from pandas import DataFrame
        >>> df = DataFrame({'col1': [1, 2], 'col2': [3, 4]})
        >>> s3 = S3('test_grizly.csv', 'test/').from_df(df)

        Parameters
        ----------
        df : DataFrame
            DataFrame object
        sep : str, optional
            Separator, by default '\t'
        clean_df : bool, optional
            Whether to clean DataFrame before loading to s3, by default True
        if_exists : str, optional
            How to behave if the S3 already exists.

            * fail: Raise ValueError
            * skip: Abort without throwing an error
            * replace: Overwrite existing file
            * archive: Move old S3 to archive/s3_key/file_name(version)
        """
        if not isinstance(df, DataFrame):
            raise ValueError("'df' must be DataFrame object")

        ext = ["csv", "parquet", "xlsx"]
        if self.file_ext not in ext:
            raise NotImplementedError(
                f"Unsupported file format. Please use files with extensions {ext}."
            )

        if clean_df:
            df = clean(df)
            df = clean_colnames(df)

        output = os.path.join(self.file_dir, self.file_name) if to_file else StringIO()
        if self.file_ext == "csv":
            df.to_csv(output, index=index, sep=sep, **kwargs)
        elif self.file_ext == "xlsx":
            df.to_excel(output, index=index, **kwargs)
        else:
            df.to_parquet(output, index=index, **kwargs)
        if to_file:
            return self.from_file(keep_file=kwargs.get("keep_file", False), if_exists=if_exists)
        else:
            return self._from_buffer(output)

    def from_file(
        self,
        keep_file: bool = True,
        if_exists: Literal["fail", "skip", "replace", "archive"] = "replace",
        versioning: bool = True,
    ):
        """Writes local file to S3.

        Parameters
        ----------
        keep_file:
            Whether to keep the local file copy after uploading it to Amazon S3, by default True
        if_exists : str, optional
            How to behave if the S3 already exists.

            * fail: Raise ValueError
            * skip: Abort without throwing an error
            * replace: Overwrite existing file
            * archive: Move old S3 to archive/s3_key/file_name(version)

        Examples
        --------
        >>> with open("test_grizly.csv", mode="w") as csvfile:
        ...     writer = csv.writer(csvfile)
        ...     writer.writerows([("col1", "col2"), (1, 2)])
        >>> s3 = S3('test_grizly.csv', s3_key='test/test/', file_dir=os.getcwd())
        >>> s3 = s3.from_file(keep_file=False)
        """
        if if_exists not in ("fail", "skip", "replace", "archive"):
            raise ValueError(f"'{if_exists}' is not valid for if_exists")

        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"File '{self.file_path}' does not exist.")

        if self.exists():
            if if_exists == "fail":
                raise ValueError(f"{self.full_s3_key} already exists")
            elif if_exists == "skip":
                return self
            elif if_exists == "archive":
                self.archive(versioning=versioning)

        s3_file = resource("s3").Object(self.bucket, self.full_s3_key)

        if not self._can_upload():
            msg = (
                f"File {self.file_name} was not uploaded to {self.s3_key} "
                "because a recent version exists.\nSet S3.min_time_window to 0 "
                f"to force the upload (currently set to: {self.min_time_window})."
            )
            self.logger.warning(msg)
            self.status = "skipped"
            return self

        s3_file.upload_file(self.file_path)
        self.status = "uploaded"

        self.logger.info(f"Successfully uploaded '{self.file_name}' to S3")
        self.logger.debug(f"{self.file_name}'s S3 location: 's3://{self.bucket}/{self.s3_key}'")

        if not keep_file:
            os.remove(self.file_path)
            self.logger.debug(f"Successfully removed '{self.file_path}'")

        return self

    def from_serializable(self, serializable) -> None:
        """Writes a JSON-serializable object to S3

        Parameters
        ----------
        serializable:
            A JSON-serializable object

        Examples
        --------
        >>> s3 = S3('test_from_serializable.json', s3_key='test/')
        >>> s3 = s3.from_serializable(["a", "b", 1])
        """
        s3_obj = resource("s3").Object(self.bucket, self.full_s3_key)
        s3_obj.put(Body=(bytes(json.dumps(serializable).encode("UTF-8"))))
        self.logger.info(
            f"Successfully uploaded '{self.file_name}' to 's3://{self.bucket}/{self.s3_key}'"
        )

    @_check_if_s3_exists
    def to_df(self, to_file=False, **kwargs) -> DataFrame:
        ext = ["csv", "parquet", "xlsx"]

        if self.file_ext not in ext:
            raise NotImplementedError(
                f"Unsupported file format. Please use files with extensions {ext}."
            )

        if to_file:
            filepath_or_buffer = os.path.join(self.file_dir, self.file_name)
            self.to_file(if_exists=kwargs.get("if_exists"))
        else:
            filepath_or_buffer = BytesIO(self._to_bytes())

        if self.file_ext == "csv":
            if kwargs.get("sep") is None:
                kwargs["sep"] = "\t"
            df = read_csv(filepath_or_buffer, **kwargs)
        elif self.file_ext == "xlsx":
            df = read_excel(filepath_or_buffer, **kwargs)
        else:
            df = read_parquet(filepath_or_buffer, **kwargs)

        return df

    @_check_if_s3_exists
    def to_file(self, if_exists: Literal["fail", "skip", "replace"] = "replace") -> None:
        r"""Writes S3 to local file.

        Parameters
        ----------
        if_exists : str, optional
            How to behave if the local file already exists.

            * fail: Raise ValueError
            * skip: Abort without throwing an error
            * replace: Overwrite existing file

        Examples
        --------
        >>> s3 = S3('test_grizly.csv', 'test/')
        >>> s3 = s3.to_file()
        """
        if if_exists not in ("fail", "skip", "replace"):
            raise ValueError(f"'{if_exists}' is not valid for if_exists")

        if os.path.exists(self.file_path):
            if if_exists == "fail":
                raise ValueError(f"File {self.file_path} already exists")
            elif if_exists == "skip":
                return None

        s3_file = resource("s3").Object(self.bucket, self.full_s3_key)
        s3_file.download_file(self.file_path)

        self.logger.info(f"'{self.full_s3_key}' was successfully downloaded to '{self.file_path}'")

    @_check_if_s3_exists
    def to_serializable(self):
        content_object = resource("s3").Object(self.bucket, self.full_s3_key)
        file_content = content_object.get()["Body"].read().decode("utf-8")
        serializable = json.loads(file_content)
        return serializable

    @_check_if_s3_exists
    def to_rds(
        self,
        table: str,
        schema: Optional[str] = None,
        dsn: Optional[str] = None,
        sqldb: Optional[SQLDB] = None,
        if_exists: Literal["fail", "skip", "replace", "drop"] = "fail",
        sep: str = "\t",
        types: Optional[dict] = None,
        column_order: Optional[List] = None,
        preserve_column_names: bool = False,
        remove_inside_quotes: bool = False,
        time_format: Optional[str] = None,
        execute_on_skip: bool = False,
        **kwargs,
    ):
        """Writes S3 to Redshift table.

        Parameters
        ----------
        table : str
            Table name
        schema : str, optional
            Schame name, by default None
        if_exists : {'fail', 'replace', 'drop', 'append'}, default 'fail'
            How to behave if the table already exists.

            * fail: Raise a ValueError.
            * replace: Clean table before inserting new values.
            * drop: Drop table and create new one.
            * append: Insert new values to the existing table.

        sep : str, optional
            Separator, by default '\t'
        types : dict, optional
            Data types to force, by default None
        column_order : list, optional
            List of column names in other order than default
        remove_inside_quotes : bool, optional
            Whether to add REMOVEQUOTES to copy statement, by default False
        """
        if if_exists not in ("fail", "replace", "append"):
            raise ValueError(f"'{if_exists}' is not valid for if_exists")

        if not execute_on_skip:
            if self.status == "skipped":
                return None

        self.status = "initiated"

        redshift_str = kwargs.get("redshift_str")
        if redshift_str is not None:
            dsn = redshift_str.split("://")[-1]
            self.logger.warning(
                f"Parameter redshift_str is deprecated as of 0.3 and will "
                f"be removed in 0.4. Please use dsn='{dsn}' instead.",
            )
        if dsn is None and sqldb is None:
            self.logger.warning(
                "Please specify dsn parameter. Since version 0.3.8 it will be obligatory."
            )

        sqldb = sqldb or SQLDB(dsn=(dsn or "redshift_acoe"), **kwargs)

        if sqldb.db.lower() != "redshift":
            raise ValueError(f"Specified datasource '{sqldb.dsn}' is not a Redshift Database.")

        self.__validate_output_table(
            table=table, schema=schema, sqldb=sqldb, if_exists=if_exists, sep=sep, types=types
        )

        columns_output_table = sqldb.get_columns(table=table, schema=schema)

        if column_order is None:
            if preserve_column_names:
                column_order = []
            else:
                column_order, is_null = self._load_column_names(sep=sep)
        else:
            not_found_columns = set(column_order) - set(columns_output_table)
            if not_found_columns != set():
                self.logger.exception(
                    f"Columns {not_found_columns} not found in output table."
                    f"Please change 'column_order' parameter or rename output table columns."
                )

        table_name = f"{schema}.{table}" if schema else table

        if self.file_ext == "csv":
            not_found_columns = set(column_order) - set(columns_output_table)
            if not_found_columns != set():
                self.logger.exception(
                    f"S3 file {self.full_s3_key} input columns {not_found_columns} "
                    f"not found in output table {table_name}."
                    f"Please rename columns in the file or in the table before the insert."
                )
            column_order_str = "(" + ", ".join(column_order) + ")" if column_order != [] else ""
            remove_inside_quotes_str = "REMOVEQUOTES" if remove_inside_quotes else ""

            if remove_inside_quotes:
                _format = ""
            else:
                _format = "FORMAT AS csv"
            sql = f"""
                COPY {table_name} {column_order_str}
                FROM 's3://{self.bucket}/{self.full_s3_key}'
                access_key_id '{self.aws_credentials["aws_access_key_id"]}'
                secret_access_key '{self.aws_credentials["aws_secret_access_key"]}'
                delimiter '{sep}'
                NULL ''
                IGNOREHEADER 1
                {remove_inside_quotes_str}
                {_format}
                ;commit;
                """
        else:
            if is_null:
                self.logger.warning(
                    "Your file contains null columns. You may consider filling "
                    "these columns with some values before the insert "
                    "as columns names validation cannot be performed."
                )
            elif column_order != columns_output_table:
                self.logger.warning(
                    f"S3 file {self.full_s3_key} input columns\n{column_order}\ndoesn't "
                    f"match table {table_name} output columns\n{columns_output_table}.\n"
                    "You may consider renaming columns in the file or in "
                    "the table before the insert."
                )

            _format = "FORMAT AS PARQUET"
            sql = f"""
                COPY {table_name}
                FROM 's3://{self.bucket}/{self.full_s3_key}'
                IAM_ROLE '{self.aws_credentials["iam_role"]}'
                {_format};
                ;commit;
                """
        if time_format:
            indent = 9
            last_line_pos = len(sql) - len(";commit;") - indent
            spaces = indent * " "  # print formatting
            time_format_argument = f"timeformat '{time_format}'"
            sql = (
                sql[:last_line_pos]
                + time_format_argument
                + "\n"
                + spaces[:-1]
                + sql[last_line_pos:]
            )

        con = sqldb.get_connection()
        self.logger.info(f"Inserting '{self.file_name}' into {table_name}...")
        try:
            con.execute(sql)
        except:
            self.logger.exception(
                f"Failed to insert '{self.file_name}' into Redshift [{table_name}]"
            )
            self.logger.exception(sql)
            self.status = "failed"
            raise
        finally:
            con.close()
        self.status = "success"
        self.logger.info(f"Successfully inserted '{self.file_name}' into Redshift")
        self.logger.debug(f"'{self.file_name}''s Redshift location: {table_name}")

    @_check_if_s3_exists
    def to_aurora(
        self,
        table: str,
        schema: Optional[str] = None,
        dsn: Optional[str] = None,
        sqldb: Optional[SQLDB] = None,
        if_exists: Literal["fail", "skip", "replace", "drop"] = "fail",
        sep: str = "\t",
        types: Optional[dict] = None,
        column_order: Optional[List] = None,
        execute_on_skip: bool = False,
        **kwargs,
    ):
        """Writes S3 to Aurora table.

        Parameters
        ----------
        table : str
            Table name
        schema : str, optional
            Schema name, by default None
        dsn : str, optional
            Data source name, if None then 'aurora_db'
        if_exists : {'fail', 'replace', 'drop', 'append'}, default 'fail'
            How to behave if the table already exists.

            * fail: Raise a ValueError.
            * replace: Clean table before inserting new values.
            * drop: Drop table and create new one.
            * append: Insert new values to the existing table.

        sep : str, optional
            Separator, by default '\t'
        types : dict, optional
            Data types to force, by default None
        column_order : list, optional
            List of column names in other order than default
        """
        if self.file_ext != "csv":
            raise NotImplementedError("Unsupported file format. Please use CSV files.")

        if if_exists not in ("fail", "replace", "drop", "append"):
            raise ValueError(f"'{if_exists}' is not valid for if_exists")

        if not execute_on_skip:
            if self.status == "skipped":
                return None

        self.status = "initiated"

        sqldb = sqldb or SQLDB(dsn=dsn, **kwargs)

        if sqldb.db.lower() != "aurora":
            raise ValueError(f"Specified datasource '{sqldb.dsn}' is not an Aurora Database.")

        self.__validate_output_table(
            table=table, schema=schema, sqldb=sqldb, if_exists=if_exists, sep=sep, types=types
        )

        con = sqldb.get_connection()
        con.execute("CREATE EXTENSION IF NOT EXISTS aws_s3 CASCADE; COMMIT;")

        columns_output_table = sqldb.get_columns(table=table, schema=schema)

        if column_order is None:
            column_order, _ = self._load_column_names(sep=sep)
        else:
            not_found_columns = set(column_order) - set(columns_output_table)
            if not_found_columns != set():
                self.logger.exception(
                    f"Columns {not_found_columns} not found in output table."
                    f"Please change 'column_order' parameter or rename output table columns."
                )

        table_name = f"{schema}.{table}" if schema else table
        columns_str = ",".join(column_order)
        sql = f"""SELECT aws_s3.table_import_from_s3 (
                    '{table_name}',
                    '{columns_str}',
                    '(DELIMITER E''{sep}'', FORMAT CSV, HEADER TRUE)',
                    '{self.bucket}',
                    '{self.s3_key}{self.file_name}',
                    '{self.aws_config["region"]}',
                    '{self.aws_credentials["aws_access_key_id"]}',
                    '{self.aws_credentials["aws_secret_access_key"]}',
                    ''
                    );
                    COMMIT;"""
        try:
            con.execute(sql)
        except:
            self.logger.exception(f"Failed to insert '{self.file_name}' into Aurora [{table_name}]")
            self.logger.exception(sql)
            self.status = "failed"
            raise
        finally:
            con.close()

        self.status = "success"
        self.logger.info(f"Successfully inserted {self.file_name} into Aurora")
        self.logger.debug(f"'{self.file_name}''s Aurora location: {table_name}")

    def archive(self, versioning=True):
        """Moves S3 to 'archive/' key. It adds also the versions of the file
        eg. file(0).csv, file(1).csv, ...

        Examples
        --------
        >>> s3 = S3('test_grizly.csv', 'test/')
        >>> s3_arch = s3.archive()
        """
        date_short = datetime.now().strftime("%Y-%m-%d")
        s3_archive = S3(
            file_name=self.file_name,
            s3_key=f"archive/{date_short}/" + self.s3_key,
            bucket=self.bucket,
        )

        version = 0
        while True:
            if versioning:
                file_name = (
                    s3_archive.file_name.split(".")[0]
                    + f"({version})."
                    + s3_archive.file_name.split(".")[1]
                )
            else:
                file_name = s3_archive.file_name

            if file_name not in s3_archive.list():
                return self.copy_to(
                    file_name=file_name,
                    s3_key=f"archive/{date_short}/" + self.s3_key,
                    keep_file=False,
                )
            else:
                if versioning:
                    version += 1

    def _can_upload(self):

        try:
            last_modified = resource("s3").Object(self.bucket, self.full_s3_key).last_modified
        except:
            last_modified = None

        if not last_modified or self.min_time_window == 0:
            return True

        now_utc = datetime.now(timezone.utc)
        diff = now_utc - last_modified
        diff_seconds = diff.seconds
        if diff_seconds < self.min_time_window:
            return False
        return True

    def _create_table_like_s3(self, table, schema, sqldb, types, sep):
        if self.file_ext == "csv":
            # TODO: this should write to df
            s3_client = resource("s3").meta.client

            obj_content = s3_client.select_object_content(
                Bucket=self.bucket,
                Key=self.full_s3_key,
                ExpressionType="SQL",
                Expression="SELECT * FROM s3object LIMIT 21",
                InputSerialization={
                    "CSV": {
                        "FileHeaderInfo": "None",
                        "AllowQuotedRecordDelimiter": True,
                        "FieldDelimiter": sep,
                    }
                },
                OutputSerialization={"CSV": {}},
            )

            records = []
            for event in obj_content["Payload"]:
                if "Records" in event:
                    records.append(event["Records"]["Payload"])

            file_str = "".join(r.decode("utf-8") for r in records)
            csv_reader = csv.reader(StringIO(file_str))

            def isfloat(s):
                try:
                    float(s)
                    return not s.isdigit()
                except ValueError:
                    return False

            ### this should be a function
            count = 0
            for row in csv_reader:
                if count == 0:
                    column_names = row
                    column_isfloat = [[] for _ in row]
                else:
                    i = 0
                    for item in row:
                        column_isfloat[i].append(isfloat(item))
                        i += 1
                count += 1
            ###

            col_names = []
            col_types = []
            count = 0
            for col in column_names:
                if types and col in types:
                    col_type = types[col].upper()
                    types.pop(col)
                else:
                    if True in set(column_isfloat[count]):
                        col_type = "FLOAT"
                    else:
                        col_type = "VARCHAR(500)"
                col_names.append(col)
                col_types.append(col_type)
                # columns.append(f"{col} {col_type}")
                count += 1
            if types:
                other_cols = list(types.keys())
                if other_cols:
                    self.logger.warning(f"Columns {other_cols} were not found.")

        elif self.file_ext == "parquet":
            # TODO: this should write to bytes
            self.to_file()
            pq_table = pq.read_table(os.path.join(self.file_dir, self.file_name))
            col_names = []
            col_types = []
            for col_name, dtype in zip(pq_table.schema.names, pq_table.schema.types):
                dtype = str(dtype)
                if types and col_name in types:
                    col_type = types[col_name]
                else:
                    col_type = pyarrow_to_rds_type(dtype)
                col_names.append(col_name)
                col_types.append(col_type)
        else:
            raise ValueError("Table cannot be created. File extension not supported.")

        sqldb.create_table(table=table, schema=schema, columns=col_names, types=col_types)

    def _load_column_names(self, sep) -> Tuple[List, Union[bool, None]]:
        if self.file_ext == "csv":
            file_ext = "CSV"
        elif self.file_ext == "parquet":
            file_ext = "Parquet"
        else:
            self.logger.warning("Column names cannot be imported. File extension not supported.")
            return [], None

        is_null = False
        s3_client = resource("s3").meta.client

        if file_ext == "CSV":
            InputSerialization = {
                "CSV": {"FieldDelimiter": sep, "AllowQuotedRecordDelimiter": True}
            }
        else:
            InputSerialization = {"Parquet": {}}

        obj_content = s3_client.select_object_content(
            Bucket=self.bucket,
            Key=self.full_s3_key,
            ExpressionType="SQL",
            Expression="SELECT s.* FROM s3object s LIMIT 1",
            InputSerialization=InputSerialization,
            OutputSerialization={"JSON": {}},
        )
        records = []
        for event in obj_content["Payload"]:
            if "Records" in event:
                records.append(event["Records"]["Payload"])
        records = records[0].splitlines()[0].decode("utf-8")

        if file_ext == "CSV":
            records = eval(records).values()
        elif file_ext == "Parquet":
            if ":null" in records:
                is_null = True
            records = eval(records.replace(":null", ":'null'")).keys()

        column_names = [r.rstrip() for r in records]

        return column_names, is_null

    @_check_if_s3_exists
    def _to_bytes(self) -> bytes:
        s3_obj = resource("s3").Object(self.bucket, self.full_s3_key)
        s3_obj_bytes = s3_obj.get()["Body"].read()
        return s3_obj_bytes

    def _from_buffer(self, buffer: Union[BytesIO, StringIO]):
        s3_obj = resource("s3").Object(self.bucket, self.full_s3_key)
        s3_obj.put(Body=buffer.getvalue())
        buffer.close()
        return self

    def __validate_output_table(self, table, schema, sqldb, if_exists, sep, types):
        if sqldb.check_if_exists(table, schema):
            if if_exists == "fail":
                raise ValueError(f"Table {table} already exists")
            elif if_exists == "replace":
                sqldb.delete_from(table=table, schema=schema)
            elif if_exists == "drop":
                sqldb.drop_table(table=table, schema=schema)
            else:
                pass
        else:
            self._create_table_like_s3(
                table=table, schema=schema, sep=sep, sqldb=sqldb, types=types
            )

    @staticmethod
    def __set_proxy():
        https_proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY")
        http_proxy = os.environ.get("HTTP_PROXY") or os.environ.get("HTTPS_PROXY")
        if https_proxy is not None and http_proxy is not None:
            os.environ["HTTPS_PROXY"] = https_proxy
            os.environ["HTTP_PROXY"] = http_proxy


@deprecation.deprecated(details="Use S3.to_file function instead",)
def s3_to_csv(csv_path, bucket: str = None):
    """
    Writes s3 to csv file.

    Parameters
    ----------
    csv_path : string
        Path to csv file.
    bucket : str, optional
        Bucket name, if None then 'teis-data'
    """
    s3 = S3(
        file_name=os.path.basename(csv_path), bucket=bucket, file_dir=os.path.dirname(csv_path),
    )
    s3.to_file()


@deprecation.deprecated(details="Use S3.from_file function instead",)
def csv_to_s3(csv_path, s3_key: str = None, keep_csv=True, bucket: str = None):
    """
    Writes csv file to s3.

    Parameters
    ----------
    csv_path : string
        Path to csv file.
    keep_csv : bool, optional
        Whether to keep the local csv copy after uploading it to Amazon S3, by default True
    bucket : str, optional
        Bucket name, if None then 'teis-data'
    """
    s3 = S3(
        file_name=os.path.basename(csv_path),
        s3_key=s3_key,
        bucket=bucket,
        file_dir=os.path.dirname(csv_path),
    )
    return s3.from_file(keep_file=keep_csv)


@deprecation.deprecated(
    details=(
        "Use S3.from_df and S3.to_rds functions instead. "
        "Parameters: schema, dtype, if_exists are ignored."
    ),
)
def df_to_s3(
    df,
    table_name,
    schema=None,
    dtype=None,
    sep="\t",
    clean_df=False,
    keep_csv=True,
    chunksize=10000,
    if_exists="fail",
    s3_key=None,
    bucket=None,
    **kwargs,
):
    s3 = S3(file_name=table_name + ".csv", s3_key=s3_key, bucket=bucket, file_dir=os.getcwd())

    return s3.from_df(df=df, sep=sep, clean_df=clean_df, keep_file=keep_csv, chunksize=chunksize)


@deprecation.deprecated(details="Use S3.to_rds function instead.",)
def s3_to_rds(
    file_name,
    table_name=None,
    schema="",
    time_format=None,
    if_exists="fail",
    sep="\t",
    redshift_str=None,
    bucket=None,
    s3_key=None,
    remove_inside_quotes=False,
):

    if not table_name:
        table = file_name.replace(".csv", "")
    else:
        table = table_name
    s3 = S3(
        file_name=file_name,
        s3_key=s3_key,
        bucket=bucket,
        file_dir=os.getcwd(),
        redshift_str=redshift_str,
    )
    s3.to_rds(
        table=table,
        schema=schema,
        if_exists=if_exists,
        sep=sep,
        remove_inside_quotes=remove_inside_quotes,
        time_format=time_format,
    )
