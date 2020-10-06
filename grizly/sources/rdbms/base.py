from ..base import BaseSource
from abc import ABC, abstractmethod
from pandas import read_sql_query
import sqlparse
import logging
from logging import Logger

from functools import partial
import deprecation

import os

deprecation.deprecated = partial(deprecation.deprecated, deprecated_in="0.3", removed_in="0.4")


class BaseTable(ABC):
    def __init__(self, name, source, schema=None):
        self.name = name
        self.source = source
        self.schema = schema
        self.fully_qualified_name = name if not schema else f"{schema}.{name}"

    def __repr__(self):
        return f"{self.__class__.__name__}(\"{self.name}\")"

    def info(self):
        print(f"""
        Table: {self.fully_qualified_name}
        Fields: {self.ncols}
        Rows: {self.nrows}
        """)

    @property
    @abstractmethod
    def fields(self):
        pass

    @property
    def columns(self):
        """Alias for fields"""
        return self.fields

    @property
    @abstractmethod
    def types(self):
        pass

    @property
    def dtypes(self):
        """Alias for types"""
        return self.types

    @property
    @abstractmethod
    def nrows(self):
        pass

    @property
    @abstractmethod
    def ncols(self):
        pass

    def __len__(self):
        return self.nrows


class RDBMSBase(BaseSource):
    last_commit = ""

    def __init__(
        self, dsn: str, **kwargs,
    ):
        self.dsn = dsn

    def __repr__(self):
        return f"{self.__class__.__name__}(dsn='{self.dsn}')"

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.dsn == other.dsn

    def get_connection(self, autocommit=False):
        # TODO: DEPRECATE
        return self.con

    @property
    def con(self, autocommit=False):
        """Returns sqlalchemy connection.

        Examples
        --------
        >>> sqldb = SQLDB(dsn="redshift_acoe")
        >>> con = sqldb.get_connection()
        >>> con.execute("SELECT * FROM grizly.table_tutorial ORDER BY 1").fetchall()
        [('item1', 1.3, None, 3.5), ('item2', 0.0, None, None)]
        >>> con.close()
        """
        import pyodbc

        try:
            con = pyodbc.connect(DSN=self.dsn, autocommit=autocommit)
        except pyodbc.InterfaceError:
            e = f"Data source name '{self.dsn}' not found"
            self.logger.exception(e)
            raise
        return con

    def _check_if_exists(self, exists_query, supported_dbs):
        if self.db not in supported_dbs:
            raise NotImplementedError(f"Unsupported database. Supported database: {supported_dbs}.")

        con = self.get_connection()
        exists = not read_sql_query(sql=exists_query, con=con).empty
        con.close()

        return exists

    def check_if_exists(self, table, schema=None, column=None, external=False):
        """Checks if a table exists.

        Examples
        --------
        >>> sqldb = SQLDB(dsn="redshift_acoe")
        >>> sqldb.check_if_exists(table="table_tutorial", schema="grizly")
        True
        """
        if external:
            supported_dbs = "redshift"
            sql_exists = f"SELECT * FROM SVV_EXTERNAL_TABLES WHERE tablename='{table}'"
            if schema:
                sql_exists += f" AND schemaname='{schema}'"
        else:
            supported_dbs = ("redshift", "aurora")
            sql_exists = f"SELECT * FROM information_schema.columns WHERE table_name='{table}'"
            if schema:
                sql_exists += f" AND table_schema='{schema}'"
            if column:
                sql_exists += f" AND column_name='{column}'"

        full_table_name = schema + "." + table if schema else table
        self.logger.info(f"Checking if table {full_table_name} exists...")

        exists = self._check_if_exists(exists_query=sql_exists, supported_dbs=supported_dbs)

        if exists:
            self.logger.info(f"Table {full_table_name} exists")
        else:
            self.logger.info(f"Table {full_table_name} does not exist")

        return exists

    def copy_table(self, in_table, out_table, in_schema=None, out_schema=None, if_exists="fail"):
        """Copies records from one table to another.

        Paramaters
        ----------
        if_exists : str, optional
            How to behave if the output table already exists.

            * fail: Raise a ValueError
            * drop: Drop table

        Examples
        --------
        >>> sqldb = SQLDB(dsn="redshift_acoe")
        >>> sqldb = sqldb.copy_table(
        ...    in_table="table_tutorial",
        ...    in_schema="grizly",
        ...    out_table="test_k",
        ...    out_schema="sandbox",
        ...    if_exists="drop",
        ... )
        >>> con = sqldb.get_connection()
        >>> con.execute("SELECT * FROM sandbox.test_k ORDER BY 1").fetchall()
        [('item1', 1.3, None, 3.5), ('item2', 0.0, None, None)]
        >>> con.close()
        """
        # if_exists to be removed
        # valid_if_exists = ("fail", "drop")
        # if if_exists not in valid_if_exists:
        #     raise ValueError(f"'{if_exists}' is not valid for if_exists. Valid values: {valid_if_exists}")

        supported_dbs = ("redshift", "aurora")
        if self.db not in supported_dbs:
            raise NotImplementedError(
                f"Unsupported database. Supported databases: {supported_dbs}."
            )

        in_table_full_name = f"{in_schema}.{in_table}" if in_schema else in_table
        out_table_full_name = f"{out_schema}.{out_table}" if out_schema else out_table
        sql = f"""
                DROP TABLE IF EXISTS {out_table_full_name};
                CREATE TABLE {out_table_full_name} AS
                SELECT * FROM {in_table_full_name}; commit;
                """
        self._run_query(sql)

        return self

    def _create_base_table(
        self, table, columns, types, schema=None, char_size=500, if_exists: str = "skip"
    ):
        """Creates a table."""
        valid_if_exists = ("fail", "skip", "drop")
        if if_exists not in valid_if_exists:
            raise ValueError(
                f"'{if_exists}' is not valid for if_exists. Valid values: {valid_if_exists}"
            )

        supported_dbs = ("redshift", "aurora")
        if self.db not in supported_dbs:
            raise NotImplementedError(f"Unsupported database. Supported database: {supported_dbs}.")

        full_table_name = f"{schema}.{table}" if schema else table

        if self.check_if_exists(table=table, schema=schema):
            if if_exists == "fail":
                raise ValueError(
                    f"Table {full_table_name} already exists and if_exists is set to 'fail'."
                )
            elif if_exists == "skip":
                self.logger.info(
                    f"Table {full_table_name} already exists and if_exists is set to 'skip'."
                )
                return self
            elif if_exists == "drop":
                self.drop_table(table=table, schema=schema)

        col_tuples = []

        for item in range(len(columns)):
            if types[item] == "VARCHAR(500)":
                column = columns[item] + " " + "VARCHAR({})".format(char_size)
            else:
                column = columns[item] + " " + types[item]
            col_tuples.append(column)

        columns_str = ", ".join(col_tuples)
        sql = "CREATE TABLE {} ({}); commit;".format(full_table_name, columns_str)
        self._run_query(sql)
        self.logger.info(f"Table {full_table_name} has been successfully created.")

        return self

    def _create_external_table(
        self, table, columns, types, bucket, s3_key, schema=None, if_exists: str = "skip"
    ):
        """Creates an external table"""
        valid_if_exists = ("fail", "skip", "drop")
        if if_exists not in valid_if_exists:
            raise ValueError(
                f"'{if_exists}' is not valid for if_exists. Valid values: {valid_if_exists}"
            )

        supported_dbs = "redshift"

        if self.db in supported_dbs:
            full_table_name = schema + "." + table if schema else table

            if self.check_if_exists(table=table, schema=schema, external=True):
                if if_exists == "fail":
                    raise ValueError(
                        f"Table {full_table_name} already exists and if_exists is set to 'fail'."
                    )
                elif if_exists == "skip":
                    self.logger.info(
                        f"Table {full_table_name} already exists and if_exists is set to 'skip'."
                    )
                    return self
                elif if_exists == "drop":
                    self.drop_table(table=table, schema=schema)

            columns_and_dtypes = ", \n".join(
                [col + " " + dtype for col, dtype in zip(columns, types)]
            )
            sql = f"""
            CREATE EXTERNAL TABLE {full_table_name} (
            {columns_and_dtypes}
            )
            ROW FORMAT SERDE
            'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
            STORED AS INPUTFORMAT
            'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
            OUTPUTFORMAT
            'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
            location 's3://{bucket}/{s3_key}';
            """
            self._run_query(sql, autocommit=True)
            self.logger.info(f"Table {full_table_name} has been successfully created.")
        else:
            raise NotImplementedError(f"Unsupported database. Supported database: {supported_dbs}.")

        return self

    def create_table(
        self,
        table,
        columns,
        types,
        schema=None,
        if_exists: str = "skip",
        type="base_table",
        **kwargs,
    ):
        """Creates a new table.

        Parameters
        ----------
        table : str
            Table name
        columns : list
            Column names
        types : list
            Column types
        schema : str, optional
            Schema name
        if_exists : {'fail', 'skip', 'drop'}, optional
            How to behave if the table already exists, by default 'skip'

            * fail: Raise a ValueError
            * skip: Abort without throwing an error
            * drop: Drop table before creating new one
        type : {'base_table', 'view', 'external_table'}, optional
            Type of a table

        Examples
        --------
        >>> sqldb = SQLDB(dsn="redshift_acoe")
        >>> sqldb = sqldb.create_table(
        ...     table="test_k",
        ...     columns=["col1", "col2"],
        ...     types=["varchar(100)", "int"],
        ...     schema="sandbox",
        ... )
        >>> sqldb.check_if_exists(table="test_k", schema="sandbox")
        True
        """

        if type == "base_table":
            self._create_base_table(
                table=table, columns=columns, types=types, schema=schema, if_exists=if_exists
            )
        elif type == "external_table":
            if not (("bucket" in kwargs) and ("s3_key" in kwargs)):
                msg = (
                    "'bucket' and 's3_key' parameters are required when creating an external table"
                )
                raise ValueError(msg)
            bucket = kwargs.get("bucket")
            s3_key = kwargs.get("s3_key")
            self._create_external_table(
                table=table,
                columns=columns,
                types=types,
                schema=schema,
                if_exists=if_exists,
                bucket=bucket,
                s3_key=s3_key,
            )
        elif type == "view":
            raise NotImplementedError()
            # self._create_view()
        else:
            raise ValueError("Type must be one of: ('base_table', 'external_table', 'view')")

        return self

    def insert_into(self, table, sql, columns=None, schema=None):
        """Inserts records into redshift table.

        Examples
        --------
        >>> sqldb = SQLDB(dsn="redshift_acoe")
        >>> sqldb = sqldb.create_table(table="test_k", columns=["col1", "col2"], types=["varchar", "int"], schema="sandbox")
        >>> sqldb = sqldb.insert_into(table="test_k", columns=["col1"], sql="SELECT col1 from grizly.table_tutorial", schema="sandbox")
        >>> con = sqldb.get_connection()
        >>> con.execute("SELECT * FROM sandbox.test_k ORDER BY 1").fetchall()
        [('item1', None), ('item2', None)]
        >>> con.close()

        """
        supported_dbs = ("redshift", "aurora")
        if self.db not in supported_dbs:
            raise NotImplementedError(f"Unsupported database. Supported database: {supported_dbs}.")

        full_table_name = f"{schema}.{table}" if schema else table
        if columns:
            columns = ", ".join(columns)
            sql = f"INSERT INTO {full_table_name} ({columns}) {sql}; commit;"
        else:
            sql = f"INSERT INTO {full_table_name} ({sql}); commit;"
        self.logger.info(f"Inserting records into table {full_table_name}...")
        self._run_query(sql)

        return self

    def delete_from(self, table, schema=None, where=None):
        """Removes records from Redshift table which satisfy where.

        Examples
        --------
        >>> sqldb = SQLDB(dsn="redshift_acoe")
        >>> sqldb = sqldb.delete_from(table="test_k", schema="sandbox", where="col2 is NULL")
        """
        supported_dbs = ("redshift", "aurora")

        if self.db not in supported_dbs:
            raise NotImplementedError(f"Unsupported database. Supported database: {supported_dbs}.")

        full_table_name = f"{schema}.{table}" if schema else table
        sql = f"DELETE FROM {full_table_name}"
        if where is not None:
            sql += f" WHERE {where} "
        sql += "; commit;"
        self.logger.info(f"Deleting records from table {full_table_name}...")
        self._run_query(sql)

        return self

    def drop_table(self, table, schema=None):
        """Drops Redshift table

        Examples
        --------
        >>> sqldb = SQLDB(dsn="redshift_acoe")
        >>> sqldb = sqldb.drop_table(table="test_k", schema="sandbox")
        >>> sqldb.check_if_exists(table="test_k", schema="sandbox")
        False
        """
        supported_dbs = ("redshift", "aurora")
        if self.db not in supported_dbs:
            raise NotImplementedError(f"Unsupported database. Supported database: {supported_dbs}.")

        full_table_name = f"{schema}.{table}" if schema else table
        sql = f"DROP TABLE IF EXISTS {full_table_name};"
        self.logger.info(f"Dropping table {full_table_name}...")
        self._run_query(sql, autocommit=True)

        return self

    def write_to(self, table, columns, sql, schema=None, if_exists="fail"):
        """Performs DELETE FROM (if table exists) and INSERT INTO queries in Redshift directly.

        Parameters
        ----------
        if_exists : {'fail', 'replace', 'append'}, optional
            How to behave if the table already exists, by default 'fail'

            * fail: Raise a ValueError
            * replace: Clean table before inserting new values
            * append: Insert new values to the existing table

        Examples
        --------
        >>> sqldb = SQLDB(dsn="redshift_acoe")
        >>> sqldb = sqldb.write_to(table="test_k", columns=["col1"], sql="SELECT col1 from grizly.table_tutorial", schema="sandbox", if_exists="replace")
        >>> con = sqldb.get_connection()
        >>> con.execute("SELECT * FROM sandbox.test_k ORDER BY 1").fetchall()
        [('item1', None), ('item2', None)]
        >>> con.close()
        """
        supported_dbs = ("redshift", "aurora")
        if self.db not in supported_dbs:
            raise NotImplementedError(f"Unsupported database. Supported database: {supported_dbs}.")

        full_table_name = schema + "." + table if schema else table
        if if_exists == "replace":
            self.delete_from(table=table, schema=schema)
            self.insert_into(table=table, columns=columns, sql=sql, schema=schema)
            self.logger.info(f"Data has been successfully inserted into {full_table_name}")
        elif if_exists == "fail":
            raise ValueError(
                f"Table {full_table_name} already exists and if_exists is set to 'fail'"
            )
        elif if_exists == "append":
            self.insert_into(table=table, columns=columns, sql=sql, schema=schema)
            self.logger.info(f"Data has been appended to {full_table_name}")

        return self

    def get_tables(self, schema=None, base_table=True, view=True, external_table=True):
        """Retrieves list of (schema, table) tuples

        Parameters
        ----------
        schema: str
            Name of schema.

        Examples
        --------
        >>> sqldb = SQLDB(dsn="redshift_acoe")
        >>> sqldb.get_tables(schema="grizly")
        [('grizly', 'track'), ('grizly', 'table_tutorial')]
        """
        output = []
        if base_table:
            if self.db in ("redshift", "mariadb", "aurora"):
                output += self._get_tables_general(schema=schema)

        if view:
            if self.db == "denodo":
                output += self._get_views_denodo(schema=schema)
            elif self.db in ("redshift", "mariadb", "aurora"):
                output += self._get_views_general(schema=schema)

        if external_table:
            if self.db == "redshift":
                output += self._get_tables_external(schema=schema)

        return output

    def _get_views_denodo(self, schema=None):
        where = f"\nWHERE database_name = '{schema}'\n" if schema else ""

        sql = f"""
            SELECT database_name, name
            FROM get_view_columns(){where}
            GROUP BY 1, 2
            """

        con = self.get_connection()
        output = con.execute(sql).fetchall()
        output = [tuple(i) for i in output]
        con.close()

        return output

    def _get_views_general(self, schema=None):
        where = f" AND table_schema = '{schema}'\n" if schema else ""

        sql = f"""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type='VIEW'{where}
            GROUP BY 1, 2
            """
        con = self.get_connection()
        output = con.execute(sql).fetchall()
        output = [tuple(i) for i in output]
        con.close()

        return output

    def _get_tables_general(self, schema=None):
        where = f" AND table_schema = '{schema}'\n" if schema else ""

        sql = f"""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type='BASE TABLE'{where}
            GROUP BY 1, 2
            """
        con = self.get_connection()
        output = con.execute(sql).fetchall()
        output = [tuple(i) for i in output]
        con.close()

        return output

    def _get_tables_external(self, schema=None):
        where = f"\nWHERE schemaname = '{schema}'\n" if schema else ""

        sql = f"""
            SELECT schemaname, tablename
            FROM svv_external_tables{where}
            GROUP BY 1, 2
            """
        con = self.get_connection()
        output = con.execute(sql).fetchall()
        output = [tuple(i) for i in output]
        con.close()

        return output

    def get_columns(self, table, schema=None, column_types=False, date_format="DATE", columns=None):
        """Retrieves column names and optionally other table metadata

        Parameters
        ----------
        table: str
            Name of table.
        schema: str
            Name of schema.
        column_types: bool
            True means user wants to get also data types.
        columns: list
            List of column names to retrive.
        date_format: str
            Denodo date format differs from those from other databases. User can choose which format is desired.

        Examples
        --------
        >>> sqldb = SQLDB(dsn="redshift_acoe")
        >>> sqldb.get_columns(table="table_tutorial", schema="grizly", column_types=True)
        (['col1', 'col2', 'col3', 'col4'], ['character varying(500)', 'double precision', 'character varying(500)', 'double precision'])
        """
        if self.db == "denodo":
            return self._get_columns_denodo(
                schema=schema,
                table=table,
                column_types=column_types,
                date_format=date_format,
                columns=columns,
            )
        elif self.db in ("redshift", "mariadb", "aurora"):
            if (schema, table) in self.get_tables(
                schema=schema, base_table=False, view=False, external_table=True
            ):
                return self._get_columns_external(
                    schema=schema, table=table, column_types=column_types, columns=columns
                )
            else:
                return self._get_columns_general(
                    schema=schema, table=table, column_types=column_types, columns=columns
                )
        elif self.db == "sqlite":
            return self._get_columns_sqlite(schema=schema, table=table, column_types=column_types)
        else:
            raise NotImplementedError("Unsupported database.")

    def _get_columns_denodo(
        self,
        table,
        schema: str = None,
        column_types: bool = False,
        columns: list = None,
        date_format: str = "DATE",
    ):
        """Get column names (and optionally types) from Denodo view.

        Parameters
        ----------
        date_format: str
            Denodo date format differs from those from other databases. User can choose which format is desired.
        """
        where = (
            f"view_name = '{table}' AND database_name = '{schema}' "
            if schema
            else f"view_name = '{table}' "
        )
        if not column_types:
            sql = f"""
                SELECT column_name
                FROM get_view_columns()
                WHERE {where}
                """
        else:
            sql = f"""
                SELECT distinct column_name,  column_sql_type, column_size
                FROM get_view_columns()
                WHERE {where}
        """
        con = self.get_connection()
        cursor = con.cursor()
        SQLDB.last_commit = sqlparse.format(sql, reindent=True, keyword_case="upper")
        cursor.execute(sql)
        col_names = []

        if not column_types:
            while True:
                column = cursor.fetchone()
                if not column:
                    break
                col_names.append(column[0])
            cursor.close()
            con.close()
            return col_names
        else:
            col_types = []
            while True:
                column = cursor.fetchone()
                if not column:
                    break
                col_names.append(column[0])
                if column[1] in ("VARCHAR", "NVARCHAR"):
                    col_types.append(column[1] + "(" + str(min(column[2], 1000)) + ")")
                elif column[1] == "DATE":
                    col_types.append(date_format)
                else:
                    col_types.append(column[1])
            cursor.close()
            con.close()
            if columns:
                col_names_and_types = {
                    col_name: col_type
                    for col_name, col_type in zip(col_names, col_types)
                    if col_name in columns
                }
                col_names = [col for col in columns if col in col_names_and_types]
                col_types = [col_names_and_types[col_name] for col_name in col_names]
            return col_names, col_types

    @staticmethod
    def _parametrize_dtype(raw_dtype, varchar_len, precision, scale):
        if varchar_len is not None:
            dtype = f"{raw_dtype}({varchar_len})"
        elif raw_dtype.upper() in ["DECIMAL", "NUMERIC"]:
            dtype = f"{raw_dtype}({precision}, {scale})"
        else:
            dtype = raw_dtype
        return dtype

    def _get_columns_general(
        self, table, schema: str = None, column_types: bool = False, columns: list = None
    ):
        """Get column names (and optionally types) from a Redshift, MariaDB or Aurora table."""
        where = (
            f"table_name = '{table}' AND table_schema = '{schema}' "
            if schema
            else f"table_name = '{table}' "
        )
        sql = f"""
            SELECT ordinal_position,
                   column_name,
                   data_type,
                   character_maximum_length,
                   numeric_precision,
                   numeric_scale
            FROM information_schema.columns
            WHERE {where}
            ORDER BY 1;
            """
        records = self._fetch_records(sql)

        cols_and_dtypes = {}
        for _, colname, raw_dtype, varchar_len, precision, scale in records:
            dtype = self._parametrize_dtype(raw_dtype, varchar_len, precision, scale)
            cols_and_dtypes[colname] = dtype

        if columns:
            # filter and sort columns and dtypes in the order provided by user
            colnames, dtypes = [], []
            for col in columns:
                if col in cols_and_dtypes:
                    colnames.append(col)
                    dtypes.append(cols_and_dtypes[col])
                else:
                    table_full_name = f"{schema}.{table}" if schema else table
                    self.logger.warning(f"Column {col} not found in {table_full_name}")
        else:
            colnames = list(cols_and_dtypes.keys())
            dtypes = list(cols_and_dtypes.values())

        if column_types:
            to_return = (colnames, dtypes)
        else:
            to_return = colnames

        return to_return

    def _get_columns_sqlite(self, table, schema: str = None, column_types: bool = False):
        """Get column names (and optionally types) from a SQLite table."""
        con = self.get_connection()
        cursor = con.cursor()
        table_name = table if schema is None or schema == "" else f"{schema}.{table}"

        sql = f"PRAGMA table_info({table_name})"
        cursor.execute(sql)

        col_names = []
        col_types = []

        while True:
            column = cursor.fetchone()
            if not column:
                break
            col_names.append(column[1])
            col_types.append(column[2])

        cursor.close()
        con.close()

        if column_types:
            return col_names, col_types
        else:
            return col_names

    def _get_columns_external(
        self, table, schema: str = None, column_types: bool = False, columns: list = None
    ):
        where = f" AND schemaname = '{schema}'\n" if schema else ""

        sql = f"""
            SELECT columnnum,
                columnname,
                external_type
            FROM SVV_EXTERNAL_COLUMNS
            WHERE tablename = '{table}'{where}
            ORDER BY 1
            """
        con = self.get_connection()
        records = con.execute(sql).fetchall()
        records = [tuple(i) for i in records]
        con.close()

        if columns is not None:
            col_names = [col for _, col, _ in records if col in columns]
            col_types = [typ for _, col, typ in records if col in columns]
        else:
            col_names = [col for _, col, _ in records]
            col_types = [typ for _, _, typ in records]

        return (col_names, col_types) if column_types else col_names

    def _run_query(self, sql: str, autocommit: bool = False):
        con = self.get_connection(autocommit=autocommit)
        try:
            SQLDB.last_commit = sql
            con.execute(sql)
            self.logger.debug(f"Successfully ran query\n {sql}")
        except:
            self.logger.exception(f"Error occured during running query\n {sql}")
        finally:
            con.close()
            self.logger.debug("Connection closed")

    def _fetch_records(self, sql):
        con = self.get_connection()
        SQLDB.last_commit = sqlparse.format(sql, reindent=True, keyword_case="upper")
        records = []
        try:
            records = con.execute(sql).fetchall()
            self.logger.debug(f"Successfully ran query\n {sql}")
        except:
            self.logger.exception(f"Error occured during running query\n {sql}")
        finally:
            con.close()
            self.logger.debug("Connection closed")
        records_tuples = [tuple(i) for i in records]  # cast from pyodbc records to python tuples
        return records_tuples
