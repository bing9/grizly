from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from pandas import read_sql_query
import os
import sqlparse
import logging
from logging import Logger
import re

from ..config import Config
from ..utils import get_sfdc_columns

from functools import partial
import deprecation

deprecation.deprecated = partial(deprecation.deprecated, deprecated_in="0.3", removed_in="0.4")


class SQLDB:
    last_commit = ""

    def __init__(
        self,
        db: str,
        engine_str: str = None,
        dsn: str = None,
        interface: str = "sqlalchemy",
        config_key: str = None,
        logger: Logger = None,
    ):
        if config_key:
            config = Config().get_service(config_key=config_key, service="sqldb")
        else:
            config = {
                "redshift": "mssql+pyodbc://redshift_acoe",
                "denodo": "mssql+pyodbc://DenodoPROD",
                "aurora": "mssql+pyodbc://aurora_db",
            }
        supported_dbs = ("redshift", "denodo", "sqlite", "mariadb", "aurora")
        if db not in supported_dbs:
            raise NotImplementedError(f"DB {db} not supported yet. Supported DB's: {supported_dbs}")
        self.db = db
        self.engine_str = engine_str or config.get(db)
        if interface not in ("sqlalchemy", "turbodbc", "pyodbc"):
            raise ValueError(
                f"Interface {interface} is not supported. Choose one of: 'sqlalchemy', 'turbodbc', 'pyodbc'"
            )
        self.interface = interface
        self.dsn = dsn or self.engine_str.split("/")[-1]
        self.logger = logger or logging.getLogger(__name__)

    def get_connection(self):
        """Returns sqlalchemy connection.

        Examples
        --------
        >>> sqldb = SQLDB(db="redshift")
        >>> con = sqldb.get_connection()
        >>> con.execute("SELECT * FROM administration.table_tutorial").fetchall()
        [('item1', 1.3, None, 3.5), ('item2', 0.0, None, None)]
        >>> con.close()
        """
        engine = create_engine(self.engine_str, encoding="utf8", poolclass=NullPool)
        if self.interface == "sqlalchemy":
            try:
                con = engine.raw_connection()
            except:
                self.logger.exception(f"Error connectig to {self.engine_str}. Retrying...")
                con = engine.raw_connection()
        elif self.interface == "turbodbc":
            import turbodbc

            try:
                con = turbodbc.connect(dsn=self.dsn)
            except turbodbc.exceptions.Error as error_msg:
                self.logger.exception(error_msg)
                raise
        elif self.interface == "pyodbc":
            import pyodbc

            try:
                con = pyodbc.connect(DSN=self.dsn)
            except pyodbc.InterfaceError:
                e = f"Data source name '{self.dsn}' not found"
                self.logger.exception(e)
                raise
        else:
            raise ValueError("Interface not specified.")
        return con

    def check_if_exists(self, table, schema=None, column=None):
        """Checks if a table exists in Redshift.

        Examples
        --------
        >>> sqldb = SQLDB(db="redshift")
        >>> sqldb.check_if_exists(table="table_tutorial", schema="administration")
        True
        """
        supported_dbs = ("redshift", "aurora")

        if self.db in supported_dbs:
            con = self.get_connection()
            sql_exists = f"select * from information_schema.columns where table_name='{table}'"
            if schema:
                sql_exists += f" and table_schema='{schema}'"
            if column:
                sql_exists += f" and column_name='{column}'"
            exists = not read_sql_query(sql=sql_exists, con=con).empty
            con.close()
        else:
            raise NotImplementedError(f"Unsupported database. Supported database: {supported_dbs}.")

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
        >>> sqldb = SQLDB(db="redshift")
        >>> sqldb = sqldb.copy_table(
        ...    in_table="table_tutorial",
        ...    in_schema="administration",
        ...    out_table="test_k",
        ...    out_schema="sandbox",
        ...    if_exists="drop",
        ... )
        >>> con = sqldb.get_connection()
        >>> con.execute("SELECT * FROM sandbox.test_k ORDER BY 1").fetchall()
        [('item1', 1.3, None, 3.5), ('item2', 0.0, None, None)]
        >>> con.close()
        >>> sqldb = sqldb.drop_table(table="test_k", schema="sandbox")
        """
        supported_dbs = ("redshift", "aurora")

        if self.db in supported_dbs:
            in_table_name = f"{in_schema}.{in_table}" if in_schema else in_table

            if not self.check_if_exists(table=in_table, schema=in_schema):
                self.logger.exception(f"Table {in_table_name} doesn't exist.")
            else:
                con = self.get_connection()
                out_table_name = f"{out_schema}.{out_table}" if out_schema else out_table
                if self.check_if_exists(table=out_table, schema=out_schema) and if_exists == "fail":
                    con.close()
                    raise ValueError(f"Table {in_table_name} already exists")
                sql = f"""
                        DROP TABLE IF EXISTS {out_table_name};
                        CREATE TABLE {out_table_name} AS
                        SELECT * FROM {in_table_name}
                        """
                SQLDB.last_commit = sqlparse.format(sql, reindent=True, keyword_case="upper")
                con.execute(sql).commit()
                con.close()
        else:
            raise NotImplementedError(f"Unsupported database. Supported database: {supported_dbs}.")

        return self

    def create_table(self, table, columns, types, schema=None, char_size=500):
        """Creates a new table in Redshift if the table doesn't exist.

        Parameters
        ----------
        columns : list
            Column names
        types : list
            Column types
        char_size : int,
            Size of the VARCHAR field in the database column

        Examples
        --------
        >>> sqldb = SQLDB(db="redshift")
        >>> sqldb = sqldb.create_table(table="test_k", columns=["col1", "col2"], types=["varchar", "int"], schema="sandbox")
        >>> sqldb.check_if_exists(table="test_k", schema="sandbox")
        True
        """
        supported_dbs = ("redshift", "aurora")

        if self.db in supported_dbs:

            if self.check_if_exists(table=table, schema=schema):
                return self
            else:
                table_name = f"{schema}.{table}" if schema else table
                col_tuples = []

                for item in range(len(columns)):
                    if types[item] == "VARCHAR(500)":
                        column = columns[item] + " " + "VARCHAR({})".format(char_size)
                    else:
                        column = columns[item] + " " + types[item]
                    col_tuples.append(column)

                columns_str = ", ".join(col_tuples)
                sql = "CREATE TABLE {} ({})".format(table_name, columns_str)
                SQLDB.last_commit = sql
                con = self.get_connection()
                con.execute(sql).commit()
                con.close()

                self.logger.info(f"Table {sql} has been created successfully.")
        else:
            raise NotImplementedError(f"Unsupported database. Supported database: {supported_dbs}.")

        return self

    def insert_into(self, table, columns, sql, schema=None):
        """Inserts records into redshift table.

        Examples
        --------
        >>> sqldb = SQLDB(db="redshift")
        >>> sqldb = sqldb.create_table(table="test_k", columns=["col1", "col2"], types=["varchar", "int"], schema="sandbox")
        >>> sqldb = sqldb.insert_into(table="test_k", columns=["col1"], sql="SELECT col1 from administration.table_tutorial", schema="sandbox")
        >>> con = sqldb.get_connection()
        >>> con.execute("SELECT * FROM sandbox.test_k ORDER BY 1").fetchall()
        [('item1', None), ('item2', None)]
        >>> con.close()

        """
        supported_dbs = ("redshift", "aurora")

        if self.db in supported_dbs:
            con = self.get_connection()
            table_name = f"{schema}.{table}" if schema else table

            if self.check_if_exists(table=table, schema=schema):
                columns = ", ".join(columns)
                sql = f"INSERT INTO {table_name} ({columns}) {sql}"
                SQLDB.last_commit = sqlparse.format(sql, reindent=True, keyword_case="upper")
                con.execute(sql).commit()
            else:
                self.logger.info(f"Table {table_name} doesn't exist.")
            con.close()
        else:
            raise NotImplementedError(f"Unsupported database. Supported database: {supported_dbs}.")

        return self

    def delete_from(self, table, schema=None, where=None):
        """Removes records from Redshift table which satisfy where.

        Examples
        --------
        >>> sqldb = SQLDB(db="redshift")
        >>> sqldb = sqldb.delete_from(table="test_k", schema="sandbox", where="col2 is NULL")
        >>> con = sqldb.get_connection()
        >>> con.execute("SELECT * FROM sandbox.test_k ORDER BY 1").fetchall()
        []
        >>> con.close()
        """
        supported_dbs = ("redshift", "aurora")

        if self.db in supported_dbs:
            con = self.get_connection()
            table_name = f"{schema}.{table}" if schema else table

            if self.check_if_exists(table=table, schema=schema):
                sql = f"DELETE FROM {table_name}"
                if where is None:
                    SQLDB.last_commit = sqlparse.format(sql, reindent=True, keyword_case="upper")
                    con.execute(sql).commit()
                    self.logger.info(f"Records from table {table_name} has been removed successfully.")
                else:
                    sql += f" WHERE {where} "
                    SQLDB.last_commit = sqlparse.format(sql, reindent=True, keyword_case="upper")
                    con.execute(sql).commit()
                    self.logger.info(f"Records from table {table_name} where {where} has been removed successfully.")
            else:
                self.logger.info(f"Table {table_name} doesn't exist.")
            con.close()
        else:
            raise NotImplementedError(f"Unsupported database. Supported database: {supported_dbs}.")

        return self

    def drop_table(self, table, schema=None):
        """Drops Redshift table

        Examples
        --------
        >>> sqldb = SQLDB(db="redshift")
        >>> sqldb = sqldb.drop_table(table="test_k", schema="sandbox")
        >>> sqldb.check_if_exists(table="test_k", schema="sandbox")
        False
        """
        supported_dbs = ("redshift", "aurora")

        if self.db in supported_dbs:
            con = self.get_connection()
            table_name = f"{schema}.{table}" if schema else table

            if self.check_if_exists(table=table, schema=schema):
                sql = f"DROP TABLE {table_name}"
                SQLDB.last_commit = sqlparse.format(sql, reindent=True, keyword_case="upper")
                con.execute(sql).commit()
                self.logger.info(f"Table {table_name} has been dropped successfully.")
            else:
                self.logger.info(f"Table {table_name} doesn't exist.")
            con.close()
        else:
            raise NotImplementedError(f"Unsupported database. Supported database: {supported_dbs}.")

        return self

    def write_to(self, table, columns, sql, schema=None, if_exists="fail"):
        """Performs DELETE FROM (if table exists) and INSERT INTO queries in Redshift directly.

        Parameters
        ----------
        if_exists : {'fail', 'replace', 'append'}, optional
            How to behave if the table already exists, by default 'fail'

            * fail: Raise a ValueError
            * replace: Clean table before inserting new values.
            * append: Insert new values to the existing table

        Examples
        --------
        >>> sqldb = SQLDB(db="redshift")
        >>> sqldb = sqldb.write_to(table="test_k", columns=["col1"], sql="SELECT col1 from administration.table_tutorial", schema="sandbox", if_exists="replace")
        >>> con = sqldb.get_connection()
        >>> con.execute("SELECT * FROM sandbox.test_k ORDER BY 1").fetchall()
        [('item1', None), ('item2', None)]
        >>> con.close()
        >>> sqldb = sqldb.drop_table(table="test_k", schema="sandbox")
        """
        supported_dbs = ("redshift", "aurora")

        if self.db in supported_dbs:

            if self.check_if_exists(table=table, schema=schema):

                if if_exists == "replace":
                    self.delete_from(table=table, schema=schema)
                    self.insert_into(table=table, columns=columns, sql=sql, schema=schema)
                    self.logger.info(f"Data has been owerwritten into {schema}.{table}")
                elif if_exists == "fail":
                    raise ValueError("Table already exists")
                elif if_exists == "append":
                    self.insert_into(table=table, columns=columns, sql=sql, schema=schema)
                    self.logger.info(f"Data has been appended to {schema}.{table}")
            else:
                self.logger.exception("Table doesn't exist. Use create_table first")
        else:
            raise NotImplementedError(f"Unsupported database. Supported database: {supported_dbs}.")

        return self

    def get_columns(
        self, table, schema=None, column_types=False, date_format="DATE", columns=None,
    ):
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
        >>> sqldb = SQLDB(db="redshift")
        >>> sqldb.get_columns(table="table_tutorial", schema="administration", column_types=True)
        (['col1', 'col2', 'col3', 'col4'], ['character varying(500)', 'double precision', 'character varying(500)', 'double precision'])
        """
        if self.db == "denodo":
            return self._get_columns_1(
                schema=schema, table=table, column_types=column_types, date_format=date_format, columns=columns,
            )
        elif self.db in ("redshift", "mariadb", "aurora"):
            return self._get_columns_2(schema=schema, table=table, column_types=column_types, columns=columns)
        elif self.db == "sqlite":
            return self._get_columns_3(schema=schema, table=table, column_types=column_types)

    def _get_columns_1(
        self, table, schema: str = None, column_types: bool = False, columns: list = None, date_format: str = "DATE",
    ):
        """Get column names (and optionally types) from Denodo view.

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
        """
        where = f"view_name = '{table}' AND database_name = '{schema}' " if schema else f"view_name = '{table}' "
        if column_types == False:
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

        if column_types == False:
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
                    col_name: col_type for col_name, col_type in zip(col_names, col_types) if col_name in columns
                }
                col_names = [col for col in col_names_and_types]
                col_types = [type for type in col_names_and_types.values()]
            return col_names, col_types

    def _get_columns_2(
        self, table, schema: str = None, column_types: bool = False, columns: list = None,
    ):
        """Get column names (and optionally types) from a Redshift, MariaDB or Aurora table.

        Parameters
        ----------
        table: str
            Name of table.
        schema: str
            Name of schema.
        column_types: bool
            Whether to retrieve field types.
        columns: list
            List of column names to retrive.
        """
        con = self.get_connection()
        cursor = con.cursor()
        where = f"table_name = '{table}' AND table_schema = '{schema}' " if schema else f"table_name = '{table}' "
        sql = f"""
            SELECT ordinal_position,
                   column_name,
                   data_type,
                   character_maximum_length,
                   numeric_precision,
                   numeric_scale
            FROM information_schema.columns
            WHERE {where}
            ORDER BY ordinal_position;
            """
        SQLDB.last_commit = sqlparse.format(sql, reindent=True, keyword_case="upper")
        cursor.execute(sql)

        col_names = []

        if column_types:
            col_types = []
            while True:
                column = cursor.fetchone()
                if not column:
                    break
                col_name = column[1]
                col_type = column[2]
                if column[3] is not None:
                    col_type = f"{col_type}({column[3]})"
                elif col_type.upper() in ["DECIMAL", "NUMERIC"]:
                    col_type = f"{col_type}({column[4]}, {column[5]})"
                col_names.append(col_name)
                col_types.append(col_type)
            # leave only the cols provided in the columns argument
            if columns:
                col_names_and_types = {
                    col_name: col_type for col_name, col_type in zip(col_names, col_types) if col_name in columns
                }
                col_names = [col for col in col_names_and_types]
                col_types = [type for type in col_names_and_types.values()]
            to_return = (col_names, col_types)
        else:
            while True:
                column = cursor.fetchone()
                if not column:
                    break
                col_name = column[1]
                col_names.append(col_name)
            # leave only the cols provided in the columns argument
            if columns:
                col_names = [col for col in col_names if col in columns]
            to_return = col_names

        cursor.close()
        con.close()

        return to_return

    def _get_columns_3(self, table, schema: str = None, column_types: bool = False):
        """Get column names (and optionally types) from a SQLite table.

        Parameters
        ----------
        table: str
            Name of table.
        schema: str
            Name of schema.
        column_types: bool
            Whether to retrieve field types.
        """
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


@deprecation.deprecated(details="Use SQLDB.check_if_exists function instead",)
def check_if_exists(table, schema=""):
    sqldb = SQLDB(db="redshift", engine_str="mssql+pyodbc://redshift_acoe")
    return sqldb.check_if_exists(table=table, schema=schema)


@deprecation.deprecated(details="Use SQLDB.create_table function instead",)
def create_table(table, columns, types, schema="", engine_str=None, char_size=500):
    sqldb = SQLDB(db="redshift", engine_str=engine_str)
    sqldb.create_table(table=table, columns=columns, types=types, schema=schema, char_size=char_size)


@deprecation.deprecated(details="Use SQLDB.write_to function instead",)
def write_to(table, columns, sql, schema="", engine_str=None, if_exists="fail"):
    sqldb = SQLDB(db="redshift", engine_str=engine_str)
    sqldb.write_to(table=table, columns=columns, sql=sql, schema=schema, if_exists=if_exists)


@deprecation.deprecated(details="Use SQLDB.get_columns function instead",)
def get_columns(
    table, schema=None, column_types=False, date_format="DATE", db="denodo", columns=None, engine_str: str = None,
):
    db = db.lower()
    if db == "denodo" or db == "redshift":
        sqldb = SQLDB(db=db, engine_str=engine_str)
        return sqldb.get_columns(
            table=table, schema=schema, column_types=column_types, date_format=date_format, columns=columns,
        )
    elif db == "sfdc":
        return get_sfdc_columns(table=table, column_types=column_types, columns=columns)
    else:
        raise NotImplementedError("This db is not yet supported")


@deprecation.deprecated(details="Use SQLDB.delete_where function instead",)
def delete_where(table, schema="", redshift_str=None, *argv):
    sqldb = SQLDB(db="redshift", engine_str=redshift_str)
    if argv is not None:
        for arg in argv:
            sqldb.delete_from(table=table, schema=schema, where=arg)
    else:
        sqldb.delete_from(table=table, schema=schema)


@deprecation.deprecated(details="Use SQLDB.copy_table function instead",)
def copy_table(schema, copy_from, to, redshift_str=None):
    sqldb = SQLDB(db="redshift", engine_str=redshift_str)
    sqldb.copy_table(in_table=copy_from, out_table=to, in_schema=schema, out_schema=schema)
    return "Success"
