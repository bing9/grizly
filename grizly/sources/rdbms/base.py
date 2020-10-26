from abc import ABC, abstractmethod
from functools import partial
from typing import Literal, Union, List, Any

import deprecation

from ..base import BaseSource

deprecation.deprecated = partial(deprecation.deprecated, deprecated_in="0.4", removed_in="0.5")


class RDBMSBase(BaseSource):
    _context = ""
    _quote = '"'
    _use_ordinal_position_notation = True
    dialect = "postgresql"

    def __init__(
        self, dsn: str, **kwargs,
    ):
        super().__init__(**kwargs)
        self.dsn = dsn

    def __repr__(self):
        return f"{self.__class__.__name__}(dsn='{self.dsn}')"

    def __eq__(self, other):
        return self.dsn == other.dsn

    def copy_object(self, **kwargs):
        return self.copy_table(**kwargs)

    def delete_object(self, **kwargs):
        return self.drop_table(**kwargs)

    def create_object(self, **kwargs):
        return self.create_table(**kwargs)

    def object(self, name):
        pass

    @property
    def con(self):
        import pyodbc

        try:
            con = pyodbc.connect(DSN=self.dsn)
        except pyodbc.InterfaceError:
            e = f"Data source name '{self.dsn}' not found"
            self.logger.exception(e)
            raise
        return con

    def get_connection(self, autocommit=False):
        """Return pyodbc connection

        Examples
        --------
        >>> sqldb = SQLDB(dsn="redshift_acoe")
        >>> con = sqldb.get_connection()
        >>> con.execute("SELECT * FROM grizly.table_tutorial ORDER BY 1").fetchall()
        [('item1', 1.3, None, 3.5), ('item2', 0.0, None, None)]
        >>> con.close()
        """
        return self.con

    def check_if_exists(self, table: str, schema: str = None, column: str = None):
        """Check if a table exists

        Examples
        --------
        >>> sqldb = SQLDB(dsn="redshift_acoe")
        >>> sqldb.check_if_exists(table="table_tutorial", schema="grizly")
        True
        """
        columns = [column] if column else None
        exists = self.get_columns(schema=schema, table=table, columns=columns) != []

        return exists

    def copy_table(
        self,
        in_table: str,
        out_table: str,
        in_schema: str = None,
        out_schema: str = None,
        if_exists="fail",
    ):
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
        in_table_full_name = f"{in_schema}.{in_table}" if in_schema else in_table
        out_table_full_name = f"{out_schema}.{out_table}" if out_schema else out_table

        sql = ""
        if if_exists == "drop":
            sql += f"DROP TABLE IF EXISTS {out_table_full_name};"

        sql += f"""
                CREATE TABLE {out_table_full_name} AS
                SELECT * FROM {in_table_full_name}; commit;
                """
        self._run_query(sql)

        return self

    def create_table(
        self,
        table: str,
        columns: list,
        types: list,
        schema: str = None,
        if_exists: Literal["fail", "skip", "drop"] = "skip",
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
            * drop: Drop table before creating new one

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

        self._create_base_table(
            table=table, columns=columns, types=types, schema=schema, if_exists=if_exists
        )

        return self

    def _create_base_table(
        self, table: str, columns: list, types: list, schema: str = None, if_exists: str = "skip"
    ):
        """Create a base table"""
        full_table_name = f"{schema}.{table}" if schema else table
        sql = ""
        if if_exists == "drop":
            sql += f"DROP TABLE IF EXISTS {full_table_name};"

        sql += "CREATE TABLE"
        if if_exists == "skip":
            sql += " IF NOT EXISTS"

        col_tuples = []

        for col, _type in zip(columns, types):
            column = col + " " + _type
            col_tuples.append(column)

        columns_str = ", ".join(col_tuples)
        sql += f" {full_table_name} ({columns_str}); commit;"
        self._run_query(sql)
        self.logger.info(f"Table {full_table_name} has been successfully created.")

        return self

    def insert_into(self, table: str, sql: str, columns: list = None, schema: str = None):
        """Inserts records into table.

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
        full_table_name = f"{schema}.{table}" if schema else table
        if columns:
            columns = ", ".join(columns)
            sql = f"INSERT INTO {full_table_name} ({columns}) {sql}; commit;"
        else:
            sql = f"INSERT INTO {full_table_name} ({sql}); commit;"
        self.logger.info(f"Inserting records into table {full_table_name}...")
        self._run_query(sql)

        return self

    def delete_from(self, table: str, schema: str = None, where: str = None):
        """Removes records from Redshift table which satisfy where.

        Examples
        --------
        >>> sqldb = SQLDB(dsn="redshift_acoe")
        >>> sqldb = sqldb.delete_from(table="test_k", schema="sandbox", where="col2 is NULL")
        """

        full_table_name = f"{schema}.{table}" if schema else table
        sql = f"DELETE FROM {full_table_name}"
        if where is not None:
            sql += f" WHERE {where} "
        sql += "; commit;"
        self.logger.info(f"Deleting records from table {full_table_name} {where}...")
        self._run_query(sql)

        return self

    def drop_table(self, table: str, schema: str = None):
        """Drop table

        Examples
        --------
        >>> sqldb = SQLDB(dsn="redshift_acoe")
        >>> sqldb = sqldb.drop_table(table="test_k", schema="sandbox")
        >>> sqldb.check_if_exists(table="test_k", schema="sandbox")
        False
        """

        full_table_name = f"{schema}.{table}" if schema else table
        sql = f"DROP TABLE IF EXISTS {full_table_name};"
        self.logger.info(f"Dropping table {full_table_name}...")
        self._run_query(sql, autocommit=True)

        return self

    def write_to(self, table: str, columns: list, sql: str, schema: str = None, if_exists="fail"):
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

    def get_tables(self, schema=None, base_table=True, view=True):
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
            output += self._get_base_tables(schema=schema)

        if view:
            output += self._get_views(schema=schema)

        return output

    @property
    def objects(self):
        return self.get_tables()

    @property
    def tables(self):
        return self.objects

    def _get_views(self, schema: str = None):
        where = f" AND table_schema = '{schema}'\n" if schema else ""

        sql = f"""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type='VIEW'{where}
            GROUP BY 1, 2
            """
        records = self._fetch_records(sql)

        return records

    def _get_base_tables(self, schema: str = None):
        where = f" AND table_schema = '{schema}'\n" if schema else ""

        sql = f"""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type='BASE TABLE'{where}
            GROUP BY 1, 2
            """
        records = self._fetch_records(sql)

        return records

    def get_columns(
        self, table: str, schema: str = None, column_types: bool = False, columns: list = None
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

        Examples
        --------
        >>> sqldb = SQLDB(dsn="redshift_acoe")
        >>> sqldb.get_columns(table="table_tutorial", schema="grizly", column_types=True)
        (['col1', 'col2', 'col3', 'col4'], ['character varying(500)', 'double precision', 'character varying(500)', 'double precision'])
        """

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

    @staticmethod
    def _parametrize_dtype(raw_dtype, varchar_len, precision, scale):
        if varchar_len is not None:
            dtype = f"{raw_dtype}({varchar_len})"
        elif raw_dtype.upper() in ["DECIMAL", "NUMERIC"]:
            dtype = f"{raw_dtype}({precision}, {scale})"
        else:
            dtype = raw_dtype
        return dtype

    def _run_query(self, sql: str, autocommit: bool = False):
        sql = self._add_context(sql)
        con = self.con
        con.autocommit = autocommit
        try:
            con.execute(sql)
            self.logger.debug(f"Successfully ran query\n {sql}")
        except:
            self.logger.exception(f"Error occurred during running query\n {sql}")
            raise
        finally:
            con.close()

    def _fetch_records(self, sql: str):
        sql = self._add_context(sql)
        con = self.con
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

    def _add_context(self, sql: str):
        if self._context:
            return sql + self._context
        else:
            return sql

    @staticmethod
    def map_types(types: Union[str, List[Any]], to: str = None):
        raise NotImplementedError


class BaseTable(ABC):
    def __init__(self, name, source, schema=None):
        self.name = name
        self.source = source
        self.schema = schema
        self.fully_qualified_name = name if not schema else f"{schema}.{name}"

    def __repr__(self):
        return f'{self.__class__.__name__}("{self.name}")'

    def __len__(self):
        return self.nrows

    def info(self):
        print(
            f"""
        Table: {self.fully_qualified_name}
        Fields: {self.ncols}
        Rows: {self.nrows}
        """
        )

    @property
    @abstractmethod
    def fields(self):
        pass

    @property
    @abstractmethod
    def types(self):
        pass

    @property
    @abstractmethod
    def nrows(self):
        pass

    @property
    @abstractmethod
    def ncols(self):
        pass

    @property
    def columns(self):
        """Alias for fields"""
        return self.fields

    @property
    def dtypes(self):
        """Alias for types"""
        return self.types
