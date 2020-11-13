from typing import List

from ...utils.type_mappers import mysql_to_postgresql, mysql_to_pyarrow, mysql_to_python
from .base import RDBMSWriteBase


class SQLite(RDBMSWriteBase):
    """
    Class that represents SQLite database.

    https://www.sqlite.org/index.html

    Examples
    --------
    >>> from pathlib import Path
    >>> from grizly import Source
    >>> dsn = Path.cwd().parent.joinpath("tests", "Chinook.sqlite")
    >>> sql_source = Source(dsn=dsn, db="sqlite", dialect="mysql")
    """

    dialect = "mysql"

    @property
    def con(self):
        """Sqlite connection."""
        import sqlite3

        con = sqlite3.connect(database=self.dsn)
        return con

    def get_columns(
        self, table: str, schema: str = None, column_types: bool = False, columns: list = None
    ):
        """Get columns names (and optionally types) from a SQLite table."""
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

    @staticmethod
    def map_types(dtypes: List[str], to: str = None):
        if to == "postgresql":
            return [mysql_to_postgresql(dtype) for dtype in dtypes]
        elif to == "python":
            return [mysql_to_python(dtype) for dtype in dtypes]
        elif to == "pyarrow":
            return [mysql_to_pyarrow(dtype) for dtype in dtypes]
        else:
            raise NotImplementedError


    def _run_query(self, sql: str, autocommit: bool = False):
        """Sqlite doesn't allow to run many queries at the same time and autocommits"""
        sql = self._add_context(sql)
        queries = self.__split_query(query=sql)
        con = self.con
        cursor = con.cursor()
        for query in queries:
            cursor.execute(query)
        try:
            con.commit()
            self.logger.debug(f"Successfully ran query\n {sql}")
        except:
            self.logger.exception(f"Error occurred during running query\n {sql}")
            raise
        finally:
            con.close()
            self.logger.debug("Connection closed")

    @staticmethod
    def __split_query(query: str)-> List[str]:
        queries = query.split(";")

        # remove 'commit' and '' elements
        cleaned_queries = []
        for query in queries:
            cleaned_query = query.replace(" ", "").lower()
            if cleaned_query not in ("", "commit"):
                cleaned_queries.append(query)

        return cleaned_queries