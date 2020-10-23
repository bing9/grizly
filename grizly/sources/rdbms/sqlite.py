from typing import List

from ...utils.type_mappers import mysql_to_postgresql, mysql_to_python
from .base import RDBMSBase


class SQLite(RDBMSBase):
    dialect = "mysql"

    @property
    def con(self):
        import sqlite3

        con = sqlite3.connect(database=self.dsn)
        return con

    def get_columns(
        self, table: str, schema: str = None, column_types: bool = False, columns: list = None
    ):
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

    @staticmethod
    def map_types(dtypes: List[str], to: str = None):
        if to == "postgresql":
            return [mysql_to_postgresql(dtype) for dtype in dtypes]
        if to == "python":
            return [mysql_to_python(dtype) for dtype in dtypes]
        else:
            raise NotImplementedError
