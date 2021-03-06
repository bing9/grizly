from .base import RDBMSReadBase
from ...utils.type_mappers import denodo_to_python, denodo_to_pyarrow
from typing import List, Any


class Denodo(RDBMSReadBase):
    """
    Class that represents Denodo database (ready only).

    https://www.denodo.com/en

    Examples
    --------
    >>> from grizly import Source
    >>> sql_source = Source(dsn="DenodoPROD")
    """

    _context = (
        " CONTEXT('swap' = 'ON', 'swapsize' = '500', 'i18n' = 'us_est', "
        "'queryTimeout' = '9000000000', 'simplify' = 'on')"
    )

    def _get_base_tables(self, schema: str = None):
        return []

    def _get_views(self, schema: str = None) -> List[Any]:
        where = f"\nWHERE database_name = '{schema}'\n" if schema else ""

        sql = f"""
            SELECT database_name, name
            FROM get_view_columns(){where}
            GROUP BY 1, 2
            """

        records = self._fetch_records(sql)

        return records

    def get_columns(
        self,
        table,
        schema: str = None,
        column_types: bool = False,
        columns: list = None,
        date_format: str = "DATE",
    ):
        """Get columns names (and optionally types) from Denodo view.

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

    @classmethod
    def map_types(cls, dtypes: List[str], to: str = None):
        if to == cls.dialect:
            return dtypes
        elif to == "postgresql":
            # TODO: implement denodo_to_postgresql mapper
            return dtypes
        elif to == "python":
            return [denodo_to_python(dtype) for dtype in dtypes]
        elif to == "pyarrow":
            return [denodo_to_pyarrow(dtype) for dtype in dtypes]
        else:
            raise NotImplementedError(f"Mapping from {cls.dialect} to {to} is not yet implemented")
