from typing import List

from ...utils.type_mappers import mysql_to_postgresql, mysql_to_pyarrow, mysql_to_python
from .base import RDBMSWriteBase


class MariaDB(RDBMSWriteBase):
    """
    Class that represents MariaDB database.

    https://mariadb.org/

    Examples
    --------
    >>> from grizly import Source
    >>> sql_source = Source(dsn="retool_dev_db")
    """

    _quote = "`"
    dialect = "mysql"

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
