from .base import RDBMSBase
from ...utils.type_mappers import mysql_to_postgresql
from typing import List


class MariaDB(RDBMSBase):
    _quote = "`"
    dialect = "mysql"

    @staticmethod
    def map_types(dtypes: List[str], to: str = None):
        if to == "postgresql":
            return [mysql_to_postgresql(dtype) for dtype in dtypes]
        else:
            raise NotImplementedError
