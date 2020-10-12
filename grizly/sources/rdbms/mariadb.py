from .base import RDBMSBase
from ...utils.type_mappers import mysql_to_postgresql
from typing import List


class MariaDB(RDBMSBase):
    _quote = "`"
    dialect = "mysql"

    def map_types(self, dtypes: List[str], to_dialect: str = None):
        if to_dialect == "postgresql":
            return [mysql_to_postgresql(dtype) for dtype in dtypes]
        else:
            raise NotImplementedError
