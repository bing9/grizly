from .base import RDBMSBase
from ...utils.type_mappers import mysql_to_postgres
from typing import List


class MariaDB(RDBMSBase):
    _quote = "`"
    dialect = "mysql"

    def map_types(self, dtypes: List[str], to_dialect: str = None):
        if to_dialect == "redshift":
            return [mysql_to_postgres(dtype) for dtype in dtypes]
        else:
            raise NotImplementedError
