from typing import List

from ...utils.type_mappers import postgresql_to_pyarrow, postgresql_to_python
from .base import RDBMSWriteBase


class Aurora(RDBMSWriteBase):
    pass


class AuroraPostgreSQL(Aurora):
    """
    Class that represents Aurora PostgreSQL database.

    https://aws.amazon.com/rds/aurora/postgresql-features/

    Examples
    --------
    >>> from grizly import Source
    >>> sql_source = Source(dsn="aurora_db")
    """

    @staticmethod
    def map_types(dtypes: List[str], to: str = None):
        if to == "python":
            return [postgresql_to_python(dtype) for dtype in dtypes]
        elif to == "pyarrow":
            return [postgresql_to_pyarrow(dtype) for dtype in dtypes]
        else:
            raise NotImplementedError
