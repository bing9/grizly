from typing import List

from ...utils.type_mappers import postgresql_to_pyarrow, postgresql_to_python
from .base import RDBMSBase


class AuroraPostgreSQL(RDBMSBase):
    @staticmethod
    def map_types(dtypes: List[str], to: str = None):
        if to == "python":
            return [postgresql_to_python(dtype) for dtype in dtypes]
        elif to == "pyarrow":
            return [postgresql_to_pyarrow(dtype) for dtype in dtypes]
        else:
            raise NotImplementedError
