from .base import RDBMSBase


class MariaDB(RDBMSBase):
    _quote = "`"
