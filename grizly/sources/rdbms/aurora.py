from .base import RDBMSBase


class AuroraPostgreSQL(RDBMSBase):
    allowed_statements = ["select", "from", "where", "limit", "offset"]
    quotes = '"'
    pass
