from .base import RDBMSBase


class AuroraPostgresql(RDBMSBase):
    allowed_statements = ["select", "from", "where", "limit", "offset"]
    quotes = '"'
    pass
