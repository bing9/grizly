from .base import RDBMSBase


class SQLite(RDBMSBase):
    @property
    def con(self):
        import sqlite3

        con = sqlite3.connect(database=self.dsn)
        return con
