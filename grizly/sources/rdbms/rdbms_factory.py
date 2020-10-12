from functools import partial

import deprecation

from ...config import config
from .aurora import AuroraPostgreSQL
from .denodo import Denodo
from .redshift import Redshift
from .sqlite import SQLite


deprecation.deprecated = partial(deprecation.deprecated, deprecated_in="0.4", removed_in="0.5")

supported_dbs = ("redshift", "denodo", "sqlite", "mariadb", "aurora", "tableau")


def RDBMS(dsn: str, dialect: str = None, db: str = None, *args, **kwargs):
    config_data = config.get_service("sources")
    if None in [dialect, db] and config_data.get(dsn) is None:
        raise ValueError(
            f"DataSource '{dsn}' not found in the config. Please specify both db and dialect parameters."
        )
    db = db or config_data[dsn]["db"]
    if db not in supported_dbs:
        raise NotImplementedError(f"DB {db} not supported yet. Supported DB's: {supported_dbs}")

    dialect = dialect or config_data[dsn]["dialect"]
    if db == "aurora" and dialect == "postgresql":
        return AuroraPostgreSQL(dsn=dsn, *args, **kwargs)
    elif db == "redshift":
        return Redshift(dsn=dsn, *args, **kwargs)
    elif db == "denodo":
        return Denodo(dsn=dsn, *args, **kwargs)
    elif db == "sqlite":
        return SQLite(dsn=dsn, *args, **kwargs)


@deprecation.deprecated(details="Use RDBMS class instead",)
def SQLDB(*args, **kwargs):
    return RDBMS(*args, **kwargs)
