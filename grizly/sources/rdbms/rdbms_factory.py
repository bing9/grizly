from ...config import config
from .aurora import Aurora
from .redshift import Redshift
from .denodo import Denodo

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
        return AuroraPostgreSQL(*args, **kwargs)
    elif db == "redshift":
        return Redshift(*args, **kwargs)
    elif db == "denodo":
        return Denodo(*args, **kwargs)
