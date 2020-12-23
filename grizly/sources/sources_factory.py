from ..config import config
from .rdbms.aurora import AuroraPostgreSQL
from .rdbms.denodo import Denodo
from .rdbms.mariadb import MariaDB
from .rdbms.redshift import Redshift, RedshiftSpectrum
from .rdbms.sfdc import SFDB
from .rdbms.sqlite import SQLite
from .rdbms.tableau import Tableau

supported_dbs = ("redshift", "denodo", "sqlite", "mariadb", "aurora", "tableau", "sfdc")


def Source(dsn: str, source_name: str = None, dialect: str = None, **kwargs):
    source_name = source_name or kwargs.get("db")
    config_data = config.get_service("sources")
    if config_data.get(dsn) is None and None in [source_name, dialect]:
        raise ValueError(
            f"DataSource '{dsn}' not found in the config. Please specify both source_name and dialect parameters."
        )
    source_name = source_name or config_data[dsn].get("source_name") or config_data[dsn].get("db")
    dialect = dialect or config_data[dsn].get("dialect")

    if source_name == "aurora" and dialect == "postgresql":
        return AuroraPostgreSQL(dsn=dsn, **kwargs)
    elif source_name == "redshift" and dialect == "postgresql":
        return Redshift(dsn=dsn, **kwargs)
    elif source_name == "redshift" and dialect == "spectrum":
        return RedshiftSpectrum(dsn=dsn, **kwargs)
    elif source_name == "denodo":
        return Denodo(dsn=dsn, **kwargs)
    elif source_name == "sqlite":
        return SQLite(dsn=dsn, **kwargs)
    elif source_name == "mariadb":
        return MariaDB(dsn=dsn, **kwargs)
    elif source_name == "sfdc":
        return SFDB(dsn=dsn, **kwargs)
    elif source_name == "tableau":
        return Tableau(dsn=dsn, **kwargs)

    raise NotImplementedError(
        f"Source {source_name} not supported yet. Supported sources: {supported_dbs}"
    )


def SQLDB(dsn: str, **kwargs):
    return Source(dsn=dsn, **kwargs)
