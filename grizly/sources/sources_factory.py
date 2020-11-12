from .rdbms.rdbms_factory import RDBMS


def Source(dsn: str, *args, **kwargs):
    return RDBMS(dsn=dsn, *args, **kwargs)
