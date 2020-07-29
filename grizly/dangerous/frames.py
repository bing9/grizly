from .drivers.github import GitHub
from .drivers.sql import SQL


def QFrame(driver="SQL", *args, **kwargs):
    if driver == "SQL":
        return SQL(*args, **kwargs)
    elif driver == "GitHub":
        return GitHub(*args, **kwargs)