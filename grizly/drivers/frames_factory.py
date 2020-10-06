from .github import GitHubDriver
from .sql import SQLDriver
from .sfdc import SFDCDriver


def QFrame(driver="SQL", *args, **kwargs):
    if driver == "SQL":
        return SQLDriver(*args, **kwargs)
    elif driver == "GitHub":
        return GitHubDriver(*args, **kwargs)
    elif driver == "SFDC":
        return SFDCDriver(*args, **kwargs)
