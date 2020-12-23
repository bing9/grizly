class JobNotFoundError(Exception):
    """Job not found in the registry"""

    pass


class JobRunNotFoundError(Exception):
    """Job run not found in the registry"""

    pass


class JobAlreadyRunningError(Exception):
    """"Job is already running"""

    pass
