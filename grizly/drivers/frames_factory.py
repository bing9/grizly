from functools import partial

from ..sources.rdbms.rdbms_factory import RDBMS
from ..utils.deprecation import deprecated_params
from .github import GitHubDriver
from .sfdc import SFDCDriver
from .sql import SQLDriver
from ..sources.rdbms.sfdc import SFDB
from ..types import Source

deprecated_params = partial(deprecated_params, deprecated_in="0.4", removed_in="0.4.5")


@deprecated_params(params_mapping={"sqldb": "source"})
def QFrame(dsn: str = None, source: Source = None, **kwargs):

    if source is None and dsn is None:
        raise ValueError("Please specify either source or dsn parameter")

    # TODO: below should be source factory not rdbms factory
    source = source or RDBMS(dsn=dsn, **kwargs)

    if isinstance(source, SFDB):
        return SFDCDriver(source=source, **kwargs)
    else:
        return SQLDriver(source=source, **kwargs)

    # TODO: add github driver
