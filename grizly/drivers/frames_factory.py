from functools import partial

from .. import types
from ..sources.rdbms.sfdc import SFDB
from ..sources.sources_factory import Source
from ..sources.base import BaseReadSource
from ..store import Store
from ..utils.deprecation import deprecated_params
from .github import GitHubDriver
from .sfdc import SFDCDriver
from .sql import SQLDriver

deprecated_params = partial(deprecated_params, deprecated_in="0.4", removed_in="0.4.5")


class QFrame:
    @deprecated_params(params_mapping={"sqldb": "source"})
    def __new__(cls, dsn: str = None, source: types.Source = None, **kwargs):

        if source is None and dsn is None:
            raise ValueError("Please specify either source or dsn parameter")

        if source and not isinstance(source, BaseReadSource):
            raise ValueError("'source' must be an instance of Source")

        source = source or Source(dsn=dsn, **kwargs)

        if isinstance(source, SFDB):
            return SFDCDriver(source=source, **kwargs)
        else:
            return SQLDriver(source=source, **kwargs)

        # TODO: add github driver

    @classmethod
    def from_json(cls, path, key=None):
        store = Store.from_json(path, key=key)
        source_stanza = store.select.get("source")

        if not source_stanza:
            raise ValueError("Source is not specified in the JSON")

        dsn = source_stanza.get("dsn")
        source_name = source_stanza.get("source_name")
        dialect = source_stanza.get("dialect")

        source = Source(dsn=dsn, source_name=source_name, dialect=dialect)

        if isinstance(source, SFDB):
            return SFDCDriver(source=source).from_json(json_path=path, key=key)
        else:
            return SQLDriver(source=source).from_json(json_path=path, key=key)
