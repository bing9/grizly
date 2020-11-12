import pytest
from .grizly.drivers.frames_factory import QFrame
from .grizly.sources.sources_factory import Source
from . import grizly

qf = QFrame(dsn="redshift_acoe", schema="grizly", table="sales")
sql_source = Source(dsn="redshift_acoe")


@pytest.fixture(autouse=True)
def add_objects(doctest_namespace):
    doctest_namespace["qf"] = qf.copy()
    doctest_namespace["sql_source"] = sql_source
    doctest_namespace["grizly"] = grizly
