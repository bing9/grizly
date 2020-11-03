import pytest
from .grizly.drivers.frames_factory import QFrame

qf = QFrame(dsn="redshift_acoe", schema="grizly", table="sales")


@pytest.fixture(autouse=True)
def add_qf(doctest_namespace):
    doctest_namespace["qf"] = qf.copy()
