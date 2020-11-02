import pytest
from .grizly.drivers.frames_factory import QFrame


@pytest.fixture(autouse=True)
def add_qf(doctest_namespace):
    qf = QFrame(dsn="redshift_acoe", schema="grizly", table="sales")
    doctest_namespace["qf"] = qf.copy()
