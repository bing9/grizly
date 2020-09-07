import pytest
import dask
from ..grizly.dangerous.experimental import Extract
from ..grizly import QFrame
from ..grizly.utils import get_path
from ..grizly.config import config
from pandas import DataFrame


'''test data'''

excel_path = get_path("tables.xlsx", from_where="here")
engine_string = "sqlite:///" + get_path("Chinook.sqlite", from_where="here")
dsn = get_path("Chinook.sqlite", from_where="here")

orders = {
    "select": {
        "fields": {
            "Order": {"type": "dim", "as": "Bookings"},
            "Part": {"type": "dim", "as": "Part1"},
            "Customer": {"type": "dim", "as": "Customer"},
            "Value": {"type": "num"},
        },
        "table": "Orders",
    }
}

customers = {
    "select": {
        "fields": {
            "Country": {"type": "dim", "as": "Country"},
            "Customer": {"type": "dim", "as": "Customer"},
        },
        "table": "Customers",
    }
}


'''fixtures'''


@pytest.fixture(scope="session")
def extract_fixture():
    qf = QFrame(dsn=dsn, db="sqlite", dialect="mysql").from_dict(orders)
    df = qf.to_df()
    print(df)
    extract_fixture = Extract(name='extract_fixture',
                              driver=qf, store_backend='local')
    return extract_fixture


'''test functions'''


def test_get_existing_partitions(extract_fixture):
    # TODO
    extract_fixture.get_existing_partitions()
    pass


def test_get_distinct_values():
    # TODO
    pass


def test_get_partitions_to_download():
    # TODO
    pass
