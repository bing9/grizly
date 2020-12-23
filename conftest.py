from copy import deepcopy

import pytest

from . import grizly
from .grizly.drivers.frames_factory import QFrame
from .grizly.sources.sources_factory import Source
from .grizly.utils.functions import get_path

# DOCTESTS
# --------
qf_doc_strings = QFrame(dsn="redshift_acoe", schema="grizly", table="sales")
sql_source_doc_strings = Source(dsn="redshift_acoe")


@pytest.fixture(autouse=True)
def add_objects(doctest_namespace):
    doctest_namespace["qf"] = qf_doc_strings.copy()
    doctest_namespace["sql_source"] = sql_source_doc_strings
    doctest_namespace["grizly"] = grizly


# STANDARD TESTS
# --------------
@pytest.fixture(scope="session")
def sources():
    dsn = get_path("Chinook.sqlite", from_where="here")
    source = Source(dsn=dsn, source_name="sqlite", dialect="mysql")
    sources = {"sqlite": source}
    return sources


@pytest.fixture(scope="session")
def sqlite(sources):
    return sources["sqlite"]


@pytest.fixture(scope="session")
def orders_data():
    data = {
        "select": {
            "fields": {
                "Order": {"dtype": "VARCHAR(500)", "as": "Bookings"},
                "Part": {"dtype": "VARCHAR(500)", "as": "Part1"},
                "Customer": {"dtype": "VARCHAR(500)", "as": "Customer"},
                "Value": {"dtype": "FLOAT(53)"},
                "HiddenColumn": {"dtype": "CHAR(1)", "select": 0},
            },
            "table": "Orders",
            "source": {
                "dsn": "Chinook.sqlite",
                "source_name": "sqlite",
                "dialect": "mysql"
            }
        }
    }
    return deepcopy(data)


@pytest.fixture(scope="session")
def customers_data():
    data = {
        "select": {
            "fields": {
                "Country": {"dtype": "VARCHAR(500)", "as": "Country"},
                "Customer": {"dtype": "VARCHAR(500)", "as": "Customer"},
            },
            "table": "Customers",
            "source": {
                "dsn": "Chinook.sqlite",
                "source_name": "sqlite",
                "dialect": "mysql"
            }
        }
    }
    return deepcopy(data)


@pytest.fixture(scope="session")
def qframes(sqlite, orders_data, customers_data):
    """Load columns and dtypes only once in the session"""
    qframes = {
        "orders_qf": QFrame(source=sqlite).from_dict(data=orders_data),
        "customers_qf": QFrame(source=sqlite).from_dict(data=customers_data),
        "table_tutorial_qf": QFrame(dsn="redshift_acoe").from_table(
            table="table_tutorial", schema="grizly"
        ),
        "invoice_line_qf": QFrame(source=sqlite, table="InvoiceLine"),
        "playlist_qf": QFrame(source=sqlite, table="Playlist"),
        "playlist_track_qf": QFrame(source=sqlite, table="PlaylistTrack"),
        "track_qf": QFrame(source=sqlite, table="Track"),
    }
    return qframes


@pytest.fixture(scope="function")
def orders_qf(qframes):
    return qframes["orders_qf"].copy()


@pytest.fixture(scope="function")
def customers_qf(qframes):
    return qframes["customers_qf"].copy()


@pytest.fixture(scope="function")
def table_tutorial_qf(qframes):
    return qframes["table_tutorial_qf"].copy()


@pytest.fixture(scope="function")
def invoice_line_qf(qframes):
    return qframes["invoice_line_qf"].copy()


@pytest.fixture(scope="function")
def playlist_qf(qframes):
    return qframes["playlist_qf"].copy()


@pytest.fixture(scope="function")
def playlist_track_qf(qframes):
    return qframes["playlist_track_qf"].copy()


@pytest.fixture(scope="function")
def track_qf(qframes):
    return qframes["track_qf"].copy()


@pytest.fixture(
    scope="function", params=["orders_qf", "customers_qf", "table_tutorial_qf", "invoice_line_qf"]
)
def qf(orders_qf, customers_qf, table_tutorial_qf, invoice_line_qf, request):
    return eval(request.param)


@pytest.fixture(scope="function", params=["table_tutorial_qf", "invoice_line_qf"])
def qf_out(table_tutorial_qf, invoice_line_qf, request):
    """QFrame which will be tested in to_* methods"""
    return eval(request.param)


def write_out(out):
    with open(get_path("output.sql", from_where="here"), "w",) as f:
        f.write(out)


def clean_testexpr(testsql):
    testsql = testsql.replace("\n", "")
    testsql = testsql.replace("\t", "")
    testsql = testsql.replace("\r", "")
    testsql = testsql.replace("  ", "")
    testsql = testsql.replace(" ", "")
    testsql = testsql.lower()
    return testsql
