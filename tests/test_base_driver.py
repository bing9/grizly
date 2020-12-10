import itertools
import os
import random

from pandas import DataFrame, read_csv, read_excel, read_parquet
from pyarrow import Table
import pytest

from ..conftest import clean_testexpr, write_out
from ..grizly.drivers.base import BaseDriver
from ..grizly.drivers.frames_factory import QFrame
from ..grizly.store import Store


def draw_fields(qf, aliased):
    """Draw fields from a QFrame"""
    sample_size = max(qf.ncols - 1, 1)
    fields = qf.get_fields(aliased=aliased, not_selected=True)
    random_fields = random.sample(fields, sample_size)

    return random_fields


def test___len__(playlist_qf):
    qf = playlist_qf
    assert len(qf) == 18


@pytest.mark.parametrize(
    "agg,aliased", list(itertools.product(BaseDriver._allowed_agg, [True, False]))
)
def test_agg(agg, aliased, qf):
    to_be_aggregated = draw_fields(qf, aliased)
    to_be_aggregated_fields_names = qf._get_fields_names(to_be_aggregated)

    qf.groupby()[to_be_aggregated].agg(agg)
    for field in to_be_aggregated_fields_names:
        assert qf.data["select"]["fields"][field]["group_by"] == agg


@pytest.mark.parametrize(
    "expr,dtype,group_by,order_by",
    [
        ("Value * 2", "FLOAT(53)", "", "DESC"),
        ("format('yyyy-MM-dd', '2019-04-05 13:00:09')", "date", "", ""),
    ],
)
def test_assign_1(expr, dtype, order_by, group_by, orders_qf):
    orders_qf.assign(new_field=expr, dtype=dtype, order_by=order_by, group_by=group_by)
    field = orders_qf.data["select"]["fields"]["new_field"]
    assert field["expression"] == expr
    assert field["order_by"] == order_by
    assert field["dtype"] == dtype
    assert field["group_by"] == group_by


def test_assign_2(orders_qf):
    """Check if assign replaces existing field by alias"""
    orders_qf.select("*")
    orders_qf.assign(MyCustomer="'Me'", dtype="CHAR(2)")
    assert orders_qf.data["select"]["fields"]["MyCustomer"]["expression"] == "'Me'"
    assert "Customer" not in orders_qf.data["select"]["fields"]


def test_columns(customers_qf):
    fields = ["Country", "Customer"]
    assert fields == customers_qf.columns


def test_copy(playlist_track_qf):
    qf = playlist_track_qf

    qf_copy = qf.copy()
    assert qf_copy.data == qf.data and qf_copy.source == qf.source

    qf_copy.remove("TrackId")
    assert qf_copy.data != qf.data and qf_copy.source == qf.source


def test_cut(playlist_qf):
    qf = playlist_qf
    assert len(qf) == 18

    qframes1 = qf.cut(18)
    test_len = 0
    for q in qframes1:
        test_len += len(q)
    assert len(qf) == test_len

    with pytest.raises(ValueError):
        qf.cut(2, order_by=["Name"])

    qframes2 = qf.cut(18, order_by=["PlaylistId"])
    assert len(qframes1) == len(qframes2)


def test_data(qf):
    data = qf.data
    assert isinstance(data, Store)


def test_distinct(qf):
    qf.distinct()
    assert qf.data["select"]["distinct"] == 1


def test_dtypes(track_qf):
    dtypes = ["INTEGER", "NVARCHAR(200)", "INTEGER", "INTEGER"]
    assert track_qf.dtypes[:4] == dtypes


def test_fields(customers_qf):
    fields = ["Country", "Customer"]
    assert fields == customers_qf.fields


def test_fix_types():
    # TODO
    pass


def test_from_dict(orders_qf):
    assert orders_qf.data["select"]["fields"]["Value"] == {"dtype": "FLOAT(53)"}


@pytest.mark.parametrize("key", ["key", None])
def test_from_json_local(key, customers_qf, customers_data):
    customers_qf.store.to_json("qframe_data.json", key=key)
    qf = QFrame(source=customers_qf.source).from_json(json_path="qframe_data.json", key=key)
    os.remove("qframe_data.json")
    assert qf.store.to_dict() == customers_data


@pytest.mark.parametrize("key", ["key", None])
def test_from_json_s3(key, customers_qf, customers_data):
    path = "s3://acoe-s3/test/test_from_json_s3_1.json"
    customers_qf.store.to_json(path, key=key)
    qf = QFrame(source=customers_qf.source).from_json(json_path=path, key=key)
    assert qf.store.to_dict() == customers_data


def test_from_json_github():
    # TODO
    pass


def test_get_dtypes(customers_qf):
    dtypes = ["VARCHAR(500)", "VARCHAR(500)"]
    assert customers_qf.get_dtypes() == dtypes


def test_get_fields(customers_qf):
    fields = ["Country", "Customer"]
    assert fields == customers_qf.get_fields()


@pytest.mark.parametrize("aliased", [True, False])
def test_groupby(aliased, qf):
    to_be_grouped = draw_fields(qf, aliased)
    to_be_grouped_fields_names = qf._get_fields_names(to_be_grouped)

    qf.groupby(to_be_grouped)
    for field in to_be_grouped_fields_names:
        assert qf.data["select"]["fields"][field]["group_by"] == "group"


def test_groupby_all(qf):
    qf.groupby()
    for field in qf.data["select"]["fields"].values():
        assert field["group_by"] == "group"


def test_having(orders_qf):
    orders_qf.having("sum(Value)=1000")
    orders_qf.having("sum(Value)>1000", if_exists="replace")
    orders_qf.having("count(Customer)<=65")
    testexpr = "sum(Value)>1000 and count(Customer)<=65"
    assert orders_qf.data["select"]["having"] == testexpr


def test_limit(qf):
    qf.limit(10)
    assert qf.data["select"]["limit"] == "10"


def test_ncols(table_tutorial_qf):
    assert table_tutorial_qf.ncols == 4


def test_nrows(table_tutorial_qf):
    assert table_tutorial_qf.nrows == 2


def test_offset(qf):
    qf.offset(10)
    assert qf.data["select"]["offset"] == "10"


def test_orderby(orders_qf):
    orders_qf.orderby("Value")
    assert orders_qf.data["select"]["fields"]["Value"]["order_by"] == "ASC"

    orders_qf.orderby(["Order", "Part"], ascending=[False, True])
    assert orders_qf.data["select"]["fields"]["Order"]["order_by"] == "DESC"
    assert orders_qf.data["select"]["fields"]["Part"]["order_by"] == "ASC"


def test_orderby_aliased(orders_qf):
    orders_qf.orderby("Value")
    assert orders_qf.data["select"]["fields"]["Value"]["order_by"] == "ASC"

    orders_qf.orderby(["Bookings", "Part1"], ascending=[False, True])
    assert orders_qf.data["select"]["fields"]["Order"]["order_by"] == "DESC"
    assert orders_qf.data["select"]["fields"]["Part"]["order_by"] == "ASC"


def test_where(orders_qf):
    orders_qf.where("country!='France'")
    assert orders_qf.data["select"]["where"] == "country!='France'"

    orders_qf.where("country!='Italy'", if_exists="replace")
    assert orders_qf.data["select"]["where"] == "country!='Italy'"

    orders_qf.where("(Customer='Enel' or Customer='Agip')")
    orders_qf.where("Value>1000", operator="or")
    testexpr = "country!='Italy' and (Customer='Enel' or Customer='Agip') or Value>1000"
    assert orders_qf.data["select"]["where"] == testexpr


@pytest.mark.parametrize("aliased", [True, False])
def test_rearrange(aliased, qf):
    new_fields_order = qf.get_fields(aliased=aliased, not_selected=True)
    random.shuffle(new_fields_order)

    with pytest.raises(ValueError):
        fields_with_one_missing = new_fields_order[:-1]
        qf.rearrange(fields_with_one_missing)

    with pytest.raises(ValueError):
        fileds_with_one_wrong = new_fields_order[:-1]
        fileds_with_one_wrong.append("wrong_field")
        qf.rearrange(fileds_with_one_wrong)

    qf.rearrange(new_fields_order)
    assert qf.get_fields(aliased=aliased, not_selected=True) == new_fields_order


@pytest.mark.parametrize("aliased", [True, False])
def test_remove(aliased, qf):
    to_be_removed = draw_fields(qf, aliased)

    qf.remove(to_be_removed)
    current_fields = qf.get_fields(aliased=aliased, not_selected=True)
    assert set(to_be_removed).intersection(set(current_fields)) == set()


@pytest.mark.parametrize("aliased", [True, False])
def test_rename(aliased, qf):
    to_be_renamed = draw_fields(qf, aliased)
    rename_mapping = {old_name: f"new_{old_name}" for old_name in to_be_renamed}

    qf.rename(rename_mapping)
    current_fields_aliases = qf.get_fields(aliased=True, not_selected=True)
    assert set(to_be_renamed).intersection(set(current_fields_aliases)) == set()
    assert set(rename_mapping.values()) - set(current_fields_aliases) == set()


def test_select(orders_qf):
    orders_qf.select(["Customer", "Value"])
    orders_qf.groupby("sq.Customer")["sq.Value"].agg("sum")

    orders_qf.select("sq.Value")
    orders_qf.select("Value")
    sql = orders_qf.get_sql()

    testsql = """
            SELECT sq."Value" AS "Value"
            FROM
            (SELECT sq."Value" AS "Value"
            FROM
                (SELECT sq."Customer" AS "Customer",
                        sum(sq."Value") AS "Value"
                FROM
                    (SELECT
                        "Order" AS "Bookings",
                        "Part" AS "Part1",
                        "Customer",
                        "Value"
                    FROM Orders) sq
                GROUP BY 1) sq) sq
            """
    assert clean_testexpr(sql) == clean_testexpr(testsql)


def test_shape(table_tutorial_qf):
    assert table_tutorial_qf.shape == (2, 4)


def test_show_duplicated_columns():
    # TODO
    pass


@pytest.mark.parametrize("aliased", [True, False])
def test_sum(aliased, qf):
    to_be_aggregated = draw_fields(qf, aliased)
    to_be_grouped = set(qf.get_fields(aliased=aliased, not_selected=True)) - set(to_be_aggregated)

    qf1 = qf.copy()
    qf1.groupby(to_be_grouped).sum()

    qf2 = qf.copy()
    qf2.groupby()[to_be_aggregated].agg("sum")
    assert qf1.data == qf2.data


def test_to_arrow(qf_out):
    qf = qf_out
    table = qf.to_arrow()
    assert isinstance(table, Table)
    assert qf.get_fields(aliased=True) == list(table.column_names)


def test_to_crosstab(table_tutorial_qf):
    table_tutorial_qf.orderby("col1")
    test_html = table_tutorial_qf.to_crosstab(
        dimensions=["col1", "col2"], measures=["col4"]
    ).to_html()
    html = """<table>
            <thead>
            <tr>
                <th> col1 </th>
                <th> col2 </th>
                <th> col4 </th>
            </tr>
            </thead>
            <tbody>
            <tr>
                <th> item1 </th>
                <th> 1.3 </th>
                <td> 3.5 </td>
            </tr>
            <tr>
                <th> item2 </th>
                <th> 0.0 </th>
                <td> 0 </td>
            </tr>
            </tbody>
            </table>"""
    assert clean_testexpr(test_html) == clean_testexpr(html)


def test_to_csv(qf_out):
    qf = qf_out
    path = "test.csv"
    qf.to_csv(path)
    df = read_csv(path, sep="\t")
    os.remove(path)

    assert isinstance(df, DataFrame)
    assert qf.get_fields(aliased=True) == list(df.columns)


def test_to_df(qf_out):
    qf = qf_out
    df = qf.to_df()
    assert isinstance(df, DataFrame)
    assert qf.get_fields(aliased=True) == list(df.columns)


def test_to_dict(qf_out):
    qf = qf_out
    data = qf.to_dict()
    assert isinstance(data, dict)
    assert qf.get_fields(aliased=True) == list(data.keys())


def test_to_dicts():
    # TODO
    pass


def test_to_excel(qf_out):
    qf = qf_out
    path = "test.xlsx"
    qf.to_excel(path)
    df = read_excel(path)
    os.remove(path)

    assert isinstance(df, DataFrame)
    assert qf.get_fields(aliased=True) == list(df.columns)


def test_to_parquet(qf_out):
    qf = qf_out
    path = "test.parquet"
    qf.to_parquet(path)
    df = read_parquet(path)
    os.remove(path)

    assert isinstance(df, DataFrame)
    assert qf.get_fields(aliased=True) == list(df.columns)


def test_to_records(qf_out):
    qf = qf_out
    records = qf.to_records()
    assert isinstance(records, list)
    assert all(isinstance(i, tuple) for i in records)
    assert len(qf) == len(records)


def test_types(customers_qf):
    dtypes = ["VARCHAR(500)", "VARCHAR(500)"]
    assert customers_qf.get_dtypes() == dtypes


def test_window(qf):
    qf.window(2, 3)
    assert qf.data["select"]["offset"] == "2"
    assert qf.data["select"]["limit"] == "3"


# OTHER TESTS
# -----------


def test_not_selected_fields(sqlite):
    q = QFrame(
        source=sqlite,
        store={
            "select": {
                "fields": {
                    "InvoiceLineId": {"dtype": "VARCHAR(500)"},
                    "InvoiceId": {"dtype": "VARCHAR(500)", "select": 0},
                    "TrackId": {"dtype": "VARCHAR(500)", "select": 0},
                    "UnitPrice": {"dtype": "FLOAT(53)"},
                },
                "table": "InvoiceLine",
            }
        },
    )
    q.groupby()["UnitPrice"].sum()
    q.orderby(["InvoiceLineId", "InvoiceId"])
    q.rename({"InvoiceId": "NewName"})
    assert q.data["select"]["fields"]["InvoiceId"] == {
        "dtype": "VARCHAR(500)",
        "select": 0,
        "group_by": "group",
        "order_by": "ASC",
        "as": "NewName",
    }

    fields = ["TrackId", "InvoiceLineId", "UnitPrice", "NewName"]
    q.rearrange(fields)
    assert fields == q.get_fields(aliased=True, not_selected=True)

    fields = ["InvoiceLineId", "UnitPrice"]
    assert fields == q.get_fields()

    sql = """SELECT "InvoiceLineId",
                "UnitPrice"
            FROM InvoiceLine
            GROUP BY "TrackId",
                    1,
                    2,
                    "InvoiceId"
            ORDER BY 1,
                    "InvoiceId"
            """
    assert clean_testexpr(q.get_sql()) == clean_testexpr(sql)

