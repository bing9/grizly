import sqlparse
import os
from copy import deepcopy
from sqlalchemy import create_engine
from pandas import read_sql, read_csv, merge, concat

from ..grizly.utils import get_path

from ..grizly.tools.qframe import (
    QFrame,
    union,
    join,
    initiate,
    _build_column_strings,
    _get_sql,
)

excel_path = get_path("tables.xlsx", from_where="here")
engine_string = "sqlite:///" + get_path("Chinook.sqlite", from_where="here")

orders = {
    "select": {
        "fields": {
            "Order": {"type": "dim", "as": "Bookings"},
            "Part": {"type": "dim", "as": "Part"},
            "Customer": {"type": "dim", "as": "Customer"},
            "Value": {"type": "num"},
        },
        "table": "Orders",
    }
}

customers = {
    "select": {
        "fields": {"Country": {"type": "dim", "as": "Country"}, "Customer": {"type": "dim", "as": "Customer"},},
        "table": "Customers",
    }
}


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


def test_save_json_and_read_json1():
    q = QFrame().read_dict(deepcopy(customers))
    q.save_json("qframe_data.json")
    q.read_json("qframe_data.json")
    os.remove(os.path.join(os.getcwd(), "qframe_data.json"))
    assert q.data == customers


def test_save_json_and_read_json2():
    q = QFrame().read_dict(deepcopy(customers))
    q.save_json("qframe_data.json", "alias")
    q.read_json("qframe_data.json", "alias")
    os.remove(os.path.join(os.getcwd(), "qframe_data.json"))
    assert q.data == customers


def test_validation_data():
    QFrame().validate_data(deepcopy(orders))

    orders_c = deepcopy(orders)
    orders_c["select"]["fields"]["Customer"]["as"] = "ABC DEF"
    data = QFrame().validate_data(orders_c)

    assert data["select"]["fields"]["Customer"]["as"] == "ABC_DEF"


def test_read_dict():
    q = QFrame().read_dict(deepcopy(customers))
    assert q.data["select"]["fields"]["Country"] == {"type": "dim", "as": "Country"}

    q = QFrame().read_dict(deepcopy(orders))
    assert q.data["select"]["fields"]["Value"] == {"type": "num"}


def test_create_sql_blocks():
    q = QFrame().read_dict(deepcopy(orders))
    assert _build_column_strings(q.data)["select_names"] == [
        "Order as Bookings",
        "Part",
        "Customer",
        "Value",
    ]
    assert _build_column_strings(q.data)["select_aliases"] == [
        "Bookings",
        "Part",
        "Customer",
        "Value",
    ]
    assert q.create_sql_blocks().data["select"]["sql_blocks"] == _build_column_strings(q.data)


def test_rename():
    q = QFrame().read_dict(deepcopy(orders))
    q.rename({"Customer": "Customer Name", "Value": "Sales"})
    assert q.data["select"]["fields"]["Customer"]["as"] == "Customer_Name"
    assert q.data["select"]["fields"]["Value"]["as"] == "Sales"


def test_remove():
    q = QFrame().read_dict(deepcopy(orders))
    q.remove(["Part", "Order"])
    assert "Part" and "Order" not in q.data["select"]["fields"]


def test_distinct():
    q = QFrame().read_dict(deepcopy(orders))
    q.distinct()
    sql = q.get_sql()
    assert sql[7:15].upper() == "DISTINCT"


def test_query():
    q = QFrame().read_dict(deepcopy(orders))
    q.query("country!='France'")
    q.query("country!='Italy'", if_exists="replace")
    q.query("(Customer='Enel' or Customer='Agip')")
    q.query("Value>1000", operator="or")
    testexpr = "country!='Italy' and (Customer='Enel' or Customer='Agip') or Value>1000"
    assert q.data["select"]["where"] == testexpr


def test_having():
    q = QFrame().read_dict(deepcopy(orders))
    q.query("sum(Value)==1000")
    q.query("sum(Value)>1000", if_exists="replace")
    q.query("count(Customer)<=65")
    testexpr = "sum(Value)>1000 and count(Customer)<=65"
    assert q.data["select"]["where"] == testexpr


def test_assign():
    q = QFrame().read_dict(deepcopy(orders))
    value_x_two = "Value * 2"
    q.assign(value_x_two=value_x_two, type="num")
    q.assign(extract_date="format('yyyy-MM-dd', '2019-04-05 13:00:09')", custom_type="date")
    q.assign(Value_div="Value/100", type="num", order_by="DESC")
    assert q.data["select"]["fields"]["value_x_two"]["expression"] == "Value * 2"
    assert q.data["select"]["fields"]["Value_div"] == {
        "type": "num",
        "as": "Value_div",
        "group_by": "",
        "order_by": "DESC",
        "custom_type": "",
        "expression": "Value/100",
    }
    assert q.data["select"]["fields"]["extract_date"] == {
        "type": "dim",
        "as": "extract_date",
        "group_by": "",
        "custom_type": "date",
        "order_by": "",
        "expression": "format('yyyy-MM-dd', '2019-04-05 13:00:09')",
    }


def test_groupby():
    q = QFrame().read_dict(deepcopy(orders))
    q.groupby(["Order", "Customer"])
    order = {"type": "dim", "as": "Bookings", "group_by": "group"}
    customer = {"type": "dim", "as": "Customer", "group_by": "group"}
    assert q.data["select"]["fields"]["Order"] == order
    assert q.data["select"]["fields"]["Customer"] == customer


def test_agg():
    q = QFrame().read_dict(deepcopy(orders))
    q.groupby(["Order", "Customer"])["Value"].agg("sum")
    value = {"type": "num", "group_by": "sum"}
    assert q.data["select"]["fields"]["Value"] == value


def test_orderby():
    q = QFrame().read_dict(deepcopy(orders))
    q.orderby("Value")
    assert q.data["select"]["fields"]["Value"]["order_by"] == "ASC"

    q.orderby(["Order", "Part"], ascending=[False, True])
    assert q.data["select"]["fields"]["Order"]["order_by"] == "DESC"
    assert q.data["select"]["fields"]["Part"]["order_by"] == "ASC"

    sql = q.get_sql()

    testsql = """
            SELECT
                Order AS Bookings,
                Part,
                Customer,
                Value
            FROM Orders
            ORDER BY Bookings DESC,
                    Part,
                    Value
            """
    assert clean_testexpr(sql) == clean_testexpr(testsql)


def test_limit():
    q = QFrame().read_dict(deepcopy(orders))
    q.limit(10)
    sql = q.get_sql()
    assert sql[-8:].upper() == "LIMIT 10"


def test_select():
    q = QFrame().read_dict(deepcopy(orders))
    q.select(["Customer", "Value"])
    q.groupby("sq.Customer")["sq.Value"].agg("sum")

    sql = q.get_sql()
    # write_out(str(sql))
    testsql = """
            SELECT sq.Customer AS Customer,
                    sum(sq.Value) AS Value
                FROM
                (SELECT
                ORDER AS Bookings,
                        Part,
                        Customer,
                        Value
                FROM Orders) sq
                GROUP BY Customer
            """
    assert clean_testexpr(sql) == clean_testexpr(testsql)


def test_rearrange():
    q = QFrame().read_dict(deepcopy(customers))
    q.rearrange(["Customer", "Country"])
    assert q.get_fields() == ["Customer", "Country"]


def test_get_fields():
    q = QFrame().read_dict(deepcopy(customers))
    fields = ["Country", "Customer"]
    assert fields == q.get_fields()


def test_get_sql():
    q = QFrame().read_dict(deepcopy(orders))
    q.assign(New_case="CASE WHEN Bookings = 100 THEN 1 ELSE 0 END", type="num")
    q.limit(5)
    q.groupby(q.data["select"]["fields"])["Value"].agg("sum")
    testsql = """SELECT Order AS Bookings,
                    Part,
                    Customer,
                    sum(Value) AS Value,
                    CASE
                        WHEN Bookings = 100 THEN 1
                        ELSE 0
                    END AS New_case
                FROM Orders
                GROUP BY Order,
                        Part,
                        Customer,
                        New_case
                LIMIT 5
            """
    sql = q.get_sql()
    # write_out(str(sql))
    assert clean_testexpr(sql) == clean_testexpr(testsql)
    assert sql == _get_sql(q.data)


def test_to_csv():
    q = QFrame(
        engine=engine_string,
        data={
            "select": {
                "fields": {
                    "InvoiceLineId": {"type": "dim"},
                    "InvoiceId": {"type": "dim"},
                    "TrackId": {"type": "dim"},
                    "UnitPrice": {"type": "num"},
                    "Quantity": {"type": "num"},
                },
                "table": "InvoiceLine",
            }
        },
    )
    q.assign(UnitPriceFlag="CASE WHEN UnitPrice>1 THEN 1 ELSE 0 END", type="dim")
    q.rename({"TrackId": "Track"})

    csv_path = os.path.join(os.getcwd(), "invoice_items_test.csv")
    q.to_csv(csv_path)
    df_from_qf = read_csv(csv_path, sep="\t")

    os.remove(csv_path)

    engine = create_engine(engine_string)
    test_df = read_sql(sql=q.sql, con=engine)
    # write_out(str(test_df))
    assert df_from_qf.equals(test_df)


def test_to_df():
    data = {
        "select": {
            "fields": {
                "InvoiceLineId": {"type": "dim"},
                "InvoiceId": {"type": "dim"},
                "TrackId": {"type": "dim"},
                "UnitPrice": {"type": "num"},
                "Quantity": {"type": "num"},
            },
            "table": "InvoiceLine",
        }
    }

    q = QFrame(engine=engine_string).read_dict(data)
    q.assign(sales="Quantity*UnitPrice", type="num")
    q.groupby(["TrackId"])["Quantity"].agg("sum")
    df_from_qf = q.to_df()

    engine = create_engine(engine_string)
    test_df = read_sql(sql=q.sql, con=engine)
    # write_out(str(test_df))
    assert df_from_qf.equals(test_df)


playlists = {"select": {"fields": {"PlaylistId": {"type": "dim"}, "Name": {"type": "dim"}}, "table": "Playlist",}}


playlist_track = {
    "select": {"fields": {"PlaylistId": {"type": "dim"}, "TrackId": {"type": "dim"}}, "table": "PlaylistTrack",}
}


tracks = {
    "select": {
        "fields": {
            "TrackId": {"type": "dim"},
            "Name": {"type": "dim"},
            "AlbumId": {"type": "dim"},
            "MediaTypeId": {"type": "dim"},
            "GenreId": {"type": "dim"},
            "Composer": {"type": "dim"},
            "Milliseconds": {"type": "num"},
            "Bytes": {"type": "num"},
            "UnitPrice": {"type": "num"},
        },
        "table": "Track",
    }
}


def test_copy():
    qf = QFrame().read_dict(deepcopy(playlist_track))

    qf_copy = qf.copy()
    assert qf_copy.data == qf.data and qf_copy.sql == qf.sql and qf_copy.engine == qf.engine

    qf_copy.remove("TrackId").get_sql()
    assert qf_copy.data != qf.data and qf_copy.sql != qf.sql and qf_copy.engine == qf.engine


def test_join_1():
    # using grizly

    playlist_track_qf = QFrame(engine=engine_string).read_dict(deepcopy(playlist_track))
    playlists_qf = QFrame(engine=engine_string).read_dict(deepcopy(playlists))

    joined_qf = join([playlist_track_qf, playlists_qf], join_type="left join", on="sq1.PlaylistId=sq2.PlaylistId",)
    joined_df = joined_qf.to_df()

    # using pandas
    engine = create_engine(engine_string)

    playlist_track_qf.get_sql()
    pl_track_df = read_sql(sql=playlist_track_qf.sql, con=engine)

    playlists_qf.get_sql()
    pl_df = read_sql(sql=playlists_qf.sql, con=engine)

    test_df = merge(pl_track_df, pl_df, how="left", on=["PlaylistId"])

    assert joined_df.equals(test_df)

    # using grizly
    tracks_qf = QFrame(engine=engine_string).read_dict(deepcopy(tracks))

    joined_qf = join(
        qframes=[playlist_track_qf, playlists_qf, tracks_qf],
        join_type=["left join", "left join"],
        on=["sq1.PlaylistId=sq2.PlaylistId", "sq1.TrackId=sq3.TrackId"],
        unique_col=False,
    )

    joined_qf.remove(["sq2.PlaylistId", "sq3.TrackId"])
    joined_qf.rename({"sq2.Name": "Name_x", "sq3.Name": "Name_y"})
    joined_df = joined_qf.to_df()

    # using pandas
    tracks_qf.get_sql()
    tracks_df = read_sql(sql=tracks_qf.sql, con=engine)

    test_df = merge(test_df, tracks_df, how="left", on=["TrackId"])

    assert joined_df.equals(test_df)


def test_join_2():

    playlist_track_qf = QFrame(engine=engine_string).read_dict(deepcopy(playlist_track))
    playlists_qf = QFrame(engine=engine_string).read_dict(deepcopy(playlists))

    joined_qf = join([playlist_track_qf, playlists_qf], join_type="cross join", on=0)

    sql = joined_qf.get_sql()

    testsql = """
            SELECT sq1.PlaylistId AS PlaylistId,
                sq1.TrackId AS TrackId,
                sq2.Name AS Name
            FROM
            (SELECT PlaylistId,
                    TrackId
            FROM PlaylistTrack) sq1
            CROSS JOIN
            (SELECT PlaylistId,
                    Name
            FROM Playlist) sq2
            """

    assert clean_testexpr(sql) == clean_testexpr(testsql)

    joined_qf = join(
        [joined_qf, playlist_track_qf, playlists_qf],
        join_type=["RIGHT JOIN", "full join"],
        on=["sq1.PlaylistId=sq2.PlaylistId", "sq2.PlaylistId=sq3.PlaylistId"],
    )

    sql = joined_qf.get_sql()

    testsql = """
                SELECT sq1.PlaylistId AS PlaylistId,
                    sq1.TrackId AS TrackId,
                    sq1.Name AS Name
                FROM
                (SELECT sq1.PlaylistId AS PlaylistId,
                        sq1.TrackId AS TrackId,
                        sq2.Name AS Name
                FROM
                    (SELECT PlaylistId,
                            TrackId
                    FROM PlaylistTrack) sq1
                CROSS JOIN
                    (SELECT PlaylistId,
                            Name
                    FROM Playlist) sq2) sq1
                RIGHT JOIN
                (SELECT PlaylistId,
                        TrackId
                FROM PlaylistTrack) sq2 ON sq1.PlaylistId=sq2.PlaylistId
                FULL JOIN
                (SELECT PlaylistId,
                        Name
                FROM Playlist) sq3 ON sq2.PlaylistId=sq3.PlaylistId
            """

    assert clean_testexpr(sql) == clean_testexpr(testsql)


def test_union():
    playlists_qf = QFrame(engine=engine_string).read_dict(deepcopy(playlists))

    unioned_qf = union([playlists_qf, playlists_qf], "union")

    testsql = """
            SELECT PlaylistId,
                Name
            FROM Playlist
            UNION
            SELECT PlaylistId,
                Name
            FROM Playlist
            """
    sql = unioned_qf.get_sql()

    assert clean_testexpr(sql) == clean_testexpr(testsql)
    assert unioned_qf.to_df().equals(playlists_qf.to_df())

    unioned_qf = union([playlists_qf, playlists_qf], "union all")

    testsql = """
            SELECT PlaylistId,
                Name
            FROM Playlist
            UNION ALL
            SELECT PlaylistId,
                Name
            FROM Playlist
            """
    sql = unioned_qf.get_sql()

    assert clean_testexpr(sql) == clean_testexpr(testsql)
    assert unioned_qf.to_df().equals(concat([playlists_qf.to_df(), playlists_qf.to_df()], ignore_index=True))

    qf1 = playlists_qf.copy()
    qf1.rename({"Name": "Old_name"})
    qf1.assign(New_name="Name || '_new'")

    qf2 = playlists_qf.copy()
    qf2.rename({"Name": "New_name"})
    qf2.assign(Old_name="Name || '_old'")

    unioned_qf = union([qf1, qf2], union_type="union", union_by="name")

    testsql = """
            SELECT PlaylistId,
                Name AS Old_name,
                Name || '_new' AS New_name
            FROM Playlist
            UNION
            SELECT PlaylistId,
                Name || '_old' AS Old_name,
                Name AS New_name
            FROM Playlist
            """
    sql = unioned_qf.get_sql()

    # write_out(sql)
    assert clean_testexpr(sql) == clean_testexpr(testsql)


def test_initiate():
    columns = ["customer", "billings"]
    json = "test.json"
    sq = "test"
    initiate(
        columns=columns, schema="test_schema", table="test_table", engine_str="engine", json_path=json, subquery=sq,
    )
    q = QFrame().read_json(json_path=json, subquery=sq)
    os.remove("test.json")

    testsql = """
        SELECT customer,
            billings
        FROM test_schema.test_table
        """

    sql = q.get_sql()
    assert clean_testexpr(sql) == clean_testexpr(testsql)


def test_pyodbc_interface():
    qf = QFrame(engine="mssql+pyodbc://redshift_acoe", interface="pyodbc").read_dict(
        data={"select": {"fields": {"col1": {"type": "dim"}}, "schema": "administration", "table": "table_tutorial"}}
    )
    assert qf.interface == "pyodbc"

    df = qf.to_df(db="redshift")
    assert not df.empty
