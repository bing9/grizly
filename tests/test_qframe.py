import pytest
import warnings
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
    _get_sql,
)

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


def test_save_json_and_from_json1():
    q = QFrame(dsn=dsn, db="sqlite", dialect="mysql").from_dict(customers)
    q.save_json("qframe_data.json")
    q.from_json("qframe_data.json")
    os.remove(os.path.join(os.getcwd(), "qframe_data.json"))
    assert q.data == customers


def test_save_json_and_from_json2():
    q = QFrame(dsn=dsn, db="sqlite", dialect="mysql").from_dict(customers)
    q.save_json("qframe_data.json", "alias")
    q.from_json("qframe_data.json", "alias")
    os.remove(os.path.join(os.getcwd(), "qframe_data.json"))
    assert q.data == customers

def test_from_json_s3():
    q = QFrame(dsn=dsn, db="sqlite", dialect="mysql").from_json("s3://acoe-s3/test/test_from_json_s3.json", subquery="test_subquery")
    assert len(q.get_fields()) == 41

def test_validation_data():
    QFrame(dsn=dsn, db="sqlite", dialect="mysql").validate_data(orders)

    orders_c = deepcopy(orders)
    orders_c["select"]["fields"]["Customer"]["as"] = "ABC DEF"
    data = QFrame(dsn=dsn, db="sqlite", dialect="mysql").validate_data(orders_c)

    assert data["select"]["fields"]["Customer"]["as"] == "ABC_DEF"


def test_from_dict():
    q = QFrame(dsn=dsn, db="sqlite", dialect="mysql").from_dict(customers)
    assert q.data["select"]["fields"]["Country"] == {"type": "dim", "as": "Country"}

    q = QFrame(dsn=dsn, db="sqlite", dialect="mysql").from_dict(orders)
    assert q.data["select"]["fields"]["Value"] == {"type": "num"}


def test_create_sql_blocks():
    q = QFrame(dsn=dsn, db="sqlite", dialect="mysql").from_dict(orders)
    assert q._build_column_strings()["select_names"] == [
        '"Order" as "Bookings"',
        '"Part" as "Part1"',
        '"Customer"',
        '"Value"',
    ]
    assert q._build_column_strings()["select_aliases"] == [
        "Bookings",
        "Part1",
        "Customer",
        "Value",
    ]
    assert q.create_sql_blocks().data["select"]["sql_blocks"] == q._build_column_strings()


def test_rename():
    q = QFrame(dsn=dsn, db="sqlite", dialect="mysql").from_dict(orders)
    q.rename({"Customer": "Customer Name", "Value": "Sales"})
    assert q.data["select"]["fields"]["Customer"]["as"] == "Customer Name"
    assert q.data["select"]["fields"]["Value"]["as"] == "Sales"


def test_rename_aliased():
    q = QFrame(dsn=dsn, db="sqlite", dialect="mysql").from_dict(orders)
    q.rename({"Part1": "Part2", "not_found_test": "test", "Bookings": "Bookings_test", "Value": "Sales"})
    assert q.data["select"]["fields"]["Part"]["as"] == "Part2"
    assert q.data["select"]["fields"]["Order"]["as"] == "Bookings_test"
    assert q.data["select"]["fields"]["Value"]["as"] == "Sales"


def test_remove():
    q = QFrame(dsn=dsn, db="sqlite", dialect="mysql").from_dict(orders)
    q.remove(["Part", "Order"])
    assert "Part" and "Order" not in q.data["select"]["fields"]


def test_remove_aliased():
    q = QFrame(dsn=dsn, db="sqlite", dialect="mysql").from_dict(orders)
    q.remove(["Part1", "Bookings"])
    assert "Part" and "Order" not in q.data["select"]["fields"]
    assert "Value" in q.data["select"]["fields"]


def test_distinct():
    q = QFrame(dsn=dsn, db="sqlite", dialect="mysql").from_dict(orders)
    q.distinct()
    sql = q.get_sql()
    assert sql[7:15].upper() == "DISTINCT"


def test_query():
    q = QFrame(dsn=dsn, db="sqlite", dialect="mysql").from_dict(orders)
    q.query("country!='France'")
    q.query("country!='Italy'", if_exists="replace")
    q.query("(Customer='Enel' or Customer='Agip')")
    q.query("Value>1000", operator="or")
    testexpr = "country!='Italy' and (Customer='Enel' or Customer='Agip') or Value>1000"
    assert q.data["select"]["where"] == testexpr


def test_having_from_dict():
    q = QFrame(dsn=dsn, db="sqlite", dialect="mysql").from_dict(orders)
    q.having("sum(Value)=1000")
    q.having("sum(Value)>1000", if_exists="replace")
    q.having("count(Customer)<=65")
    testexpr = "sum(Value)>1000 and count(Customer)<=65"
    assert q.data["select"]["having"] == testexpr

def test_having_from_table():
    q = QFrame(dsn=dsn, db="sqlite", dialect="mysql").from_table(table="Track")
    q.having("sum(Value)=1000")
    assert q.data["select"]["having"] == "sum(Value)=1000"
    q.having("sum(Value)>1000", if_exists="replace")
    q.having("count(Customer)<=65")
    testexpr = "sum(Value)>1000 and count(Customer)<=65"
    assert q.data["select"]["having"] == testexpr


def test_assign():
    q = QFrame(dsn=dsn, db="sqlite", dialect="mysql").from_dict(orders)
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
    q = QFrame(dsn=dsn, db="sqlite", dialect="mysql").from_dict(orders)
    q.groupby(["Order", "Customer"])
    order = {"type": "dim", "as": "Bookings", "group_by": "group"}
    customer = {"type": "dim", "as": "Customer", "group_by": "group"}
    assert q.data["select"]["fields"]["Order"] == order
    assert q.data["select"]["fields"]["Customer"] == customer


def test_groupby_aliased():
    q = QFrame(dsn=dsn, db="sqlite", dialect="mysql").from_dict(orders)
    q.groupby(["Bookings", "Customer"])
    order = {"type": "dim", "as": "Bookings", "group_by": "group"}
    customer = {"type": "dim", "as": "Customer", "group_by": "group"}
    assert q.data["select"]["fields"]["Order"] == order
    assert q.data["select"]["fields"]["Customer"] == customer


def test_groupby_all():
    q = QFrame(dsn=dsn, db="sqlite", dialect="mysql").from_dict(orders)
    q.groupby().create_sql_blocks()
    fields_1 = q.data["select"]["sql_blocks"]["group_dimensions"]
    fields_2 = ["1", "2", "3", "4"]
    assert fields_1 == fields_2


def test_agg():
    q = QFrame(dsn=dsn, db="sqlite", dialect="mysql").from_dict(orders)
    q.groupby(["Order", "Customer"])["Value"].agg("sum")
    value = {"type": "num", "group_by": "sum"}
    assert q.data["select"]["fields"]["Value"] == value


def test_agg_aliased():
    q = QFrame(dsn=dsn, db="sqlite", dialect="mysql").from_dict(orders)
    q.rename({"Value": "NewValue"})
    q.groupby(["Order", "Customer"])["NewValue"].agg("sum")
    value = {"as": "NewValue", "type": "num", "group_by": "sum"}
    assert q.data["select"]["fields"]["Value"] == value


def test_orderby():
    q = QFrame(dsn=dsn, db="sqlite", dialect="mysql").from_dict(orders)
    q.orderby("Value")
    assert q.data["select"]["fields"]["Value"]["order_by"] == "ASC"

    q.orderby(["Order", "Part"], ascending=[False, True])
    assert q.data["select"]["fields"]["Order"]["order_by"] == "DESC"
    assert q.data["select"]["fields"]["Part"]["order_by"] == "ASC"

    sql = q.get_sql()

    testsql = """
            SELECT
                "Order" AS "Bookings",
                "Part" AS "Part1",
                "Customer",
                "Value"
            FROM Orders
            ORDER BY 1 DESC,
                    2,
                    4
            """
    assert clean_testexpr(sql) == clean_testexpr(testsql)


def test_orderby_aliased():
    q = QFrame(dsn=dsn, db="sqlite", dialect="mysql").from_dict(orders)
    q.orderby("Value")
    assert q.data["select"]["fields"]["Value"]["order_by"] == "ASC"

    q.orderby(["Bookings", "Part1"], ascending=[False, True])
    assert q.data["select"]["fields"]["Order"]["order_by"] == "DESC"
    assert q.data["select"]["fields"]["Part"]["order_by"] == "ASC"

    sql = q.get_sql()

    testsql = """
            SELECT
                "Order" AS "Bookings",
                "Part" AS "Part1",
                "Customer",
                "Value"
            FROM Orders
            ORDER BY 1 DESC,
                    2,
                    4
            """
    assert clean_testexpr(sql) == clean_testexpr(testsql)


def test_limit():
    q = QFrame(dsn=dsn, db="sqlite", dialect="mysql").from_dict(orders)
    q.limit(10)
    sql = q.get_sql()
    assert sql[-8:].upper() == "LIMIT 10"


def test_select():
    q = QFrame(dsn=dsn, db="sqlite", dialect="mysql").from_dict(orders)
    q.select(["Customer", "Value"])
    q.groupby("sq.Customer")["sq.Value"].agg("sum")

    q.select("sq.Value")
    q.select("Value")
    sql = q.get_sql()

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


def test_rearrange():
    q = QFrame(dsn=dsn, db="sqlite", dialect="mysql").from_dict(customers)
    q.rearrange(["Customer", "Country"])
    assert q.get_fields() == ["Customer", "Country"]


def test_rearrange_aliased():
    q = QFrame(dsn=dsn, db="sqlite", dialect="mysql").from_dict(orders)

    with pytest.raises(ValueError):
        q.rearrange(["Part1", "Order", "Value", "random_field"])

    with pytest.raises(ValueError):
        q.rearrange(["Part1", "Order", "Value"])

    q.rearrange(["Part1", "Order", "Value", "Customer"])
    assert q.get_fields() == ["Part", "Order", "Value", "Customer"]


def test_get_fields():
    q = QFrame(dsn=dsn, db="sqlite", dialect="mysql").from_dict(customers)
    fields = ["Country", "Customer"]
    assert fields == q.get_fields()


def test_not_selected_fields():
    q = QFrame(
        dsn=dsn,
        db="sqlite",
        dialect="mysql",
        data={
            "select": {
                "fields": {
                    "InvoiceLineId": {"type": "dim"},
                    "InvoiceId": {"type": "dim", "select": 0},
                    "TrackId": {"type": "dim", "select": 0},
                    "UnitPrice": {"type": "num"},
                },
                "table": "InvoiceLine",
            }
        },
    )
    q.groupby()["UnitPrice"].sum()
    q.orderby(["InvoiceLineId", "InvoiceId"])
    q.rename({"InvoiceId": "NewName"})
    assert q.data["select"]["fields"]["InvoiceId"] == {
        "type": "dim",
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
    # write_out(q.get_sql())

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


def test_get_sql():
    q = QFrame(dsn=dsn, db="sqlite", dialect="mysql").from_dict(orders)
    q.assign(New_case="CASE WHEN Bookings = 100 THEN 1 ELSE 0 END", type="num")
    q.limit(5)
    q.groupby(q.data["select"]["fields"])["Value"].agg("sum")
    testsql = """SELECT "Order" AS "Bookings",
                    "Part" AS "Part1",
                    "Customer",
                    sum("Value") AS "Value",
                    CASE
                        WHEN Bookings = 100 THEN 1
                        ELSE 0
                    END AS "New_case"
                FROM Orders
                GROUP BY 1,
                        2,
                        3,
                        5
                LIMIT 5
            """
    sql = q.get_sql()
    # write_out(str(sql))
    assert clean_testexpr(sql) == clean_testexpr(testsql)
    assert sql == _get_sql(q.data, q.sqldb)


def test_to_csv():
    q = QFrame(
        dsn=dsn,
        db="sqlite",
        dialect="mysql",
        data={
            "select": {
                "fields": {
                    "InvoiceLineId": {"type": "dim"},
                    "InvoiceId": {"type": "dim"},
                    "TrackId": {"type": "dim"},
                    "UnitPrice": {"type": "num"},
                    "Quantity": {"type": "num", "select": 0},
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

    q = QFrame(dsn=dsn, db="sqlite", dialect="mysql").from_dict(data)
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
    qf = QFrame(dsn=dsn, db="sqlite", dialect="mysql").from_dict(deepcopy(playlist_track))

    qf_copy = qf.copy()
    assert qf_copy.data == qf.data and qf_copy.sql == qf.sql and qf_copy.sqldb == qf.sqldb

    qf_copy.remove("TrackId").get_sql()
    assert qf_copy.data != qf.data and qf_copy.sql != qf.sql and qf_copy.sqldb == qf.sqldb


def test_join_1():
    # using grizly

    playlist_track_qf = QFrame(dsn=dsn, db="sqlite", dialect="mysql").from_dict(deepcopy(playlist_track))
    playlists_qf = QFrame(dsn=dsn, db="sqlite", dialect="mysql").from_dict(deepcopy(playlists))

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
    tracks_qf = QFrame(dsn=dsn, db="sqlite", dialect="mysql").from_dict(deepcopy(tracks))

    joined_qf = join(
        qframes=[playlist_track_qf, playlists_qf, tracks_qf],
        join_type=["left join", "left join"],
        on=["sq1.PlaylistId=sq2.PlaylistId", "sq1.TrackId=sq3.TrackId"],
        unique_col=False,
    )

    assert joined_qf.get_fields(aliased=True) == [
        "PlaylistId",
        "TrackId",
        "PlaylistId",
        "Name",
        "TrackId",
        "Name",
        "AlbumId",
        "MediaTypeId",
        "GenreId",
        "Composer",
        "Milliseconds",
        "Bytes",
        "UnitPrice",
    ]
    assert joined_qf.get_fields(aliased=False) == [
        "sq1.PlaylistId",
        "sq1.TrackId",
        "sq2.PlaylistId",
        "sq2.Name",
        "sq3.TrackId",
        "sq3.Name",
        "sq3.AlbumId",
        "sq3.MediaTypeId",
        "sq3.GenreId",
        "sq3.Composer",
        "sq3.Milliseconds",
        "sq3.Bytes",
        "sq3.UnitPrice",
    ]

    joined_qf.remove(["sq2.PlaylistId", "sq3.TrackId"])
    joined_qf.rename({"sq2.Name": "Name_x", "sq3.Name": "Name_y"})
    joined_df = joined_qf.to_df()

    # using pandas
    tracks_qf.get_sql()
    tracks_df = read_sql(sql=tracks_qf.sql, con=engine)

    test_df = merge(test_df, tracks_df, how="left", on=["TrackId"])

    assert joined_df.equals(test_df)


def test_join_2():

    playlist_track_qf = QFrame(dsn=dsn, db="sqlite", dialect="mysql").from_dict(deepcopy(playlist_track))
    playlists_qf = QFrame(dsn=dsn, db="sqlite", dialect="mysql").from_dict(deepcopy(playlists))

    joined_qf = join([playlist_track_qf, playlists_qf], join_type="cross join", on=0)

    sql = joined_qf.get_sql()

    testsql = """
            SELECT sq1."PlaylistId" AS "PlaylistId",
                sq1."TrackId" AS "TrackId",
                sq2."Name" AS "Name"
            FROM
            (SELECT "PlaylistId",
                    "TrackId"
            FROM PlaylistTrack) sq1
            CROSS JOIN
            (SELECT "PlaylistId",
                    "Name"
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
                SELECT sq1."PlaylistId" AS "PlaylistId",
                    sq1."TrackId" AS "TrackId",
                    sq1."Name" AS "Name"
                FROM
                (SELECT sq1."PlaylistId" AS "PlaylistId",
                        sq1."TrackId" AS "TrackId",
                        sq2."Name" AS "Name"
                FROM
                    (SELECT "PlaylistId",
                            "TrackId"
                    FROM PlaylistTrack) sq1
                CROSS JOIN
                    (SELECT "PlaylistId",
                            "Name"
                    FROM Playlist) sq2) sq1
                RIGHT JOIN
                (SELECT "PlaylistId",
                        "TrackId"
                FROM PlaylistTrack) sq2 ON sq1.PlaylistId=sq2.PlaylistId
                FULL JOIN
                (SELECT "PlaylistId",
                        "Name"
                FROM Playlist) sq3 ON sq2.PlaylistId=sq3.PlaylistId
            """

    assert clean_testexpr(sql) == clean_testexpr(testsql)


def test_union():
    playlists_qf = QFrame(dsn=dsn, db="sqlite", dialect="mysql").from_dict(deepcopy(playlists))

    unioned_qf = union([playlists_qf, playlists_qf], "union")

    testsql = """
            SELECT "PlaylistId",
                "Name"
            FROM Playlist
            UNION
            SELECT "PlaylistId",
                "Name"
            FROM Playlist
            """
    sql = unioned_qf.get_sql()

    assert clean_testexpr(sql) == clean_testexpr(testsql)
    assert unioned_qf.to_df().equals(playlists_qf.to_df())

    unioned_qf = union([playlists_qf, playlists_qf], "union all")

    testsql = """
            SELECT "PlaylistId",
                "Name"
            FROM Playlist
            UNION ALL
            SELECT "PlaylistId",
                "Name"
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
            SELECT "PlaylistId",
                "Name" AS "Old_name",
                Name || '_new' AS "New_name"
            FROM Playlist
            UNION
            SELECT "PlaylistId",
                Name || '_old' AS "Old_name",
                "Name" AS "New_name"
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
    q = QFrame(dsn=dsn, db="sqlite", dialect="mysql").from_json(json_path=json, subquery=sq)
    os.remove(json)

    testsql = """
        SELECT "customer",
            "billings"
        FROM test_schema.test_table
        """

    sql = q.get_sql()
    assert clean_testexpr(sql) == clean_testexpr(testsql)


def test_cut():
    qf = QFrame(dsn=dsn, db="sqlite", dialect="mysql").from_dict(deepcopy(playlists))
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


def test_from_table_sqlite():
    qf = QFrame(dsn=dsn, db="sqlite", dialect="mysql").from_table(table="Track")

    sql = """SELECT "TrackId",
                "Name",
                "AlbumId",
                "MediaTypeId",
                "GenreId",
                "Composer",
                "Milliseconds",
                "Bytes",
                "UnitPrice"
            FROM Track"""

    assert clean_testexpr(sql) == clean_testexpr(qf.get_sql())


def test_from_table_sqlite_json():
    QFrame(dsn=dsn, db="sqlite", dialect="mysql").from_table(table="Playlist", json_path="test.json", subquery="q1")
    QFrame(dsn=dsn, db="sqlite", dialect="mysql").from_table(
        table="PlaylistTrack", json_path="test.json", subquery="q2"
    )

    qf1 = QFrame(dsn=dsn, db="sqlite", dialect="mysql").from_json(json_path="test.json", subquery="q1")
    sql = """SELECT "PlaylistId",
                "Name"
            FROM Playlist"""
    assert clean_testexpr(sql) == clean_testexpr(qf1.get_sql())

    qf2 = QFrame(dsn=dsn, db="sqlite", dialect="mysql").from_json(json_path="test.json", subquery="q2")
    sql = """SELECT "PlaylistId",
                "TrackId"
            FROM PlaylistTrack"""
    assert clean_testexpr(sql) == clean_testexpr(qf2.get_sql())
    os.remove("test.json")


def test_from_table_rds():
    engine_str = "mssql+pyodbc://redshift_acoe"
    qf = QFrame(engine=engine_str, db="redshift", interface="pyodbc")
    qf = qf.from_table(table="table_tutorial", schema="grizly")

    sql = """SELECT "col1",
               "col2",
               "col3",
               "col4"
        FROM grizly.table_tutorial"""

    assert clean_testexpr(sql) == clean_testexpr(qf.get_sql())

    dtypes = ["CHARACTER VARYING(500)", "DOUBLE PRECISION", "CHARACTER VARYING(500)", "DOUBLE PRECISION"]

    assert dtypes == qf.get_dtypes()


def test_pivot_rds():
    engine_str = "mssql+pyodbc://redshift_acoe"
    qf = QFrame(engine=engine_str, db="redshift", interface="pyodbc")
    qf = qf.from_table(table="table_tutorial", schema="grizly")

    with pytest.raises(ValueError, match=f"'my_value' not found in fields."):
        qf.pivot(rows=["col1"], columns=["col2", "col3"], values="my_value")

    # sorted
    qf1 = qf.copy()
    qf1.pivot(rows=["col1"], columns=["col2", "col3"], values="col4", prefix="p_", sort=True)

    sql = """SELECT sq."col1" AS "col1",
                sum(CASE
                    WHEN "col2"='0.0'
                            AND "col3" IS NULL THEN "col4"
                    ELSE 0
                END) AS "p_0.0_None",
                sum(CASE
                        WHEN "col2"='1.3'
                                AND "col3" IS NULL THEN "col4"
                        ELSE 0
                    END) AS "p_1.3_None"
            FROM
            (SELECT "col1",
                    "col2",
                    "col3",
                    "col4"
            FROM grizly.table_tutorial) sq
            GROUP BY 1"""

    assert clean_testexpr(sql) == clean_testexpr(qf1.get_sql())


def test_join_pivot_sqlite():
    playlist_track_qf = QFrame(dsn=dsn, db="sqlite", dialect="mysql").from_dict(deepcopy(playlist_track))
    playlists_qf = QFrame(dsn=dsn, db="sqlite", dialect="mysql").from_dict(deepcopy(playlists))
    tracks_qf = QFrame(dsn=dsn, db="sqlite", dialect="mysql").from_dict(deepcopy(tracks))

    joined_qf = join(
        qframes=[playlist_track_qf, playlists_qf, tracks_qf],
        join_type=["left join", "left join"],
        on=["sq1.PlaylistId=sq2.PlaylistId", "sq1.TrackId=sq3.TrackId"],
        unique_col=True,
    )

    joined_qf = joined_qf.window(offset=3500, limit=100, order_by=["PlaylistId", "TrackId"])

    qf1 = joined_qf.copy()
    qf1.pivot(rows=["GenreId", "Composer"], columns=["Name", "PlaylistId"], values="UnitPrice", aggtype="sum")

    qf2 = joined_qf.copy()
    qf2.pivot(
        rows=["sq3.GenreId", "sq3.Composer"],
        columns=["sq2.Name", "sq1.PlaylistId"],
        values="sq3.UnitPrice",
        aggtype="sum",
    )

    qf3 = joined_qf.copy()
    qf3.pivot(rows=["sq3.GenreId", "Composer"], columns=["Name", "sq1.PlaylistId"], values="UnitPrice", aggtype="sum")

    assert clean_testexpr(qf1.get_sql()) == clean_testexpr(qf2.get_sql())
    assert clean_testexpr(qf2.get_sql()) == clean_testexpr(qf3.get_sql())

    # qf1.rename(
    #     {"sq.GenreId": "Group 2.0*", "Composer": "Group 2.0*", "90’s Music_5": "Measure1", "TV Shows_3": "Measure2"}
    # )
    qf11 = qf1.copy()

    qf11.select(["90’s Music_5"])

    sql = """SELECT sq."90’s Music_5" AS "90’s_Music_5"
            FROM
            (SELECT sq."GenreId" AS "GenreId",
                    sq."Composer" AS "Composer",
                    sum(CASE
                            WHEN "Name"='90’s Music'
                                AND "PlaylistId"='5' THEN "UnitPrice"
                            ELSE 0
                        END) AS "90’s_Music_5",
                    sum(CASE
                            WHEN "Name"='TV Shows'
                                AND "PlaylistId"='3' THEN "UnitPrice"
                            ELSE 0
                        END) AS "TV_Shows_3"
            FROM
                (SELECT sq1."PlaylistId" AS "PlaylistId",
                        sq1."TrackId" AS "TrackId",
                        sq2."Name" AS "Name",
                        sq3."AlbumId" AS "AlbumId",
                        sq3."MediaTypeId" AS "MediaTypeId",
                        sq3."GenreId" AS "GenreId",
                        sq3."Composer" AS "Composer",
                        sq3."Milliseconds" AS "Milliseconds",
                        sq3."Bytes" AS "Bytes",
                        sq3."UnitPrice" AS "UnitPrice"
                FROM
                    (SELECT "PlaylistId",
                            "TrackId"
                    FROM PlaylistTrack) sq1
                LEFT JOIN
                    (SELECT "PlaylistId",
                            "Name"
                    FROM Playlist) sq2 ON sq1.PlaylistId=sq2.PlaylistId
                LEFT JOIN
                    (SELECT "TrackId",
                            "Name",
                            "AlbumId",
                            "MediaTypeId",
                            "GenreId",
                            "Composer",
                            "Milliseconds",
                            "Bytes",
                            "UnitPrice"
                    FROM Track) sq3 ON sq1.TrackId=sq3.TrackId
                ORDER BY 1,
                        2
                LIMIT 100
                OFFSET 3500) sq
            GROUP BY 1,
                        2) sq
    """
    # write_out(qf11.get_sql())
    assert clean_testexpr(qf11.get_sql()) == clean_testexpr(sql)