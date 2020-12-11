from pandas import concat, merge, read_sql
import pytest

from ..conftest import clean_testexpr, write_out
from ..grizly.drivers.frames_factory import QFrame
from ..grizly.drivers.sql import join, union


def test_create_sql_blocks(orders_qf):
    assert orders_qf._build_column_strings()["select_names"] == [
        '"Order" as "Bookings"',
        '"Part" as "Part1"',
        '"Customer"',
        '"Value"',
    ]
    assert orders_qf._build_column_strings()["select_aliases"] == [
        "Bookings",
        "Part1",
        "Customer",
        "Value",
    ]
    assert (
        orders_qf._create_sql_blocks().data["select"]["sql_blocks"]
        == orders_qf._build_column_strings()
    )


def test_create_external_table():
    # TODO
    pass


def test_create_table():
    # TODO
    pass


def test_from_table_sqlite(track_qf):
    qf = track_qf

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


def test_from_table_rds(table_tutorial_qf):
    qf = table_tutorial_qf

    sql = """SELECT "col1",
               "col2",
               "col3",
               "col4"
        FROM grizly.table_tutorial"""

    assert clean_testexpr(sql) == clean_testexpr(qf.get_sql())

    dtypes = [
        "character varying(500)",
        "double precision",
        "character varying(500)",
        "double precision",
    ]

    assert dtypes == qf.get_dtypes()


def test_get_sql(orders_qf):
    orders_qf.assign(New_case="CASE WHEN Bookings = 100 THEN 1 ELSE 0 END", dtype="FLOAT(53)")
    orders_qf.limit(5)
    orders_qf.groupby(orders_qf.data["select"]["fields"])["Value"].agg("sum")
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
                        "HiddenColumn",
                        5
                LIMIT 5
            """
    sql = orders_qf.get_sql()
    # write_out(str(sql))
    assert clean_testexpr(sql) == clean_testexpr(testsql)


def test_pivot_rds_1(table_tutorial_qf):
    qf = table_tutorial_qf

    with pytest.raises(ValueError, match="'my_value' not found in fields."):
        qf.pivot(rows=["col1"], columns=["col2", "col3"], values="my_value")

    qf1 = qf.copy()
    with pytest.warns(UserWarning):
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


def test_pivot_rds_2(table_tutorial_qf):
    qf = table_tutorial_qf
    qf.assign(flag="CAST((CASE WHEN col2 = 0 THEN 1 ELSE 0 END) AS CHAR(1))", dtype="CHAR(1)")

    # col2 has only two values (1.3 and 0.0)
    # so pivot with sort=True should sort records to put first 0 then 1

    # sorted
    qf1 = qf.copy()
    qf1.pivot(rows=["col1"], columns=["flag"], values="col4", sort=True)

    sql = """SELECT sq."col1" AS "col1",
                sum(CASE
                        WHEN "flag"='0' THEN "col4"
                        ELSE 0
                    END) AS "0",
                sum(CASE
                        WHEN "flag"='1' THEN "col4"
                        ELSE 0
                    END) AS "1"
            FROM
            (SELECT "col1",
                    "col2",
                    "col3",
                    "col4",
                    CAST((CASE
                                WHEN col2 = 0 THEN 1
                                ELSE 0
                            END) AS CHAR(1)) AS "flag"
            FROM grizly.table_tutorial) sq
            GROUP BY 1"""

    assert clean_testexpr(sql) == clean_testexpr(qf1.get_sql())


def test_join_pivot_sqlite(playlist_track_qf, playlist_qf, track_qf):
    joined_qf = join(
        qframes=[playlist_track_qf, playlist_qf, track_qf],
        join_type=["left join", "left join"],
        on=["sq1.PlaylistId=sq2.PlaylistId", "sq1.TrackId=sq3.TrackId"],
        unique_col=True,
    )

    joined_qf = joined_qf.window(offset=3500, limit=100, order_by=["PlaylistId", "TrackId"])

    qf1 = joined_qf.copy()
    with pytest.warns(UserWarning):
        qf1.pivot(
            rows=["GenreId", "Composer"],
            columns=["Name", "PlaylistId"],
            values="UnitPrice",
            aggtype="sum",
        )

    qf2 = joined_qf.copy()
    with pytest.warns(UserWarning):
        qf2.pivot(
            rows=["sq3.GenreId", "sq3.Composer"],
            columns=["sq2.Name", "sq1.PlaylistId"],
            values="sq3.UnitPrice",
            aggtype="sum",
        )

    qf3 = joined_qf.copy()
    with pytest.warns(UserWarning):
        qf3.pivot(
            rows=["sq3.GenreId", "Composer"],
            columns=["Name", "sq1.PlaylistId"],
            values="UnitPrice",
            aggtype="sum",
        )

    assert clean_testexpr(qf1.get_sql()) == clean_testexpr(qf2.get_sql())
    assert clean_testexpr(qf2.get_sql()) == clean_testexpr(qf3.get_sql())

    # qf1.rename(
    #     {"sq.GenreId": "Group 2.0*", "Composer": "Group 2.0*", "90’s Music_5": "Measure1", "TV Shows_3": "Measure2"}
    # )
    qf11 = qf1.copy()

    qf11.select(["90’s Music_5"])

    sql = """SELECT sq."90’s Music_5" AS "90’s Music_5"
            FROM
            (SELECT sq."GenreId" AS "GenreId",
                    sq."Composer" AS "Composer",
                    sum(CASE
                            WHEN "Name"='90’s Music'
                                AND "PlaylistId"='5' THEN "UnitPrice"
                            ELSE 0
                        END) AS "90’s Music_5",
                    sum(CASE
                            WHEN "Name"='TV Shows'
                                AND "PlaylistId"='3' THEN "UnitPrice"
                            ELSE 0
                        END) AS "TV Shows_3"
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
    assert not qf11.to_df().empty


def test_sql():
    # TODO
    pass


def test_to_table(track_qf):
    qf_in = track_qf
    qf_in.limit(10)

    out_table = "test_to_table"

    # if_exists PARAMETER
    # -------------------
    qf_in.to_table(table=out_table, if_exists="drop")

    qf_out = QFrame(source=qf_in.source, table=out_table)
    assert qf_out.nrows == 10

    with pytest.raises(Exception):
        qf_in.to_table(table=out_table)

    qf_in.limit(5)
    qf_in.to_table(table=out_table, if_exists="replace")
    assert qf_out.nrows == 5

    qf_in.limit(10)
    qf_in.to_table(table=out_table, if_exists="append")
    assert qf_out.nrows == 15

    qf_in.rename({"Name": "name_2"})
    qf_in.to_table(table=out_table, if_exists="drop")
    qf_out = QFrame(source=qf_in.source, table=out_table)
    assert qf_out.nrows == 10
    assert "name_2" in qf_out.get_fields()

    qf_out.source.drop_table(table=out_table)


def test_join_1(playlist_track_qf, playlist_qf, track_qf):
    # using grizly

    joined_qf = join(
        [playlist_track_qf, playlist_qf], join_type="left join", on="sq1.PlaylistId=sq2.PlaylistId",
    )
    joined_df = joined_qf.to_df()

    con = playlist_track_qf.source.get_connection()

    playlist_track_qf.get_sql()
    pl_track_df = read_sql(sql=playlist_track_qf.get_sql(), con=con)

    playlist_qf.get_sql()
    pl_df = read_sql(sql=playlist_qf.get_sql(), con=con)

    test_df = merge(pl_track_df, pl_df, how="left", on=["PlaylistId"])

    assert joined_df.equals(test_df)

    # using grizly
    with pytest.warns(UserWarning):
        joined_qf = join(
            qframes=[playlist_track_qf, playlist_qf, track_qf],
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
    track_qf.get_sql()
    tracks_df = read_sql(sql=track_qf.get_sql(), con=con)

    test_df = merge(test_df, tracks_df, how="left", on=["TrackId"])

    assert joined_df.equals(test_df)


def test_join_2(playlist_track_qf, playlist_qf):
    joined_qf = join([playlist_track_qf, playlist_qf], join_type="cross join", on=0)

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
        [joined_qf, playlist_track_qf, playlist_qf],
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


def test_union(playlist_qf):
    unioned_qf = union([playlist_qf, playlist_qf], "UNION")

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
    assert unioned_qf.to_df().equals(playlist_qf.to_df())

    unioned_qf = union([playlist_qf, playlist_qf], "UNION ALL")

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
    assert unioned_qf.to_df().equals(
        concat([playlist_qf.to_df(), playlist_qf.to_df()], ignore_index=True)
    )

    qf1 = playlist_qf.copy()
    qf1.rename({"Name": "Old_name"})
    qf1.assign(New_name="Name || '_new'", dtype="NVARCHAR(120)")

    qf2 = playlist_qf.copy()
    qf2.rename({"Name": "New_name"})
    qf2.assign(Old_name="Name || '_old'", dtype="NVARCHAR(120)")

    unioned_qf = union([qf1, qf2], union_type="UNION", union_by="name")

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

