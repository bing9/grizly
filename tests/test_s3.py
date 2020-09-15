import os
import re
from time import sleep

from pandas import DataFrame, read_csv
import pytest
from hypothesis import given
from hypothesis.strategies import integers, text, lists

from ..grizly.tools.qframe import QFrame
from ..grizly.tools.s3 import S3
from ..grizly.tools.sqldb import SQLDB
from ..grizly.utils import get_path
from ..grizly.config import config


def test_df_to_s3_and_s3_to_file():
    s3 = S3(file_name="testing_s3_class.csv", s3_key="bulk/")
    df = DataFrame({"col1": [1, 2], "col2": [3, 4]})
    s3.from_df(df, sep="\t")

    file_path = os.path.join(s3.file_dir, s3.file_name)

    s3.to_file()

    assert df.equals(read_csv(file_path, sep="\t"))
    os.remove(file_path)


def test_can_upload():
    s3 = S3(file_name="test_s3_2.csv", s3_key="bulk/tests/", min_time_window=3)
    df = DataFrame({"col1": [1, 2], "col2": [3, 4]})
    s3.from_df(df)

    assert not s3._can_upload()
    sleep(3)
    assert s3._can_upload()


def test_to_rds():
    import os
    print(os.environ)

    dsn = get_path("grizly_dev", "tests", "Chinook.sqlite")
    qf = QFrame(dsn=dsn, db="sqlite", dialect="mysql").from_table(table="Track")

    qf.window(offset=100, limit=30, order_by=["TrackId"])

    qf.assign(LikeIt="CASE WHEN GenreId = 5 THEN 1 ELSE 0 END", custom_type="BOOL")
    qf.assign(SpareColumn="NULL")

    qf.rename(
        {
            field: "_".join(re.findall("[A-Z][^A-Z]*", alias)).lower()
            for field, alias in zip(qf.get_fields(aliased=False), qf.get_fields(aliased=True))
        }
    )

    s3_key = "test/"
    bucket = "acoe-s3"
    table_parquet = "grizly_test_parquet"
    table_csv = "grizly_test_csv"
    schema = "sandbox"
    path_csv = get_path("grizly_test.csv")
    path_parquet = get_path("grizly_test.parquet")

    s3_parquet = S3(
        file_name=os.path.basename(path_parquet),
        file_dir=os.path.dirname(path_parquet),
        s3_key=s3_key,
        bucket=bucket,
    )
    s3_csv = S3(
        file_name=os.path.basename(path_csv),
        file_dir=os.path.dirname(path_csv),
        s3_key=s3_key,
        bucket=bucket,
    )

    qf.to_parquet(path_parquet)
    s3_parquet.from_file(keep_file=False)
    s3_parquet.to_rds(table=table_parquet, schema=schema, if_exists="replace")
    qf_parquet = QFrame(dsn="redshift_acoe").from_table(table=table_parquet, schema=schema)
    assert len(qf_parquet) == 30

    qf.to_csv(path_csv)
    s3_csv.from_file(keep_file=False)
    s3_csv.to_rds(table=table_csv, schema=schema, if_exists="replace")
    qf_csv = QFrame(dsn="redshift_acoe").from_table(table=table_csv, schema=schema)
    assert len(qf_csv) == 30

    qf.to_parquet(path_parquet)
    s3_parquet.from_file(keep_file=False)
    s3_parquet.to_rds(table=table_parquet, schema=schema, if_exists="append")
    assert len(qf_parquet) == 60

    qf.rearrange(
        [
            "composer",
            "milliseconds",
            "bytes",
            "unit_price",
            "like_it",
            "spare_column",
            "track_id",
            "name",
            "album_id",
            "media_type_id",
            "genre_id",
        ]
    )
    qf.to_csv(path_csv)
    s3_csv.from_file(keep_file=False)
    s3_csv.to_rds(table=table_csv, schema=schema, if_exists="append")
    assert len(qf_csv) == 60

    qf_csv.distinct()
    assert len(qf_csv) == 30

    sqldb = SQLDB(dsn="redshift_acoe")
    sqldb.drop_table(table=table_parquet, schema=schema)
    sqldb.drop_table(table=table_csv, schema=schema)


def test_to_aurora():

    dsn = get_path("grizly_dev", "tests", "Chinook.sqlite")
    qf = QFrame(dsn=dsn, db="sqlite", dialect="mysql").from_table(table="Track")

    qf.window(offset=100, limit=30, order_by=["TrackId"])

    qf.assign(LikeIt="CASE WHEN GenreId = 5 THEN 1 ELSE 0 END", custom_type="BOOL")
    qf.assign(SpareColumn="NULL")

    qf.rename(
        {
            field: "_".join(re.findall("[A-Z][^A-Z]*", alias)).lower()
            for field, alias in zip(qf.get_fields(aliased=False), qf.get_fields(aliased=True))
        }
    )

    s3_key = "test/"
    bucket = "acoe-s3"
    table_csv = "grizly_test_csv"
    schema = "sandbox"
    path_csv = get_path("grizly_test.csv")

    s3_csv = S3(
        file_name=os.path.basename(path_csv),
        file_dir=os.path.dirname(path_csv),
        s3_key=s3_key,
        bucket=bucket,
    )

    qf.to_csv(path_csv)
    s3_csv.from_file(keep_file=False)
    s3_csv.to_aurora(table=table_csv, schema=schema, dsn="aurora_db", if_exists="replace")
    qf_csv = QFrame(dsn="aurora_db").from_table(table=table_csv, schema=schema)
    assert len(qf_csv) == 30

    qf.rearrange(
        [
            "composer",
            "milliseconds",
            "bytes",
            "unit_price",
            "like_it",
            "spare_column",
            "track_id",
            "name",
            "album_id",
            "media_type_id",
            "genre_id",
        ]
    )
    qf.to_csv(path_csv)
    s3_csv.from_file(keep_file=False)
    s3_csv.to_aurora(table=table_csv, schema=schema, dsn="aurora_db", if_exists="append")
    assert len(qf_csv) == 60

    qf_csv.distinct()
    assert len(qf_csv) == 30

    SQLDB(dsn="aurora_db").drop_table(table=table_csv, schema=schema)


def test_to_serializable():
    s3 = S3(bucket="acoe-s3", s3_key="test/", file_name="test_s3.json")
    serializable = s3.to_serializable()
    assert serializable == {"a": 42}


# This will fail because of DataFrame replacing empty strings with NaN values
#  (lists(text() will generate a list of empty strings) - Michal
# @pytest.mark.parametrize("ext", ["csv", "parquet", "xlsx"])
# @given(col1=lists(text(), min_size=3, max_size=3), col2=lists(integers(), min_size=3, max_size=3))
# def test_from_df_to_df(col1, col2, ext):
#     d = {"col1": col1, "col2": col2}
#     df = DataFrame(data=d)
#     s3 = S3(f"test.{ext}", s3_key="grizly/")
#     s3.from_df(df)
#     test_df = s3.to_df()
#     assert test_df.equals(df)
