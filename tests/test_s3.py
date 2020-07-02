from pandas import DataFrame
import os
from time import sleep
from filecmp import cmp
import re
from ..grizly.tools.s3 import S3
from ..grizly.utils import get_path
from ..grizly.tools.qframe import QFrame
from ..grizly.tools.sqldb import SQLDB


def test_df_to_s3_and_s3_to_file():
    s3 = S3(file_name="testing_aws_class.csv", s3_key="bulk/")
    df = DataFrame({"col1": [1, 2], "col2": [3, 4]})
    s3.from_df(df)

    first_file_path = os.path.join(s3.file_dir, s3.file_name)
    second_file_path = os.path.join(s3.file_dir, "testing_aws_class_1.csv")

    os.rename(first_file_path, second_file_path)
    print(os.path.join(s3.file_dir, s3.file_name))
    s3.to_file()

    assert cmp(first_file_path, second_file_path) == True
    os.remove(first_file_path)
    os.remove(second_file_path)


def test_can_upload():
    s3 = S3(file_name="test_s3_2.csv", s3_key="bulk/tests/", min_time_window=3)
    df = DataFrame({"col1": [1, 2], "col2": [3, 4]})
    s3.from_df(df)

    assert not s3._can_upload()
    sleep(3)
    assert s3._can_upload()


def test_to_rds():

    engine_string = "sqlite:///" + get_path("grizly_dev", "tests", "Chinook.sqlite")
    qf = QFrame(engine=engine_string, db="sqlite").from_table(table="Track")

    qf.window(offset=100, limit=30, order_by=["TrackId"])

    qf.assign(LikeIt="CASE WHEN GenreId = 5 THEN 1 ELSE 0 END", custom_type="BOOL")
    qf.assign(SpareColumn="NULL")
    qf.rename({field: "_".join(re.findall('[A-Z][^A-Z]*', alias)).lower() for field, alias in zip(qf.get_fields(aliased=False), qf.get_fields(aliased=True))})

    s3_key = "test/"
    bucket = "acoe-s3"
    table_parquet = "grizly_test_parquet"
    table_csv = "grizly_test_csv"
    schema = "sandbox"
    path_csv = get_path("grizly_test.csv")
    path_parquet = get_path("grizly_test.parquet")

    s3_parquet = S3(file_name=os.path.basename(path_parquet), file_dir=os.path.dirname(path_parquet), s3_key=s3_key, bucket=bucket)
    s3_csv = S3(file_name=os.path.basename(path_csv), file_dir=os.path.dirname(path_csv), s3_key=s3_key, bucket=bucket)

    qf.to_parquet(path_parquet)
    s3_parquet.from_file(keep_file=False)
    s3_parquet.to_rds(table=table_parquet, schema=schema)
    qf_parquet = QFrame(engine="mssql+pyodbc://redshift_acoe", interface="pyodbc").from_table(table=table_parquet, schema=schema)
    assert len(qf_parquet) == 30

    qf.to_csv(path_csv)
    s3_csv.from_file(keep_file=False)
    s3_csv.to_rds(table=table_csv, schema=schema)
    qf_csv = QFrame(engine="mssql+pyodbc://redshift_acoe", interface="pyodbc").from_table(table=table_csv, schema=schema)
    assert len(qf_csv) == 30

    qf.to_parquet(path_parquet)
    s3_parquet.from_file(keep_file=False)
    s3_parquet.to_rds(table=table_parquet, schema=schema, if_exists="append")
    assert len(qf_parquet) == 60

    qf.rearrange(['composer', 'milliseconds', 'bytes', 'unit_price', 'like_it', 'spare_column', 'track_id', 'name', 'album_id', 'media_type_id', 'genre_id'])
    qf.to_csv(path_csv)
    s3_csv.from_file(keep_file=False)
    s3_csv.to_rds(table=table_csv, schema=schema, if_exists="append")
    assert len(qf_csv) == 60

    qf_csv.distinct()
    assert len(qf_csv) == 30

    sqldb = SQLDB(db="redshift", engine_str="mssql+pyodbc://redshift_acoe", interface="pyodbc")
    sqldb.drop_table(table=table_parquet, schema=schema)
    sqldb.drop_table(table=table_csv, schema=schema)

def test_to_dict():
    s3 = S3(bucket="acoe-s3", s3_key="test/", file_name="test_s3.json")
    dict = s3.to_dict()
    assert dict == {"a": 42}
