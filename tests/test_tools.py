from pandas import DataFrame
import os
from filecmp import cmp
<<<<<<< HEAD:tests/test_tools.py
from grizly import extract
from tests import config
=======
from grizly import (
    AWS
)
# from grizly import extract
# from grizly.tests import config
>>>>>>> 573270c19ec87fc95b34ddc708203916354e4f43:grizly/tests/test_tools.py

def write_out(out):
    with open(
        config.tests_txt_file,
        "w",
    ) as f:
        f.write(out)

def test_df_to_s3_and_s3_to_file():
    aws = AWS(file_name='testing_aws_class.csv')
    df = DataFrame({'col1': [1, 2], 'col2': [3, 4]})
    aws.df_to_s3(df)
   
    first_file_path = os.path.join(aws.file_dir, aws.file_name)
    second_file_path = os.path.join(aws.file_dir, 'testing_aws_class_1.csv')

    os.rename(first_file_path, second_file_path)
    aws.s3_to_file()

    assert cmp(first_file_path, second_file_path) == True
    os.remove(first_file_path)
    os.remove(second_file_path)

<<<<<<< HEAD:tests/test_tools.py
def test_csv_from_sql():
    csv = extract.Csv(csv_path=config.csv_path).from_sql(table="artist", engine_str=config.engine_str, chunksize=100)
    write_out(csv.deletethis)
=======
# def test_csv_from_sql():
#     csv = extract.Csv(csv_path=config.csv_path).from_sql(table="artist", engine_str=config.engine_str, chunksize=100)
#     write_out(csv.deletethis)
>>>>>>> 573270c19ec87fc95b34ddc708203916354e4f43:grizly/tests/test_tools.py