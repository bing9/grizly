from pandas import DataFrame
import os
from filecmp import cmp
from ..grizly.tools.s3 import (
    S3
)

def test_df_to_s3_and_s3_to_file():
    s3 = S3(file_name='testing_aws_class.csv', s3_key='bulk/')
    df = DataFrame({'col1': [1, 2], 'col2': [3, 4]})
    s3.from_df(df)

    first_file_path = os.path.join(s3.file_dir, s3.file_name)
    second_file_path = os.path.join(s3.file_dir, 'testing_aws_class_1.csv')

    os.rename(first_file_path, second_file_path)
    print(os.path.join(s3.file_dir, s3.file_name))
    s3.to_file()

    assert cmp(first_file_path, second_file_path) == True
    os.remove(first_file_path)
    os.remove(second_file_path)
