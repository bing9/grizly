import os
from filecmp import cmp
from sys import platform

from ..grizly.utils import set_cwd, get_path, file_extension


def write_out(out):
    with open(get_path("output.sql", from_where="here"), "w",) as f:
        f.write(out)


def test_set_cwd():
    cwd = set_cwd("test")
    if platform.startswith("linux"):
        user_cwd = os.getenv("HOME")
    else:
        user_cwd = os.getenv("USERPROFILE")
    user_cwd = os.path.join(user_cwd, "test")
    assert cwd == user_cwd


def test_file_extention():
    file_path = get_path("test.csv")
    assert file_extension(file_path) == "csv"
    assert file_extension(get_path()) == ""
