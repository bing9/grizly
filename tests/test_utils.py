import os
from sys import platform
from typing import TypeVar

from ..grizly.utils.functions import file_extension, get_path, isinstance2, set_cwd


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


def test_isinstance2():
    class ParentClass:
        pass

    class MyClass(ParentClass):
        pass

    TestTypeVarOther = TypeVar("OtherType")  # type: ignore
    TestTypeClass = TypeVar("MyClass")  # type: ignore
    TestTypeParent = TypeVar("ParentClass")  # type: ignore

    my_class_instance = MyClass()
    assert isinstance2(my_class_instance, MyClass) is True
    assert isinstance2(my_class_instance, ParentClass) is True
    assert isinstance2(my_class_instance, TestTypeVarOther) is False
    assert isinstance2(my_class_instance, TestTypeClass) is True
    assert isinstance2(my_class_instance, TestTypeParent) is True
