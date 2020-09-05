from ..grizly.tools.dialects import check_if_valid_type


def test_check_if_valid_type():
    assert check_if_valid_type("INT")
    assert not check_if_valid_type("string")
    assert check_if_valid_type("varchar(30)")
