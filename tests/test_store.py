from ..grizly.store import Store
from ..grizly.config import config


def test_store_from_json_local():
    path = "store.json"
    store = Store.from_json(path)
    assert store.partition_cols == ["Composer"]


def test_store_from_json_s3():
    bucket = config.get_service("s3").get("bucket")
    url = f"s3://{bucket}/test/store.json"
    store = Store.from_json(url)
    assert store.partition_cols == ["Composer"]
