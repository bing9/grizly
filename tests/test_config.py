from ..grizly.config import Config
import os
from pathlib import Path
from copy import deepcopy


def test_from_json():
    json_path = str(Path.cwd().parent.joinpath("tutorials", "resources", "config.json"))

    old_data = deepcopy(Config.data)
    Config.data = {}
    Config().from_json(json_path=json_path)

    data = {
        "standard": {
            "proxies": {"http": "first_proxy", "https": "second_proxy"},
            "email": {
                "email_address": "my_email@example.com",
                "email_password": "my_password",
                "send_as": "Team",
            },
            "github": {"username": "my_login", "pages": 100, "username_password": "my_password"},
            "sfdc": {
                "stage": {
                    "username": "my_login",
                    "instance_url": "https://na1.salesforce.com",
                    "password": "my_password",
                    "organizationId": "OrgId",
                },
                "prod": {
                    "username": "my_login",
                    "password": "my_password",
                    "organizationId": "OrgId",
                },
            },
            "sources": {
                "redshift_acoe": {"db": "redshift", "dialect": "postgresql"},
                "DenodoPROD": {"db": "denodo", "dialect": "denodo"},
                "aurora_db": {"db": "aurora", "dialect": "postgresql"},
            },
        }
    }

    assert Config.data == data

    Config.data = old_data


def test_env():
    old_data = deepcopy(Config.data)
    old_config_file_path = os.environ["GRIZLY_CONFIG_FILE"]

    json_path = str(Path.cwd().parent.joinpath("tutorials", "resources", "config.json"))
    os.environ["GRIZLY_CONFIG_FILE"] = json_path

    Config.data = {}
    data = Config().get_service(service="proxies")
    assert data == {"http": "first_proxy", "https": "second_proxy"}

    if old_config_file_path is None:
        del os.environ["GRIZLY_CONFIG_FILE"]
    else:
        os.environ["GRIZLY_CONFIG_FILE"] = old_config_file_path
    Config.data = old_data
