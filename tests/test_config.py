from ..grizly.config import Config
from ..grizly.utils import get_path
import os


data = {
    "config": {
        "standard": {
            "email": {"email_address": "my_email@example.com", "email_password": "my_password", "send_as": "Team"},
            "github": {
                "username": "my_login",
                "username_password": "my_password",
                "pages": 100,
                "proxies": {
                    "http": "http://restrictedproxy.tycoelectronics.com:80",
                    "https": "https://restrictedproxy.tycoelectronics.com:80",
                },
            },
            "sfdc": {
                "stage": {
                    "username": "my_login",
                    "instance_url": "https://na1.salesforce.com",
                    "password": "my_password",
                    "organizationId": "OrgId",
                },
                "prod": {"username": "my_login", "password": "my_password", "organizationId": "OrgId"},
            },
        }
    }
}


def test_from_dict():
    Config().from_dict(data)
    assert Config.data == data["config"]


def test_from_json():
    json_path = get_path("grizly_dev", "notebooks", "config.json")

    Config.data = {}
    Config().from_json(json_path=json_path)

    data = {
        "standard": {
            "proxies": {"http": "first_proxy", "https": "second_proxy"},
            "email": {"email_address": "my_email@example.com", "email_password": "my_password", "send_as": "Team"},
            "github": {
                "username": "my_login",
                "proxies": {"http": "first_proxy", "https": "second_proxy"},
                "pages": 100,
                "username_password": "my_password",
            },
            "sfdc": {
                "stage": {
                    "username": "my_login",
                    "instance_url": "https://na1.salesforce.com",
                    "password": "my_password",
                    "organizationId": "OrgId",
                },
                "prod": {"username": "my_login", "password": "my_password", "organizationId": "OrgId"},
            },
        }
    }

    assert Config.data == data


def test_env():
    json_path = get_path("grizly_dev", "notebooks", "config.json")
    os.environ["GRIZLY_CONFIG_FILE"] = json_path

    Config.data = {}
    data = Config().get_service(service="proxies")
    assert data == {"http": "first_proxy", "https": "second_proxy"}
