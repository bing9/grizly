import toml
import pytest

from ..grizly.tools.email import Email, EmailAccount
from ..grizly.config import config as grizly_config

settings = toml.load("settings.toml")
email_settings = settings.get("email")
address = email_settings.get("address")
password = email_settings.get("password")


def test_email_account():
    EmailAccount(address, password)


def test_email_account_defaults():
    grizly_config_address = grizly_config.get_service("email").get("address")
    grizly_config_password = grizly_config.get_service("email").get("password")

    if grizly_config_address is not None and grizly_config_password is not None:
        EmailAccount()


def test_email_account_wrong_creds():
    with pytest.raises(ConnectionError):
        EmailAccount("wrong_email", "wrong_password")


# TODO: add tests for Email
