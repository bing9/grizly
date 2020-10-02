from simple_salesforce import Salesforce
from simple_salesforce.login import SalesforceAuthenticationFailed
import logging
from logging import Logger

from ..config import config as default_config, Config

import os


class SFDB:
    def __init__(
        self,
        config: Config = None,
        username: str = "",
        password: str = "",
        organization_id: str = "",
        env: str = "prod",
        proxies: dict = None,
        logger: Logger = None,
    ):
        self.env = env
        self.logger = logger or logging.getLogger(__name__)
        if not (config or (username and password)):
            config = default_config
        if config:
            self._load_attrs_from_config(config)
        else:
            self.proxies = proxies or {
                "http": os.getenv("HTTP_PROXY"),
                "https": os.getenv("HTTPS_PROXY"),
            }

    def _load_attrs_from_config(self, config):
        sfdc_config = config.get_service("sfdc")[self.env]
        self.username = sfdc_config.get("username")
        self.password = sfdc_config.get("password")
        self.organization_id = sfdc_config.get("organization_id")

    @property
    def con(self):
        try:
            con = Salesforce(
                password=self.password,
                username=self.username,
                organizationId=self.organization_id,
                proxies=self.proxies,
            )
        except SalesforceAuthenticationFailed:
            self.logger.info(
                "Could not log in to SFDC. Are you sure your password hasn't expired and your proxy is set up correctly?"
            )
            raise SalesforceAuthenticationFailed
        return con
