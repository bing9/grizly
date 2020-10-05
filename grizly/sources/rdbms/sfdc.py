import logging
import os
from logging import Logger

from grizly.config import Config
from grizly.config import config as default_config
from grizly.sources.base import BaseSource
from ...type_mappers import sfdc_to_sqlalchemy_dtype
from simple_salesforce import Salesforce
from simple_salesforce.login import SalesforceAuthenticationFailed


class SFDB(BaseSource):
    def __init__(
        self,
        config: Config = None,
        username: str = "",
        password: str = "",
        organization_id: str = "",
        proxies: dict = None,
        logger: Logger = None,
    ):
        self.logger = logger or logging.getLogger(__name__)
        if username and password and organization_id and proxies:
            self.username = username
            self.password = password
            self.organization_id = organization_id
            self.proxies = proxies
        else:
            if not config:
                config = default_config
            self._load_attrs_from_config(config)

    def _load_attrs_from_config(self, config):
        sfdc_config = config.get_service("sfdc")
        self.username = sfdc_config.get("username")
        self.password = sfdc_config.get("password")
        self.organization_id = sfdc_config.get("organizationId")
        self.proxies = sfdc_config.get("proxies") or {
            "http": os.getenv("HTTP_PROXY"),
            "https": os.getenv("HTTPS_PROXY"),
        }

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

    def get_fields(self, table, columns=None, column_types=True):
        """Get column names (and optionally types) from a SFDC table.

        The columns are sent by SFDC in a messy format and the types are custom SFDC types,
        so they need to be manually converted to sql data types.

        Parameters
        ----------
        table : str
            Name of table.
        column_types : bool
            Whether to retrieve field types.

        Returns
        ----------
        List or Dict
        """

        sf = self.con
        field_descriptions = eval(f'sf.{table}.describe()["fields"]')  # change to variable table
        types = {field["name"]: (field["type"], field["length"]) for field in field_descriptions}

        if columns:
            fields = columns
        else:
            fields = [field["name"] for field in field_descriptions]

        if column_types:
            dtypes = {}
            for field in fields:

                field_sfdc_type = types[field][0]
                field_len = types[field][1]
                field_sqlalchemy_type = sfdc_to_sqlalchemy_dtype(field_sfdc_type)
                if field_sqlalchemy_type == "NVARCHAR":
                    field_sqlalchemy_type = f"{field_sqlalchemy_type}({field_len})"

                dtypes[field] = field_sqlalchemy_type
            return dtypes
    else:
        raise NotImplementedError("Retrieving columns only is currently not supported")

    def copy_object(self):
        pass

    def delete_object(self):
        pass

    def create_object(self):
        pass

    def get_objects(self):
        pass

