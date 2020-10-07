import logging
import os
from logging import Logger

from ...config import Config
from ...config import config as default_config
from ..base import BaseSource
from ...utils.type_mappers import sfdc_to_sqlalchemy_dtype
from simple_salesforce import Salesforce
from simple_salesforce.login import SalesforceAuthenticationFailed


class SFDCTable(BaseTable):
    def __init__(self, name):
        super().__init__(name=name)

    @property
    def fields(self):
        sf_table = getattr(self.source.con, self.name)
        field_descriptions = sf_table.describe()["fields"]
        fields = [field["name"] for field in field_descriptions]
        return fields

    @property
    def types(self):
        sf_table = getattr(self.source.con, self.name)
        field_descriptions = sf_table.describe()["fields"]

        types_and_lengths = [(field["type"], field["length"]) for field in field_descriptions]
        dtypes = []
        for field_sfdc_type, field_len in types_and_lengths:
            field_sqlalchemy_type = sfdc_to_sqlalchemy_dtype(field_sfdc_type)
            if field_sqlalchemy_type == "NVARCHAR":
                field_sqlalchemy_type = f"{field_sqlalchemy_type}({field_len})"
            dtypes.append(field_sqlalchemy_type)
        return dtypes

    @property
    def nrows(self):
        query = f"SELECT COUNT() FROM {self.name}"
        return self.source.con.query(query)["totalSize"]

    @property
    def ncols(self):
        pass


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

    @property
    def tables(self):
        """Alias for objects"""
        return self.objects

    @property
    def objects(self):
        table_names = [obj["name"] for obj in self.con.describe()["sobjects"]]
        return table_names

    def table(self, name):
        return SFDCTable(name, con=self.con)

    def copy_object(self):
        raise NotImplementedError

    def delete_object(self):
        raise NotImplementedError

    def create_object(self):
        raise NotImplementedError


sfdb = SFDB()
