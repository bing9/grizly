import logging
import os
from logging import Logger
from sqlite3.dbapi2 import NotSupportedError

from ...config import Config
from ...config import config as default_config
from .base import BaseTable, RDBMSBase
from ...utils.type_mappers import sfdc_to_sqlalchemy
from simple_salesforce import Salesforce
from simple_salesforce.login import SalesforceAuthenticationFailed


class SFDCTable(BaseTable):
    def __init__(self, name, source, schema=None):
        super().__init__(name=name, source=source, schema=schema)
        self.db = "sfdc"

    @property
    def sf_table(self):
        return getattr(self.source.con, self.name)

    @property
    def fields(self):
        field_descriptions = self.sf_table.describe()["fields"]
        fields = [field["name"] for field in field_descriptions]
        return fields

    # @property
    # def mapped_types(self):
    #     field_descriptions = self.sf_table.describe()["fields"]
    #     types_and_lengths = [(field["type"], field["length"]) for field in field_descriptions]
    #     dtypes = []
    #     for field_sfdc_type, field_len in types_and_lengths:
    #         field_sqlalchemy_type = sfdc_to_sqlalchemy(field_sfdc_type)
    #         if field_sqlalchemy_type == "NVARCHAR":
    #             field_sqlalchemy_type = f"{field_sqlalchemy_type}({field_len})"
    #         dtypes.append(field_sqlalchemy_type)
    #     return dtypes

    @property
    def types(self):
        field_descriptions = self.sf_table.describe()["fields"]
        types_and_lengths = [(field["type"], field["length"]) for field in field_descriptions]
        dtypes = []
        for _type, field_len in types_and_lengths:
            sql_type = sfdc_to_sqlalchemy(_type)
            if sql_type == "VARCHAR":
                _type += f"({field_len})"
            dtypes.append(_type)
        return dtypes

    @property
    def nrows(self):
        query = f"SELECT COUNT() FROM {self.name}"
        return self.source.con.query(query)["totalSize"]

    @property
    def ncols(self):
        pass


class SFDB(RDBMSBase):
    _context = ""
    _quote = ""
    _use_ordinal_position_notation = False
    dialect = "sfdc"

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
        self._con = None
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
        if self._con:
            return self._con
        try:
            con = Salesforce(
                password=self.password,
                username=self.username,
                organizationId=self.organization_id,
                proxies=self.proxies,
            )
            self._con = con
            return self._con
        except SalesforceAuthenticationFailed:
            self.logger.info(
                "Could not log in to SFDC. Are you sure your password hasn't expired and your proxy is set up correctly?"
            )
            raise SalesforceAuthenticationFailed

    @property
    def tables(self):
        """Alias for objects"""
        return self.objects

    # TODO: delete
    def get_columns(self, schema=None, table=None, columns=None, column_types=True):
        if column_types:
            return self.table(table).columns, self.table(table).types
        return self.table(table).columns

    @property
    def objects(self):
        table_names = [obj["name"] for obj in self.con.describe()["sobjects"]]
        return table_names

    def object(self, name):
        return SFDCTable(name=name, source=self)

    def table(self, name, schema=None):
        """Alias for object"""
        return self.object(name=name)

    @staticmethod
    def map_types(types, to):
        if to == "postgresql":
            mapped = [sfdc_to_sqlalchemy(t) for t in types]
        else:
            raise NotImplementedError
        return mapped

    def copy_object(self):
        raise NotImplementedError

    def delete_object(self):
        raise NotImplementedError

    def create_object(self):
        raise NotImplementedError

    def copy_table(self, **kwargs):
        raise NotSupportedError

    def create_table(self, **kwargs):
        raise NotSupportedError

    def insert_into(self, **kwargs):
        raise NotSupportedError

    def delete_from(self, **kwargs):
        raise NotSupportedError

    def drop_table(self, **kwargs):
        raise NotSupportedError

    def write_to(self, **kwargs):
        raise NotSupportedError


sfdb = SFDB()
