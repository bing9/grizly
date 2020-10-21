import logging
import os
from logging import Logger
from sqlite3.dbapi2 import NotSupportedError
from typing import Any, Dict, Iterable, List

from simple_salesforce import Salesforce
from simple_salesforce.login import SalesforceAuthenticationFailed

from ...config import Config
from ...config import config as default_config
from ...utils.functions import chunker
from ...utils.type_mappers import sfdc_to_pyarrow, sfdc_to_python, sfdc_to_sqlalchemy
from .base import BaseTable, RDBMSBase


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

    def _load_attrs_from_config(self, config: Config):
        sfdc_config = config.get_service("sfdc")
        self.username = sfdc_config.get("username")
        self.password = sfdc_config.get("password")
        self.organization_id = sfdc_config.get("organizationId")
        self.proxies = sfdc_config.get("proxies") or {
            "http": os.getenv("HTTP_PROXY"),
            "https": os.getenv("HTTPS_PROXY"),
        }

    def _fetch_records(self, query: str, table: str) -> List[tuple]:
        table = getattr(self.con.bulk, table)
        response = table.query(query)
        records = self._sfdc_records_to_records(response)
        return records

    def _fetch_records_iter(self, query: str, chunksize: int = 20) -> Iterable:
        urls = self._get_urls_from_response(query=query)
        url_chunks = chunker(urls, size=chunksize)
        for url_chunk in url_chunks:
            records_chunk = []
            for url in url_chunk:
                records = self._fetch_records_url(url)
                records_chunk.extend(records)
            yield records_chunk

    def _get_urls_from_response(self, query: str) -> List[str]:
        result = self.con.query(query, include_deleted=False)
        second_url = result["nextRecordsUrl"]
        chunksize = second_url.split("-")[1]
        first_url = second_url.replace(chunksize, "0")
        urls = [first_url]
        while True:
            if not result["done"]:
                url = result["nextRecordsUrl"]
                urls.append(url)
                result = self.con.query_more(result["nextRecordsUrl"], identifier_is_url=True)
            else:
                break
        return urls

    def _fetch_records_url(self, url: str) -> List[tuple]:
        self.logger.debug(f"Fetching records from {url}...")
        response = self.con.query_more(url, identifier_is_url=True)
        records = self._sfdc_records_to_records(response)
        self.logger.debug(f"Fetched {len(records)} from {url}.")
        return records

    @staticmethod
    def _sfdc_records_to_records(sfdc_records: List[Dict[str, Any]]) -> List[tuple]:
        """Convert weird SFDC response to records"""

        if "records" in sfdc_records:  # non-bulk API responses
            sfdc_records = sfdc_records["records"]

        records = []
        for i in range(len(sfdc_records)):
            sfdc_records[i].pop("attributes")
            records.append(tuple(sfdc_records[i].values()))
        return records

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
        all_source_columns = self.table(table).columns
        all_source_dtypes = self.table(table).types
        if columns:
            cols_to_return = [col for col in all_source_columns if col in columns]
        else:
            cols_to_return = all_source_columns
        if column_types:
            types = [
                dtype
                for col, dtype in dict(zip(all_source_columns, all_source_dtypes)).items()
                if col in cols_to_return
            ]
            return cols_to_return, types
        else:
            return cols_to_return

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
            mapping_func = sfdc_to_sqlalchemy
        elif to == "pyarrow":
            mapping_func = sfdc_to_pyarrow
        elif to == "python":
            mapping_func = sfdc_to_python
        else:
            raise NotImplementedError
        mapped = [mapping_func(t) for t in types]
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
