import logging
from logging import Logger
from copy import deepcopy
from simple_salesforce import Salesforce
from .base import BaseDriver
from ...config import Config
import os


def build_query(flow):
    query = "SELECT "
    columns = ", ".join([field for field in flow["fields"]])
    query += f"{columns} FROM {flow['table']}"
    if "where" in flow:
        query += f" WHERE {flow['where']}"
    if "limit" in flow:
        query += f" LIMIT {flow['limit']}"
    return query


class SF(BaseDriver):
    def __init__(self, logger: Logger = None):
        """Pulls Salesforce data

        Parameters
        ----------
        param1 : str
            [description]
        """
        self.logger = logger or logging.getLogger(__name__)
        self.flow = {}

    def connect(
        self,
        username: str = "",
        password: str = "",
        organization_id: str = "",
        config_key: str = "standard",
        env: str = "prod",
        proxies: dict = None,
    ):
        config = Config().get_service(config_key=config_key, service="sfdc", env=env)
        proxies = (
            proxies
            or deepcopy(Config().get_service(config_key=config_key, service="proxies"))
            or {"http": os.getenv("HTTP_PROXY"), "https": os.getenv("HTTPS_PROXY")}
        )
        username = username or config.get("username")
        password = password or config.get("password")
        organization_id = organization_id or config.get("organizationId")
        print(username, password, organization_id, proxies)
        self.sf_conn = Salesforce(password=password, username=username, organizationId=organization_id, proxies=proxies)
        return self

    def from_source(self, table: str):
        self.flow["table"] = table

    def to_records(self):
        flow = self.flow
        query = build_query(flow)
        flow["query"] = query
        bulk_str = f"self.sf_conn.bulk.{flow['table']}.query('{query}')"
        return eval(bulk_str)

    def to_file(self):
        pass
