import csv
import logging
import os
import json

import numpy as np
import pandas as pd
from simple_salesforce import Salesforce
from simple_salesforce.login import SalesforceAuthenticationFailed
from ..config import Config
from ..utils import get_path
from logging import Logger
from copy import deepcopy


class SFDC:
    """A class for extracting with Salesforce data.

    Examples
    --------
    >>> config_path = get_path('.grizly', 'config.json') # contains SFDC username and password
    >>> config = Config().from_json(config_path)

    Returns
    -------
    CSV or DataFrame
    """

    def __init__(
        self,
        username: str = None,
        password: str = None,
        organization_id: str = None,
        instance_url: str = None,
        env: str = "prod",
        logger: Logger = None,
        proxies: dict = None,
        config_key: str = "standard",
    ):
        self.env = env
        try:
            config = Config().get_service(config_key=config_key, service="sfdc", env=env)
        except:
            config = None
        # first lookup in parameters, then config, then env variables
        self.proxies = (
            proxies
            or deepcopy(Config().get_service(config_key=config_key, service="proxies"))
            or {"http": os.getenv("HTTP_PROXY"), "https": os.getenv("HTTPS_PROXY")}
        )
        self.username = username or config.get("username")
        self.password = password or config.get("password")
        self.organization_id = organization_id or config.get("organizationId")
        self.instance_url = instance_url
        self.logger = logger if logger else logging.getLogger(__name__)

    def _connect(self):

        if self.env == "prod":
            try:
                sf_conn = Salesforce(
                    username=self.username,
                    password=self.password,
                    organizationId=self.organization_id,
                    proxies=self.proxies,
                )
            except SalesforceAuthenticationFailed:
                self.logger.exception(
                    "Could not log in to SFDC. \
                Are you sure your password hasn't expired and your proxy is set up correctly?"
                )
                raise

        elif self.env == "stage":
            try:
                sf_conn = Salesforce(
                    instance_url=self.instance_url,
                    username=self.username,
                    password=self.password,
                    organizationId=self.organization_id,
                    sandbox=True,
                    security_token="",
                    proxies=self.proxies,
                )
            except SalesforceAuthenticationFailed:
                self.logger.exception(
                    "Could not log in to SFDC. \
                Are you sure your password hasn't expired and your proxy is set up correctly?"
                )
                raise

        else:
            raise ValueError("Only 'prod' and 'stage' environments are supported")

        return sf_conn

    def get_next_batch(self, sf, raw_response):

        next_batch_url = raw_response.pop("nextRecordsUrl", None)

        if not next_batch_url:
            return None

        next_batch_id = next_batch_url.split("/")[-1]
        next_batch = sf.query_more(next_batch_id)

        return next_batch

    def get_response_in_batches(self, sf, table, query):

        raw_response = eval(f"sf.bulk.{table}.query(query)")
        return raw_response
        next_batch = self.get_next_batch(sf, raw_response)

        while next_batch:
            self.logger.info(f"Batch of size {len(next_batch)} added. Loading next batch...")
            raw_response.extend(next_batch)
            next_batch = self.get_next_batch(sf, next_batch)

        return raw_response

    def query(self, query, bulk=False):
        """Query a SFDC table. Only simple SELECTs are supported.

        Parameters
        ----------
        columns : list
            Columns to be passed to SELECT
        table : str
            The SFDC table to query
        env : str
            The environment on which to run query
        where : str, optional
            Where clause, by default None
        """

        query_words = query.lower().split()
        table = query_words[query_words.index("from") + 1]
        sf = self._connect()

        self.logger.info(f"Querying {table}...")

        try:
            if bulk:
                # raw_response = self.get_response_in_batches(sf, table, query)
                raw_response = eval(f"sf.bulk.{table}.query(query)")
                # return raw_response
                response = SFDCResponseBulk(raw_response, logger=self.logger)
            else:
                raw_response = sf.query_all(query)
                response = SFDCResponse(raw_response, logger=self.logger)
        except:
            self.logger.exception("Error when connecting to SFDC")
            raise

        self.logger.debug(f"SFDC table {table} was successfully queried")

        return response


class SFDCResponse:
    def __init__(self, data, logger=None):
        self.data = data
        self.columns = [item for item in data["records"][0] if item != "attributes"]
        self.logger = logger if logger else logging.getLogger(__name__)

    def __str__(self):
        return json.dumps(self.data, indent=4)

    def to_df(self, dtype=None):

        l = []
        for item in self.data["records"]:
            row = []
            for column in self.columns:
                row.append(item[column])
            l.append(row)

        df = pd.DataFrame(l, columns=self.columns, dtype=dtype).replace(to_replace=["None"], value=np.nan)

        return df

    def to_csv(self, file_path, sep="\t"):

        if not file_path:
            raise ValueError("File path is required")

        rows = []
        rows.append(self.columns)
        for item in self.data["records"]:
            row = []
            for column in self.columns:
                row.append(item[column])
            rows.append(row)

        with open(file_path, "w", newline="", encoding="utf-8") as csv_file:
            self.logger.debug(f"Writing to {os.path.basename(file_path)}...")
            writer = csv.writer(csv_file, delimiter=sep)
            writer.writerows(rows)
            self.logger.debug(f"Successfuly wrote to {os.path.basename(file_path)}")


class SFDCResponseBulk(SFDCResponse):
    def __init__(self, data, logger):
        self.data = data
        self.columns = [item for item in data[0] if item != "attributes"]
        self.logger = logger

    def to_df(self, dtype=None):
        df = pd.DataFrame(self.data).drop("attributes", axis=1).replace(to_replace=["None"], value=np.nan)
        return df

    def to_csv(self, file_path, sep="\t"):

        if not file_path:
            raise ValueError("File path is required")

        rows = []
        rows.append(self.columns)
        for item in self.data:
            row = []
            for column in self.columns:
                row.append(item[column])
            rows.append(row)

        with open(file_path, "w", newline="", encoding="utf-8") as csv_file:
            self.logger.debug(f"Writing to {os.path.basename(file_path)}...")
            writer = csv.writer(csv_file, delimiter=sep)
            writer.writerows(rows)
            self.logger.debug(f"Successfuly wrote to {os.path.basename(file_path)}")
