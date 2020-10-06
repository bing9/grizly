import logging
from logging import Logger
from copy import deepcopy
from simple_salesforce import Salesforce
from .base import BaseDriver
from ...config import Config
import datetime
import time
import os


def build_query(data):
    query = '"""SELECT '
    columns = ", ".join([field for field in list(data["select"]["fields"].keys())])
    query += f"{columns} FROM {data['select']['table']}"
    if "where" in data["select"] and len(data["select"]["where"]):
        query += f" WHERE {data['select']['where']}"
    if "limit" in data["select"]:
        query += f" LIMIT {data['select']['limit']}"
    query += '"""'
    return query


class SFDC(BaseDriver):
    def __init__(self, logger: Logger = None):
        """Pulls Salesforce data
        Parameters
        ----------
        param1 : str
            [description]
        """
        self.logger = logger or logging.getLogger(__name__)
        self.data = {}
        self.connect()

    def _validate_fields(self):
        """Check if requested fields are in SF table 
        and if can be pulled (we can't pull compound fields)
        """
        fields_data = set(self.get_fields())
        fields_sf = set()
        execute_str = f"self.sf_conn.{self.get_table()}.describe()"
        schema = eval(execute_str)

        # loop through the SF fields
        # if not a compound field, then append to the list of fields
        # Note: Addresses, Locations and other compound fields are not supported by the Bulk API, which we use
        for fieldDict in schema.get("fields", []):
            fieldType = fieldDict.get("type")
            if fieldType not in ["address", "location"]:
                fields_sf.add(fieldDict.get("name"))

        if not len(fields_sf):
            "Empty table"
        fields_missing = fields_data - fields_sf
        fields_valid = fields_data - fields_missing
        for field in fields_missing:
            del self.data["select"]["fields"][field]
        self.data["select"]["fields_missing"] = fields_missing
        return self.data["select"]["fields"], self.data["select"]["fields_missing"]

    def _cast_column_values(self, column_number, column_dtype, records):
        if "string" in column_dtype:
            column_values = [str(line[column_number]) for line in records]
        elif "float" in column_dtype:
            column_values = [float(line[column_number]) for line in records]
        elif "date" in column_dtype and type(records[0][column_number]) == str:
            column_values = [
                None
                if not line[column_number]
                else datetime.datetime.strptime(line[column_number], "%Y-%m-%d")
                for line in records
            ]
        else:
            column_values = [line[column_number] for line in records]
        return column_values

    def _pull_SF_dtypes(self):
        dtypes = {}
        fields_str = f"""self.sf_conn.{self.get_table()}.describe()['fields']"""
        fields_sf = eval(fields_str)

        for i in range(0, len(fields_sf)):
            if fields_sf[i]["name"] in self.get_fields():
                self.data["select"]["fields"][fields_sf[i]["name"]]["custom_type"] = fields_sf[i][
                    "type"
                ]

    #         dtypes_ordered = {}
    #         for field in self.data["fields"]:
    #             dtypes_ordered[field] = dtypes[field]
    #         self.data['dtypes'] = dtypes_ordered

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
        self.sf_conn = Salesforce(
            password=password, username=username, organizationId=organization_id, proxies=proxies
        )
        return self

    def from_source(self, table: str):
        self.data["table"] = table

    def to_records(self):
        data = self.data
        self._validate_fields()
        #         self.logger.info('Fields missing:', flow['missig_fields'])
        if len(data["select"]["fields_missing"]):
            print(
                "WARNING field is missing or it is a compound field:",
                data["select"]["fields_missing"],
            )
        query = build_query(data)
        bulk_str = f"self.sf_conn.bulk.{self.get_table()}.query({build_query(data)})"
        sf_data_j = eval(bulk_str)
        sf_data = []
        for i in range(0, len(sf_data_j)):
            sf_data_j[i].pop("attributes")
            sf_data.append(tuple(sf_data_j[i].values()))
        return sf_data

    def get_dtypes(self):
        fields = self.get_fields()
        if not "custom_type" in self.data["select"]["fields"][fields[0]]:
            self._pull_SF_dtypes()
        dtypes = [
            self.data["select"]["fields"][column]["custom_type"] for column in self.get_fields()
        ]
        return dtypes

    def to_dict(self):
        _dict = {}
        records = self.to_records()
        dtypes = self.get_dtypes()
        colnames = self.get_fields()
        for i, column in enumerate(colnames):
            column_dtype = str(self._to_pyarrow_dtype(dtype=dtypes[i]))
            column_values = self._cast_column_values(
                column_number=i, column_dtype=column_dtype, records=records
            )
            _dict[column] = column_values
        return _dict

    def groupby(self, fields: list = None):
        """Adds GROUP BY statement.
        Parameters
        ----------
        fields : list or string
            List of fields or a field, if None then all fields are grouped
        Examples
        --------
        >>> data = {'select': {'fields': {'CustomerId': {'type': 'dim'}, 'Sales': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}}
        >>> qf = QFrame(dsn="redshift_acoe").from_dict(data)
        >>> qf = qf.groupby(['CustomerId'])['Sales'].agg('sum')
        >>> print(qf)
        SELECT "CustomerId",
               sum("Sales") AS "Sales"
        FROM schema.table
        GROUP BY 1
        Returns
        -------
        QFrame
        """
        assert (
            "union" not in self.data["select"]
        ), "You can't group by inside union. Use select() method first."

        if isinstance(fields, str):
            fields = [fields]

        if fields is None:
            fields = self.get_fields(not_selected=True)
        else:
            fields = self._get_fields_names(fields)

        for field in fields:
            self.data["select"]["fields"][field]["group_by"] = "group"

        return self

    def to_file(self):
        pass
