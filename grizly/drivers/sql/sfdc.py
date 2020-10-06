import logging
from logging import Logger
from copy import deepcopy
from simple_salesforce import Salesforce
from grizly.config import Config
from grizly.config import config as default_config
from simple_salesforce.login import SalesforceAuthenticationFailed
from .base import BaseDriver
from ...config import Config
import datetime
import time
import os


def build_query(data):
    query = '"""SELECT '
    columns = ", ".join([field for field in list(data['select']["fields"].keys())])
    query += f"{columns} FROM {data['select']['table']}"
    if "where" in data['select'] and len(data['select']['where']):
        query += f" WHERE {data['select']['where']}"
    if "limit" in data['select']:
        query += f" LIMIT {data['select']['limit']}"
    query += '"""'
    return query

class SFDCDriver(BaseDriver):
    def _validate_fields(self):
        """Check if requested fields are in SF table 
        and if can be pulled (we can't pull compound fields)
        """
        fields_and_dtypes = zip(dict(self.fields, self.types))
        compound_fields = [field for field in fields_and_dtypes if fields_and_dtypes[fields] in ("address", "location")]
        if compund_fields:
            raise ValueError(f"Compund fields are unsupported. Please remove the following fields: \n{compound_fields}")
    
    def _cast_column_values(self, column_number, column_dtype, records):
        if 'string' in column_dtype:
            column_values = [str(line[column_number]) for line in records]
        elif 'float' in column_dtype:
            column_values = [float(line[column_number]) for line in records]
        elif 'date' in column_dtype and type(records[0][column_number]) == str:
            column_values = [None if not line[column_number] else datetime.datetime.strptime(line[column_number],"%Y-%m-%d") for line in records]
        else:
            column_values = [line[column_number] for line in records]
        return column_values

    def to_records(self):
        self._validate_fields()
        query = build_query(data)
        sf_table = getattr(self.source.con, self.data["table"])
        sf_data_j = sf_table.query(query)
        sf_data = []
        for i in range(0,len(sf_data_j)):
            sf_data_j[i].pop('attributes')
            sf_data.append(tuple(sf_data_j[i].values()))
        return sf_data
    
    def to_dict(self):
        _dict = {}
        records = self.to_records()
        dtypes = self.get_dtypes()
        colnames = self.get_fields()
        for i, column in enumerate(colnames):
            column_dtype = str(self._to_pyarrow_dtype(dtype=dtypes[i]))
            column_values = self._cast_column_values(column_number=i, column_dtype=column_dtype, records=records)
            _dict[self.data['select']['fields'][column]['as']] = column_values
        return _dict
