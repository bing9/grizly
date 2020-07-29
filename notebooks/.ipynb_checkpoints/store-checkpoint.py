import json
from grizly import (
    get_path,
    initiate,
    get_columns,
    QFrame, 
    join, 
    union
)
import pandas as pd
import dask

json_path = get_path("acoe_projects","sales_ops","pipeline",
                            "eng","01_workflows","json_redshift_test.json")

class Store():
    def __init__(self, path):
        self.path = path
        print(path)
        with open(self.path, 'r') as json_file:
            d = json.load(json_file)
        self.data = d
        self.new_data = {}
        
    def to_json(self):
        for k in self.new_data:
            if k in self.data:
                msg = f"Key # {k} # already in store. Use remove_key() if you don't need it"
                raise KeyError(msg)
            else:
                self.data[k] = self.new_data[k]
                with open(self.path, 'w') as json_file:
                    json.dump(self.data, json_file)
    
    def add_key(self, **kwargs):
        for kwarg in kwargs:
            if isinstance(kwargs[kwarg], dict):
                self.new_data[kwarg] = kwargs[kwarg]
            elif isinstance(kwargs[kwarg], list):
                self.new_data[kwarg] = kwargs[kwarg]
            elif isinstance(kwargs[kwarg], str):
                self.new_data[kwarg] = kwargs[kwarg]
            else:
                raise ValueError("The key value passed to this method is not a dictionary")
    
    def initiate(columns, schema, table, json_path, subquery="", col_types=None):
        """Creates a dictionary with fields information for a Qframe and saves the data in json file.

        Parameters
        ----------
        columns : list
            List of columns.
        schema : str
            Name of schema.
        table : str
            Name of table.
        json_path : str
            Path to output json file.
        subquery : str, optional
            Name of the query in json file. If this name already exists it will be overwritten, by default ''
        col_type : list
            List of data types of columns (in 'columns' list)
        """
        if os.path.isfile(json_path):
            with open(json_path, 'r') as f:
                json_data = json.load(f)
                if json_data =="":
                    json_data = {}
        else:
            json_data = {}

        fields = {}

        if col_types == None:
            for col in columns:
                type = "num" if "amount" in col else "dim"
                field = {
                            "type": type,
                            "as": "",
                            "group_by": "",
                            "order_by": "",
                            "expression": "",
                            "select": "",
                            "custom_type": ""
                        }
                fields[col] = field

        elif isinstance(col_types, list):
            for index, col in enumerate(columns):
                custom_type = col_types[index]
                field = {
                            "type": "",
                            "as": "",
                            "group_by": "",
                            "order_by": "",
                            "expression": "",
                            "select": "",
                            "custom_type": custom_type
                        }
                fields[col] = field

        data = {
                "select": {
                    "table": table,
                    "schema": schema,
                    "fields": fields,
                    "where": "",
                    "distinct": "",
                    "having": "",
                    "limit": ""
                }
                }


        if subquery != '':
            json_data[subquery] = data
        else:
            json_data = data

        with open(json_path, 'w') as f:
            json.dump(json_data, f)

        print(f"Data saved in {json_path}")
                
    def remove_key(self, key):
        del self.data[key]
        
    def read_key(self, key):
        print(self.data[key])
        
store = Store(json_path)
store.read_key("somekey")