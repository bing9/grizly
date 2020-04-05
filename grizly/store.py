import json
from .tools.sqldb import SQLDB


class Store:
    def __init__(self, path):
        self.path = path
        print(path)
        try:
            with open(self.path, "r") as json_file:
                d = json.load(json_file)
        except FileNotFoundError:
            with open(self.path, "w") as json_file:
                d = json.dump({"sample_key": "sample_key_value"}, json_file)
        self.data = d
        self.new_data = {}

    def to_store(self, key=None):
        if self.data == None:
            self.data = {}
        if key == None:
            for k in self.new_data:
                if k in self.data:
                    msg = f"Key # {k} # already in store. Use remove_key() if you don't need it"
                    raise KeyError(msg)
                else:
                    # 1
                    self.data[k] = self.new_data[k]
                    with open(self.path, "w") as json_file:
                        json.dump(self.data, json_file)
                        self.new_data = {}
        else:
            if key in self.data:
                msg = f"Key # {key} # already in store. Use remove_key() if you don't need it"
                raise KeyError(msg)
            else:
                # 2
                self.data[key] = self.new_data[key]
                with open(self.path, "w") as json_file:
                    json.dump(self.data, json_file)
                    self.new_data = {}

    def add_qframe_expression(self, subquery, field_name, expression):
        if self.new_data == {}:
            self.new_data = self.data
        self.new_data[subquery]["select"][field_name] = expression

    def add_key(self, **kwargs):
        for kwarg in kwargs:
            if isinstance(kwargs[kwarg], dict):
                self.new_data[kwarg] = kwargs[kwarg]
            elif isinstance(kwargs[kwarg], list):
                self.new_data[kwarg] = kwargs[kwarg]
            elif isinstance(kwargs[kwarg], str):
                self.new_data[kwarg] = kwargs[kwarg]
            else:
                raise ValueError(
                    "The key value passed to this method is not a dictionary"
                )

    def add_query(
        self, columns, schema, table, key_query, engine_str="", col_types=None
    ):
        """Creates a dictionary with fields information for a Qframe and saves the data in json file.

        Parameters
        ----------
        columns : list
            List of columns.
        schema : str
            Name of schema.
        table : str
            Name of table.
        key_query : str, optional
            Name of the query in json file. If this name already exists it will be overwritten, by default ''
        col_type : list
            List of data types of columns (in 'columns' list)
        """
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
                    "custom_type": "",
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
                    "custom_type": custom_type,
                }
                fields[col] = field

        data = {
            "select": {
                "table": table,
                "schema": schema,
                "fields": fields,
                "engine": engine_str,
                "where": "",
                "distinct": "",
                "having": "",
                "limit": "",
            }
        }

        self.new_data[key_query] = data

        return self

    def remove_key(self, key):
        del self.data[key]

    def get_key(self, key, oftype="dict"):
        if oftype == "dict":
            return self.data[key]
        elif oftype == "list":
            l = [k for k in self.data[key]]
            return l
        else:
            raise BaseException(f"oftype value # {str(oftype)} # not supported")

