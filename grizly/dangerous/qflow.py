import os
import json
import logging
from logging import Logger
from copy import deepcopy
import pandas
from .drivers.github import GitHub


"""
- to_arrow
- get_dtypes
- get_extract (should be to_extract and implemented in parent, subclass should implement get_partitions)
- submit -> to_cluster or to_dask
done in subclass
- select
- get_fields
- from_json (renamed to read)
- save_json (renamed to save)
- get_query
- where -> add_where
- limit -> add_limit
done in parent class
- copy (done in QFlow not subclass)
- rename
"""

class QFlow():
    def __init__(
            self
            , driver_name: str = None
            , driver=None
            , flow=None
            , logger: Logger = None
            ):
        self.logger = logger or logging.getLogger(__name__)
        self.driver_name = driver_name
        if driver_name == "github":
            self.driver = GitHub()
        if flow:
            self.driver.flow = flow
        try:
            self.connect = self.driver.connect
        except:
            pass
        self.select = self.driver.select
        self.where = self.driver.where
        self.limit = self.driver.limit
        self.get_query = self.driver.get_query
        self.get_fields = self.driver.get_fields #can be moved to parent
        self.from_source = self.driver.from_source
        self.to_records = self.driver.to_records
        self.to_file = self.driver.to_file

    def save(self, json_path: str, key: str = None):
        """Saves flow in text file format
        """
        if os.path.isfile(json_path):
            with open(json_path, "r") as f:
                json_data = json.load(f)
                if json_data == "":
                    json_data = {}
        else:
            json_data = {}

        if key != None:
            json_data[key] = self.driver.flow
        else:
            json_data = self.driver.flow

        with open(json_path, "w") as f:
            json.dump(json_data, f, indent=4)

        self.logger.info(f"Data saved in {json_path}")

    def copy(self):
        """Makes a copy of QFlow.

        Returns
        -------
        QFlow
        """
        flow = deepcopy(self.driver.flow)
        driver = deepcopy(self.driver)
        return QFlow(driver_name=self.driver_name, driver=driver, flow=flow)
    
    def rename(self, fields: dict):
        if not isinstance(fields, dict):
            raise ValueError("Fields parameter should be of type dict.")
        if "fields" not in self.driver.flow:
            raise ValueError("Fields are not in your flow. Try doing select() first")
        
        for field in fields:
            self.driver.flow["fields"][field]["as"] = fields[field]
        return self

    def help(self):
        methods = [ m for m in dir(self) if not m.startswith('__')]
        print(self.driver.__init__.__doc__)
        for method in methods:
            header = f"""{method}
                        =========
                     """
            print(header)
            try:
                print(eval(f"self.driver.{method}.__doc__"))
            except AttributeError:
                print(eval(f"self.{method}.__doc__"))
        return self

    def get_flow(self):
        return self.driver.flow

    def from_json(self, json_path: str, key: str = None):
        """Reads from a json file

        Parameters
        ----------
        json_path : str
            [description]
        key : str, optional
            [description], by default None
        """
        with open(json_path, "r") as f:
            data = json.load(f)
            if data != {}:
                if key == "":
                    self.driver.flow = data
                else:
                    self.driver.flow = data[key]
            else:
                self.flow = data
        return self

    def to_csv(self, path):
        """Create a CSV using the Pandas DataFrame

        Parameters
        ----------
        path : str
            path to the csv file

        Returns
        -------
        class
            self
        """
        df = self.to_df()
        df.to_csv(path)
        return self
    
    def to_arrow(self):
        pass
    
    def to_df(self):
        dicts = self.to_memory()
        return pandas.DataFrame.from_records(dicts)

    def from_dict(self, flow: dict):
        self.driver.flow = flow
        return self

    def __str__(self):
        sql = self.get_query()
        return sql