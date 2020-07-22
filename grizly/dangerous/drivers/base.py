import os
import json
import logging
from logging import Logger
from copy import deepcopy
import pandas
from abc import ABC, abstractmethod
from ..experimental import Extract


class BaseDriver(ABC):
    def __init__(self, driver_name: str = None, driver=None, flow=None, logger: Logger = None):
        self.logger = logger or logging.getLogger(__name__)
        self.driver_name = driver_name
        self.flow = flow

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def from_source(self):
        pass

    @abstractmethod
    def to_records(self):
        pass

    @abstractmethod
    def to_file(self):
        pass

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

        if key is not None:
            json_data[key] = self.flow
        else:
            json_data = self.flow

        with open(json_path, "w") as f:
            json.dump(json_data, f, indent=4)

        self.logger.info(f"Data saved in {json_path}")

    def copy(self):
        """Makes a copy of QFlow.

        Returns
        -------
        QFlow
        """
        flow = deepcopy(self.flow)
        driver = deepcopy(self.driver)
        return QFlow(driver_name=self.driver_name, driver=driver, flow=flow)

    def rename(self, fields: dict):
        if not isinstance(fields, dict):
            raise ValueError("Fields parameter should be of type dict.")
        if "fields" not in self.flow:
            raise ValueError("Fields are not in your flow. Try doing select() first")

        for field in fields:
            self.flow["fields"][field]["as"] = fields[field]
        return self

    def help(self):
        methods = [m for m in dir(self) if not m.startswith("__")]
        print(self.__init__.__doc__)
        for method in methods:
            header = f"""{method}
                        =========
                     """
            print(header)
            try:
                print(eval(f"self.{method}.__doc__"))
            except AttributeError:
                print(eval(f"self.{method}.__doc__"))
        return self

    def select(self, fields: list or dict or str):
        """TO Review: if select is dict create fields
        maybe this is not good workflow though might
        be confusing

        Parameters
        ----------
        fields : listordictorstr
            [description]

        Returns
        -------
        [type]
            [description]
        """
        if isinstance(fields, dict):
            self.flow["fields"] = fields
        return self

    def where(self, where):
        """Implements API filters (SQL where)

        Parameters
        ----------
        where : str
            URL API filter parameters

        Examples
        --------
        Get All
        >>> qf.where("filter=all")
        Combine where parameters with &
        >>> qf.where("filter=all&state=open")
        """
        self.flow["where"] = where
        return self

    def limit(self, limit):
        self.flow["limit"] = limit
        return self

    def get_fields(self):
        return self.flow["fields"].keys()

    def get_query(self):
        """Returns the final API url REST query string
        """
        url = self.flow["url"] + "?" + self.flow["url_params"]
        return url

    def get_flow(self):
        return self.flow

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
                    self.flow = data
                else:
                    self.flow = data[key]
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
        dicts = self.to_records()
        return pandas.DataFrame.from_records(dicts)

    def to_extract(self, name, *args, **kwargs):
        return Extract(name=name, driver=self, *args, **kwargs)

    def schedule(self, extract_name, *args, **kwargs):
        """ Schedule an extract job described by the flow """
        Extract(name=extract_name, driver=self, *args, **kwargs).register(**kwargs)

    def from_dict(self, flow: dict):
        self.flow = flow
        return self

    def __str__(self):
        sql = self.get_query()
        return sql
