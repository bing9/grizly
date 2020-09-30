import logging
from box import Box
import json
import os


from .tools.s3 import S3


class Store(Box):
    logger = logging.getLogger("store")

    def __repr__(self):
        return f"Store({self.to_dict()})"

    def from_json(self, json_path: str, subquery: str = None):
        """Reads QFrame.data from json file.

        Parameters
        ----------
        json_path : str
            Path to json file.
        subquery : str, optional
            Key in json file, by default None

        Returns
        -------
        QFrame
        """
        if json_path.startswith("s3://"):
            data = S3(url=json_path).to_serializable()
        else:
            with open(json_path, "r") as f:
                data = json.load(f)

        self.clear()
        if data and subquery:
            self.update(data[subquery])
        else:
            self.update(data)

        return self

    def to_json(self, json_path: str, subquery: str = None):
        """Save Store to json file

        Parameters
        ----------
        json_path : str
            Path to json file.
        subquery : str, optional
            Key in json file, by default None
        """
        json_data = {}
        data = {}

        # attempt to load the json from provided location
        if os.path.isfile(json_path):
            with open(json_path, "r") as f:
                json_data = json.load(f)
                if json_data:
                    data = json_data
        if subquery:
            data[subquery] = self.qf.data
        else:
            if json_data:
                self.qf.logger.warning("Overwriting existing store.")
            data = self.to_dict()

        if json_path.startswith("s3://"):
            self._to_s3(json_path, data)
        else:
            self._to_local(json_path, data)

        self.logger.info(f"Data saved in {json_path}")

    @staticmethod
    def _to_local(path, serializable):
        with open(path, "w") as f:
            json.dump(serializable, f, indent=4)

    @staticmethod
    def _to_s3(url, serializable):
        S3(url=url).from_serializable(serializable)
