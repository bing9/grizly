from copy import deepcopy
import json
import logging
import os

from box import Box

from .sources.filesystem.old_s3 import S3


class Store(Box):
    logger = logging.getLogger("store")

    def __repr__(self):
        return f"Store({self.to_dict()})"

    def deepcopy(self) -> "Store":
        """Make deep copy of Store

        Returns
        -------
        Store
            deep copy of store
        """
        d = deepcopy(self.to_dict())
        return Store(d)

    def from_json(self, json_path: str, subquery: str = None):
        """Read QFrame.data from json file

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
            json_data = S3(url=json_path).to_serializable()
        else:
            with open(json_path, "r") as f:
                json_data = json.load(f)

        if json_data and subquery:
            new_data = json_data[subquery]
        else:
            new_data = json_data

        self.clear()
        self.update(new_data)

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

        # attempt to load the json from provided location
        if json_path.startswith("s3://"):
            json_data = self._from_s3(url=json_path)
        else:
            json_data = self._from_local(json_path=json_path)

        if subquery:
            json_data[subquery] = self.to_dict()
        else:
            if json_data:
                self.logger.warning("Overwriting existing store.")
            json_data = self.to_dict()

        if json_path.startswith("s3://"):
            self._to_s3(json_path, json_data)
        else:
            self._to_local(json_path, json_data)

        self.logger.info(f"Data saved in {json_path}")

    @staticmethod
    def _to_local(path, serializable):
        with open(path, "w") as f:
            json.dump(serializable, f, indent=4)

    @staticmethod
    def _to_s3(url, serializable):
        S3(url=url).from_serializable(serializable)

    @staticmethod
    def _from_local(json_path: str) -> dict:
        if os.path.exists(json_path):
            with open(json_path, "r") as f:
                data = json.load(f)
        else:
            data = {}
        return data

    @staticmethod
    def _from_s3(url: str) -> dict:
        s3 = S3(url=url)
        if s3.exists():
            data = s3.to_serializable()
        else:
            data = {}
        return data
