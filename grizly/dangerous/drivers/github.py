import logging
from logging import Logger
import pandas
import requests
import os
import base64
from ..basecreators import QueryDriver
from ...config import Config, _validate_config
from ...tools.s3 import S3


def get_final(d, keys, lastkey):
    if keys == []:
        return d
    elif isinstance(d, dict):
        d = d.get(keys[0])
        return get_final(d, keys[1:], lastkey=lastkey)
    elif isinstance(d, list):
        return ", ".join([item[lastkey] for item in d])

class GitHub(QueryDriver):
    def __init__(
        self,
        logger: Logger = None
    ):
        """Pulls GitHub data

        Parameters
        ----------
        username : str
            [description]
        username_password : str
            [description]
        pages : int, optional
            [description], by default 100
        """
        self.logger = logger or logging.getLogger(__name__)

    def connect(
        self,
        username: str = None,
        username_password: str = None,
        pages: int = 100,
        proxies: dict = None,
    ):
        self.flow["username"]  = username
        self.flow["username_password"] = username_password
        self.flow["pages"] = pages
        self.flow["config"] = None
        self.flow["proxies"] = proxies
        return self

    def from_source(self, path: str, owner: str, repo: str = None):
        """API url from where to pull the github data

        Parameters
        ----------
        path : {'issues', 'file_path'}
            path to github data

        Returns
        -------
        self
        """
        if path == "issues":
            self.flow["url"] = f"https://api.github.com/orgs/{owner}/issues"
        else:
            self.flow["url"] = f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"
        return self

    def to_records(self, flatten=True, sep="_"):
        flow = self.flow
        url = self.get_query() + f"&page={flow['limit']}"
        data = requests.get(url
                    , auth=(flow["username"], flow["username_password"])
                    , proxies=flow["proxies"],
                )
        if flatten:
            records = []
            fields = self.get_fields()
            for dict_record in data.json():
                record = {}
                for field in fields:
                    if "/" in field:
                        keys = field.split("/")
                        value = get_final(dict_record, keys, keys[-1])
                        record["_".join(keys)] = value
                    else:
                        record[field] = dict_record[field]
                records.append(record)
            return records
        else:
            return data.json()

    def to_file(self, path):
        flow = self.flow
        if flow["owner"]==None or flow["repo"] == None or flow["content_path"]==None:
            msg = f"In from_source() you are missing owner or/and repo and or content_path"
            return self.logger.warning(msg)
        data = requests.get(self.flow["base_url"], auth=(flow["username"]
                    , flow["username_password"]), proxies=flow["proxies"],)
        decoded_content = str(base64.b64decode(data.json()["content"]), "utf-8")
        with open(path, "w") as f:
            f.write(decoded_content)
        return self