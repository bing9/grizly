import pandas
import requests
import os
from ..config import Config, _validate_config
from .base import BaseTool
from .s3 import S3


class GitHub(BaseTool):
    def __init__(
        self,
        username: str = None,
        username_password: str = None,
        pages: int = 100,
        proxies: dict = None,
        *args,
        **kwargs,
    ):
        """Pulls GitHub data into a pandas data frame

        Parameters
        ----------
        username : str
            [description]
        username_password : str
            [description]
        pages : int, optional
            [description], by default 100
        """
        super().__init__(*args, **kwargs)

        if username_password is None:
            config = Config().get_service(config_key=self.config_key, service="github")
            self.config = config
            self.username = config["username"]
            self.username_password = config["username_password"]
            self.pages = config["pages"]
        else:
            self.username = username
            self.username_password = username_password
            self.pages = pages
            self.config = None
        self.proxies = proxies or Config().get_service(config_key=self.config_key, service="proxies")

    def from_issues(self, url: str):
        """Gets issues into a data frame extract

        Parameters
        ----------
        org_name : str
            [name of the github org]
        
        Returns
        -------
        self, do self.data for the dataframe
        """

        print("Test")
        records = []
        if self.username is None:
            self.username = Config().get_service(config_key=self.config_key, service="github")["username"]
        if self.username_password is None:
            self.username_password = Config().get_service(config_key=self.config_key, service="github")[
                "username_password"
            ]

        for page in range(self.pages):
            page += 1
            issues = f"{url}&page={page}"
            data = requests.get(issues, auth=(self.username, self.username_password), proxies=self.proxies,)
            if len(data.json()) == 0:
                break
            if page == 1:
                records.append(
                    [
                        "url",
                        "repository_name",
                        "user_login",
                        "assignees_login",
                        "milestone_id",
                        "milestone_title",
                        "milestone_description",
                        "milestone_open_issues",
                        "milestone_closed_issues",
                        "milestone_created_at",
                        "milestone_updated_at",
                        "milestone_closed_at",
                        "milestone_due_on",
                        "title",
                        "created_at",
                        "updated_at",
                        "state",
                        "labels",
                    ]
                )
            for i in range(len(data.json())):
                record = []
                record.append(data.json()[i]["url"])
                record.append(data.json()[i]["repository"]["name"])
                record.append(data.json()[i]["user"]["login"])
                record.append(", ".join([assignee["login"] for assignee in data.json()[i]["assignees"]]))
                try:
                    record.append(data.json()[i]["milestone"]["id"])
                    record.append(data.json()[i]["milestone"]["title"])
                    record.append(data.json()[i]["milestone"]["description"])
                    record.append(data.json()[i]["milestone"]["open_issues"])
                    record.append(data.json()[i]["milestone"]["closed_issues"])
                    record.append(data.json()[i]["milestone"]["created_at"])
                    record.append(data.json()[i]["milestone"]["updated_at"])
                    record.append(data.json()[i]["milestone"]["closed_at"])
                    record.append(data.json()[i]["milestone"]["due_on"])
                except:
                    no_milestones = [
                        0,
                        "no_milestone",
                        "no_milestone",
                        0,
                        0,
                        "no_milestone",
                        "no_milestone",
                        "no_milestone",
                        "no_milestone",
                    ]
                    for no_milestone in no_milestones:
                        record.append(no_milestone)
                record.append(data.json()[i]["title"])
                record.append(data.json()[i]["created_at"])
                record.append(data.json()[i]["updated_at"])
                record.append(data.json()[i]["state"])
                record.append(", ".join([label["name"] for label in data.json()[i]["labels"]]))
                records.append(record)

        self.df = pandas.DataFrame(records[1:])
        self.df.columns = records[0]

        return self
