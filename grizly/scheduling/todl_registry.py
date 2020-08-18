import logging
import os
from typing import List, Literal
from ..config import Config
from .job import Job
from croniter import croniter

import dask
from redis import Redis


config = Config().get_service("scheduling")


class Registry:
    def __init__(
        self,
        env: Literal["dev", "prod"] = "prod",
        redis_host: str = None,
        redis_port: int = None,
        logger: logging.Logger = None,
    ):
        self.logger = logger or logging.getLogger(__name__)
        self.env = env
        self.redis_host = redis_host
        self.redis_port = redis_port

    @property
    def con(self):
        if self.env == "prod":
            self.host = self.redis_host or os.getenv("GRIZLY_REDIS_HOST") or config.get("redis_host")
            self.port = self.redis_port or os.getenv("GRIZLY_REDIS_PORT") or config.get("redis_port")
        elif self.env == "dev":
            self.host = self.redis_host or os.getenv("GRIZLY_REDIS_DEV_HOST") or config.get("redis_dev_host")
            self.port = self.redis_port or os.getenv("GRIZLY_REDIS_DEV_HOST") or config.get("redis_dev_port")
        else:
            raise ValueError("Only dev and prod environments are supported")

        # dev host = "10.125.68.177"
        con = Redis(host=self.host, port=self.port, db=0)
        return con

    def add_job(
        self,
        name: str,
        owner: str = None,
        trigger_names: list = [],
        tasks: List[dask.delayed] = None,
        type: Literal["regular", "system"] = "regular",
    ):
        Job(name=name).register(type=type, trigger_names=trigger_names, owner=owner, tasks=tasks)

    def get_job_names(self):
        prefix = Job.registry_prefix
        job_names = []
        for job_name_with_prefix in self.con.keys(f"{prefix}*"):
            if job_name_with_prefix is not None:
                job_name = job_name_with_prefix.decode("utf-8")[len(prefix) :]
                job_names.append(job_name)
        return job_names

    def get_jobs(self):
        job_names = self.get_job_names()
        jobs = []
        for job_name in job_names:
            jobs.append(Job(job_name, env=self.env, logger=self.logger))
        return jobs