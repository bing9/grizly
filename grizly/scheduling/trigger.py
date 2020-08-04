from datetime import datetime, timezone
import json
import logging

from redis import Redis

from . import job as _job
from ..utils import none_safe_loads


class Trigger:
    prefix = "grizly:trigger:"

    def __init__(self, name: str, logger: logging.Logger = None):
        self.name = name
        self.name_with_prefix = self.prefix + name
        self.logger = logger or logging.getLogger(__name__)

    def __repr__(self):
        return f"{self.__class__.__name__}(name='{self.name}')"

    @property
    def getall(self):
        return self.con.hgetall(self.name)

    @property
    def con(self):
        con = Redis(host="10.125.68.177", port=80, db=0)
        return con

    @property
    def jobs(self):
        jobs = none_safe_loads(self.con.hget(self.name_with_prefix, "jobs"))
        return [_job.Job(name=job) for job in jobs]

    @property
    def is_triggered(self):
        return none_safe_loads(self.con.hget(self.name_with_prefix, "is_triggered"))

    @is_triggered.setter
    def is_triggered(self, value: bool):
        self.con.hset(self.name_with_prefix, "is_triggered", json.dumps(value))

    @property
    def created_at(self):
        created_at = json.loads(self.con.hget(self.name_with_prefix, "created_at"))
        return datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S.%f")

    @property
    def last_run(self):
        last_run = none_safe_loads(self.con.hget(self.name_with_prefix, "last_run"))
        if last_run is not None:
            return datetime.strptime(last_run, "%Y-%m-%d %H:%M:%S.%f")

    @last_run.setter
    def last_run(self, value):
        self.con.hset(self.name_with_prefix, "last_run", value)

    @property
    def exists(self):
        return self.con.exists(self.name_with_prefix)

    def register(self):
        mapping = {"is_triggered": "", "jobs": "[]", "created_at": str(datetime.now(timezone.utc))}
        self.con.hset(name=self.name_with_prefix, key=None, value=None, mapping=mapping)
        return self

    def add_job(self, job_name):
        if not self.exists:
            self.register()
        jobs = [job.name for job in self.jobs]
        if job_name in jobs:
            raise ValueError(f"Job {job_name} already registered with trigger {self.name}")
        else:
            jobs.append(job_name)
            jobs_str = json.dumps(jobs)
            self.con.hset(name=self.name_with_prefix, key="jobs", value=jobs_str)

    def remove_job(self, job_name):
        jobs = [job.name for job in self.jobs if job.name != job_name]
        jobs_str = json.dumps(jobs)
        self.con.hset(name=self.name_with_prefix, key="jobs", value=jobs_str)
