import logging
import redis
from datetime import datetime

from . import job as _job


class Trigger:
    def __init__(self, name: str, logger: logging.Logger = None):
        self.name = name
        self.con = redis.Redis(host="10.125.68.177", port=80, db=0)
        self.key = self.name
        self.logger = logger or logging.getLogger(__name__)

    def __repr__(self):
        return f"{self.__class__.__name__}(name='{self.name}')"

    @property
    def type(self):
        return self.con.hget(self.key, "type").decode("utf-8")

    @property
    def value(self):
        return self.con.hget(self.key, "value").decode("utf-8")

    @property
    def is_triggered(self):
        is_triggered = self.con.hget(self.key, "is_triggered")
        if is_triggered is not None:
            return int(is_triggered.decode("utf-8"))

    @is_triggered.setter
    def is_triggered(self, value):
        if value == 1 or value == 0:
            self.con.hset(self.name, "is_triggered", value)
        else:
            raise ValueError("Value can only be 1 or 0")

    @property
    def created_at(self):
        created_at = self.con.hget(self.name, "created_at")
        if created_at is not None:
            created_at = created_at.decode("utf-8")
            created_at = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S.%f")
            return created_at

    @property
    def last_run(self):
        last_run = self.con.hget(self.key, "last_run")
        if last_run is not None:
            last_run = last_run.decode("utf-8")
            last_run = datetime.strptime(last_run, "%Y-%m-%d %H:%M:%S.%f")
            return last_run

    @last_run.setter
    def last_run(self, value):
        return self.con.hset(self.key, "last_run", value)

    def register(self, type, value):
        mapping = {"type": type, "value": value, "is_triggered": "", "created_at": datetime.utcnow().__str__()}
        self.con.hset(name=f"{self.name}", key=None, value=None, mapping=mapping)
        return self

    def get_jobs(self):
        jobs = self.con.keys("job*")
        triggered_jobs = []
        for job in jobs:
            job_name = job.decode("utf-8")
            try:
                job_trigger = self.con.hget(job_name, "trigger_name").decode("utf-8")
                if job_trigger == self.key:
                    triggered_jobs.append(_job.Job(name=job_name, logger=self.logger))
            except:
                pass
        return triggered_jobs
