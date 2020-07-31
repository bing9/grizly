import logging
import redis
from datetime import datetime

from . import job as _job


class Trigger:
    def __init__(self, name: str, logger: logging.Logger = None):
        self.name = name
        self.con = redis.Redis(host="10.125.68.177", port=80, db=0)
        self.key = "trigger " + self.name
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
        return self.con.hget(self.key, "is_triggered").decode("utf-8")

    @property
    def created_at(self):
        return self.con.hget(self.key, "created_at").decode("utf-8")

    def register(self, type, value):
        mapping = {"type": type, "value": value, "is_triggered": "", "created_at": datetime.utcnow().__str__()}
        self.con.hset(name=f"trigger {self.name}", key=None, value=None, mapping=mapping)
        return self

    def trigger(self):
        self.con.hset(self.key, "is_triggered", 1)
        return self

    def get_jobs(self):
        jobs = self.con.keys("job*")
        triggered_jobs = []
        for job in jobs:
            job_key = job.decode("utf-8")
            try:
                job_trigger = self.con.hget(job_key, "trigger").decode("utf-8")
                if job_trigger == self.key:
                    job_name = job_key.split(" ")[1]
                    triggered_jobs.append(_job.Job(name=job_name))
            except:
                pass
        return triggered_jobs
