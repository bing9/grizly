from datetime import datetime, timezone
import json
import logging
import os
import sys
from time import time
from typing import Any, Dict, List

import dask
from dask.delayed import Delayed
from distributed import Client, Future
from distributed.protocol.serialize import serialize as dask_serialize
from distributed.protocol.serialize import deserialize as dask_deserialize
from redis import Redis

from ..config import Config


class Registry:
    def __init__(self, logger: logging.Logger = None):
        self.logger = logger or logging.getLogger(__name__)

    @property
    def con(self):
        schedule = Config().get_service("schedule")
        con = Redis(host=schedule["redis_host"], port=schedule["redis_port"], db=0)
        return con

    def get_triggers(self):
        triggers = []
        for trigger_name_with_prefix in self.con.keys("grizly:trigger:*"):
            if trigger_name_with_prefix is not None:
                trigger_name = trigger_name_with_prefix.decode("utf-8").split(":")[-1]
                triggers.append(Trigger(trigger_name, logger=self.logger))
        return triggers

    def get_jobs(self):
        jobs = []
        for job_name_with_prefix in self.con.keys("grizly:job:*"):
            if job_name_with_prefix is not None:
                job_name = job_name_with_prefix.decode("utf-8").split(":")[-1]
                jobs.append(Job(job_name, logger=self.logger))
        return jobs

    def add_trigger(self, name: str, type: str, value: str):
        Trigger(name=name).register(type=type, value=value)

    def add_job(self, name: str, trigger_name: str, type: str, value: str):
        Trigger(name=name).register(type=type, value=value)


class RegistryObject:
    prefix = "grizly:"

    def __init__(
        self, name: str, logger: logging.Logger = None,
    ):
        self.name = name
        self.name_with_prefix = self.prefix + name
        self.logger = logger or logging.getLogger(__name__)

    def __repr__(self):
        return f"{self.__class__.__name__}(name='{self.name}')"

    @property
    def getall(self):
        return self.con.hgetall(self.name_with_prefix)

    @property
    def con(self):
        schedule = Config().get_service("schedule")
        con = Redis(host=schedule["redis_host"], port=schedule["redis_port"], db=0)
        return con

    @property
    def exists(self):
        return self.con.exists(self.name_with_prefix)

    @staticmethod
    def serialize(value):
        if isinstance(value, datetime):
            value = str(value)

        if isinstance(value, list) and all(isinstance(i, Delayed) for i in value):
            value = str(dask_serialize(value))

        return json.dumps(value)

    @staticmethod
    def deserialize(value, type: str = None) -> Any:
        if value is None:
            return None
        else:
            value = json.loads(value)
            if type == "datetime":
                value = datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f")
            elif type == "dask":
                value = dask_deserialize(*eval(value))

            return value

class Trigger(RegistryObject):
    prefix = "grizly:trigger:"

    @property
    def type(self):
        return self.deserialize(self.con.hget(self.name_with_prefix, "type"))

    @property
    def value(self):
        return self.deserialize(self.con.hget(self.name_with_prefix, "value"))

    @value.setter
    def value(self, value):
        self.con.hset(self.name_with_prefix, "value", self.serialize(value))

    @property
    def jobs(self):
        job_names = self.deserialize(self.con.hget(self.name_with_prefix, "jobs"))
        return [Job(name=job) for job in job_names]

    @property
    def is_triggered(self):
        return self.deserialize(self.con.hget(self.name_with_prefix, "is_triggered"))

    @is_triggered.setter
    def is_triggered(self, value: bool):
        self.con.hset(self.name_with_prefix, "is_triggered", self.serialize(value))

    @property
    def created_at(self):
        return self.deserialize(self.con.hget(self.name_with_prefix, "created_at"), type="datetime")

    @property
    def last_run(self):
        return self.deserialize(self.con.hget(self.name_with_prefix, "last_run"), type="datetime")

    @last_run.setter
    def last_run(self, value):
        self.con.hset(self.name_with_prefix, "last_run", self.serialize(value))

    def register(self, type: str, value: str):
        mapping = {
            "type": self.serialize(type),
            "value": self.serialize(value),
            "is_triggered": "null",
            "jobs": self.serialize([]),
            "created_at": self.serialize(datetime.now(timezone.utc)),
        }
        self.con.hset(name=self.name_with_prefix, key=None, value=None, mapping=mapping)
        return self

    def add_job(self, job_name):
        if not self.exists:
            raise ValueError(f"Trigger {self.name} does not exist.")
        job_names = [job.name for job in self.jobs]
        if job_name in job_names:
            raise ValueError(f"Job {job_name} already registered with trigger {self.name}")
        else:
            job_names.append(job_name)
            self.con.hset(name=self.name_with_prefix, key="jobs", value=self.serialize(job_names))

    def remove_job(self, job_name):
        job_names = [job.name for job in self.jobs if job.name != job_name]
        self.con.hset(name=self.name_with_prefix, key="jobs", value=self.serialize(job_names))


class Job(RegistryObject):
    prefix = "grizly:job:"

    @property
    def owner(self):
        return self.deserialize(self.con.hget(self.name_with_prefix, "owner"))

    @owner.setter
    def owner(self, value):
        self.con.hset(self.name_with_prefix, "owner", self.serialize(value))

    @property
    def trigger_name(self):
        return self.deserialize(self.con.hget(self.name_with_prefix, "trigger_name"))

    @trigger_name.setter
    def trigger_name(self, value):
        """Removes job from old trigger and adds to new one"""
        self.trigger.remove_job(job_name=value)
        Trigger(name=value).add_job(job_name=self.name)
        self.con.hset(self.name_with_prefix, "trigger_name", self.serialize(value))

    @property
    def type(self):
        return self.deserialize(self.con.hget(self.name_with_prefix, "type"))

    @property
    def last_run(self):
        return self.deserialize(self.con.hget(self.name_with_prefix, "last_run"), type="datetime")

    @last_run.setter
    def last_run(self, value):
        self.con.hset(self.name_with_prefix, "last_run", self.serialize(value))

    @property
    def run_time(self):
        return self.deserialize(self.con.hget(self.name_with_prefix, "run_time"))

    @run_time.setter
    def run_time(self, value):
        self.con.hset(self.name_with_prefix, "run_time", self.serialize(value))

    @property
    def status(self):
        return self.deserialize(self.con.hget(self.name_with_prefix, "status"))

    @status.setter
    def status(self, value):
        self.con.hset(self.name_with_prefix, "status", self.serialize(value))

    @property
    def error(self):
        return self.deserialize(self.con.hget(self.name_with_prefix, "error"))

    @error.setter
    def error(self, value):
        self.con.hset(self.name_with_prefix, "error", self.serialize(value))

    @property
    def created_at(self):
        return self.deserialize(self.con.hget(self.name_with_prefix, "created_at"), type="datetime")

    @property
    def tasks(self):
        return self.deserialize(self.con.hget(self.name_with_prefix, "tasks"), type="dask")

    @tasks.setter
    def tasks(self, tasks):
        self.con.hset(self.name_with_prefix, "tasks", self.serialize(tasks))

    @property
    def trigger(self) -> Trigger:
        if self.trigger_name is not None:
            return Trigger(name=self.trigger_name)

    @property
    def graph(self):
        return dask.delayed()(self.tasks, name=self.name + "_graph")

    def visualize(self, **kwargs):
        return self.graph.visualize(**kwargs)

    def register(
        self, owner: str = None, trigger_name: str = None, tasks: List[dask.delayed] = None, type: str = "regular"
    ):
        mapping = {
            "owner": self.serialize(owner),
            "trigger_name": self.serialize(trigger_name),
            "type": self.serialize(type),
            "last_run": "null",
            "run_time": "null",
            "status": "null",
            "error": "null",
            "created_at": self.serialize(datetime.now(timezone.utc)),
        }
        self.con.hset(name=self.name_with_prefix, key=None, value=None, mapping=mapping)
        if trigger_name is not None:
            Trigger(name=trigger_name).add_job(job_name=self.name)
        tasks = tasks or self.tasks
        if tasks is None:
            raise ValueError("Please specify tasks.")
        self.tasks = tasks
        return self

    def submit(
        self,
        client: Client = None,
        scheduler_address: str = None,
        priority: int = None,
        resources: Dict[str, Any] = None,
        to_dask=True,
    ) -> None:

        priority = priority or 1
        if to_dask:
            if not client:
                self.scheduler_address = scheduler_address or os.getenv("GRIZLY_DEV_DASK_SCHEDULER_ADDRESS")
                client = Client(self.scheduler_address)
            else:
                self.scheduler_address = client.scheduler.address

            if not client and not self.scheduler_address:
                raise ValueError("distributed.Client/scheduler address was not provided")

        self.logger.info(f"Submitting job {self.name}...")
        self.status = "running"
        self.last_run = datetime.now(timezone.utc)
        self.error = ""

        start = time()
        try:
            self.graph.compute()
            status = "success"
        except Exception:
            status = "fail"
            _, exc_value, _ = sys.exc_info()
            self.error = str(exc_value)

        end = time()
        self.run_time = int(end - start)
        self.status = status
        self.last_run = datetime.now(timezone.utc)

        self.logger.info(f"Job {self.name} finished with status {status}")

        client.close()

    def cancel(self, scheduler_address=None):
        if not scheduler_address:
            scheduler_address = self.scheduler_address
        client = Client(scheduler_address)
        f = Future(self.name + "_graph", client=client)
        f.cancel(force=True)
        client.close()


