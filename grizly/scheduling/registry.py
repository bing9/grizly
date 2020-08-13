from datetime import datetime, timezone
import json
import logging
import os
import sys
from time import time
from typing import Any, Dict, List, Union, Literal
from ..config import Config
from croniter import croniter

import dask
from dask.delayed import Delayed
from distributed import Client, Future
from distributed.protocol.serialize import serialize as dask_serialize
from distributed.protocol.serialize import deserialize as dask_deserialize
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
            self.host = self.redis_host

        # dev host = "10.125.68.177"
        con = Redis(host=self.host, port=self.port, db=0)
        return con

    def add_trigger(self, name: str, type: str, value: str):
        Trigger(name=name).register(type=type, value=value)

    def get_trigger_names(self):
        trigger_names = []
        prefix = Trigger.prefix
        for trigger_name_with_prefix in self.con.keys(f"{prefix}*"):
            if trigger_name_with_prefix is not None:
                trigger_name = trigger_name_with_prefix.decode("utf-8")[len(prefix) :]
                trigger_names.append(trigger_name)
        return trigger_names

    def get_triggers(self):
        trigger_names = self.get_trigger_names()
        triggers = []
        for trigger_name in trigger_names:
            triggers.append(Trigger(trigger_name, env=self.env, logger=self.logger))
        return triggers

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
        prefix = Job.prefix
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


class RegistryObject:
    prefix = "grizly:"

    def __init__(self, name: str, env: Literal["dev", "prod"] = "prod", logger: logging.Logger = None, **kwargs):
        self.name = name
        self.name_with_prefix = self.prefix + name
        self.registry = Registry(env=env, **kwargs)
        self.logger = logger or logging.getLogger(__name__)

    def __repr__(self):
        return f"{self.__class__.__name__}(name='{self.name}')"

    @property
    def getall(self):
        return self.con.hgetall(self.name_with_prefix)

    @property
    def con(self):
        con = self.registry.con
        return con

    @property
    def exists(self):
        return self.con.exists(self.name_with_prefix)

    def remove(self):
        self.con.delete(self.name_with_prefix)

    @staticmethod
    def serialize(value: Any) -> str:
        if isinstance(value, datetime):
            value = str(value)

        if isinstance(value, list) and all(isinstance(i, Delayed) for i in value) and value != []:
            value = str(dask_serialize(value))

        return json.dumps(value)

    @staticmethod
    def deserialize(value: Any, type: Union[Literal["datetime", "dask"], None] = None) -> Any:
        if value is None:
            return None
        else:
            value = json.loads(value)
            if value is not None:
                if type == "datetime":
                    value = datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f")
                elif type == "dask":
                    value = dask_deserialize(*eval(value))

            return value


class Job(RegistryObject):
    prefix = "grizly:job:"

    # def __init__(self, *args, **kwargs):
    #     super(Job, self).__init__(*args, **kwargs)

    @property
    def created_at(self) -> datetime:
        return self.deserialize(self.con.hget(self.name_with_prefix, "created_at"), type="datetime")

    @property
    def error(self) -> str:
        return self.deserialize(self.con.hget(self.name_with_prefix, "error"))

    @error.setter
    def error(self, error: str):
        self.con.hset(self.name_with_prefix, "error", self.serialize(error))

    @property
    def graph(self) -> Delayed:
        return dask.delayed()(self.tasks, name=self.name + "_graph")

    @property
    def last_run(self) -> datetime:
        return self.deserialize(self.con.hget(self.name_with_prefix, "last_run"), type="datetime")

    @last_run.setter
    def last_run(self, last_run: datetime):
        self.con.hset(self.name_with_prefix, "last_run", self.serialize(last_run))

    @property
    def owner(self) -> str:
        return self.deserialize(self.con.hget(self.name_with_prefix, "owner"))

    @owner.setter
    def owner(self, owner: str):
        self.con.hset(self.name_with_prefix, "owner", self.serialize(owner))

    @property
    def run_time(self) -> int:
        return self.deserialize(self.con.hget(self.name_with_prefix, "run_time"))

    @run_time.setter
    def run_time(self, run_time: int):
        self.con.hset(self.name_with_prefix, "run_time", self.serialize(run_time))

    @property
    def status(self) -> Literal["fail", "running", "success"]:
        return self.deserialize(self.con.hget(self.name_with_prefix, "status"))

    @status.setter
    def status(self, status: Literal["fail", "running", "success"]):
        self.con.hset(self.name_with_prefix, "status", self.serialize(status))

    @property
    def tasks(self) -> List[Delayed]:
        return self.deserialize(self.con.hget(self.name_with_prefix, "tasks"), type="dask")

    @tasks.setter
    def tasks(self, tasks: List[Delayed]):
        self.con.hset(self.name_with_prefix, "tasks", self.serialize(tasks))

    # TRIGGERS
    @property
    def triggers(self) -> List["Trigger"]:
        triggers = [Trigger(name=trigger_name) for trigger_name in self.trigger_names]
        return triggers

    @property
    def trigger_names(self) -> List[str]:
        return self.deserialize(self.con.hget(self.name_with_prefix, "trigger_names"))

    @trigger_names.setter
    def trigger_names(self, trigger_names: List[str]):
        """Removes job from old trigger and adds to new one"""
        for trigger in self.triggers:
            trigger.remove_job(job_name=self.name)
        for trigger_name in trigger_names:
            Trigger(name=trigger_name).add_job(job_name=self.name)
        self.con.hset(self.name_with_prefix, "trigger_names", self.serialize(trigger_names))

    def add_trigger(self, trigger_name: str):
        Trigger(name=trigger_name).add_job(job_name=self.name)
        trigger_names = self.trigger_names
        trigger_names.append(trigger_name)
        self.con.hset(self.name_with_prefix, "trigger_names", self.serialize(trigger_names))

    # TRIGGERS END

    @property
    def type(self) -> Literal["regular", "system"]:
        return self.deserialize(self.con.hget(self.name_with_prefix, "type"))

    def cancel(self, scheduler_address: str = None):
        if not scheduler_address:
            scheduler_address = self.scheduler_address
        client = Client(scheduler_address)
        f = Future(self.name + "_graph", client=client)
        f.cancel(force=True)
        client.close()

    def register(
        self,
        tasks: List[Delayed],
        owner: str = None,
        trigger_names: List[str] = [],
        type: Literal["regular", "system"] = "regular",
    ) -> "Job":
        mapping = {
            "owner": self.serialize(owner),
            "trigger_names": self.serialize(trigger_names),
            "type": self.serialize(type),
            "last_run": "null",
            "run_time": "null",
            "status": "null",
            "error": "null",
            "created_at": self.serialize(datetime.now(timezone.utc)),
        }
        self.con.hset(name=self.name_with_prefix, key=None, value=None, mapping=mapping)
        for trigger_name in trigger_names:
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
    ) -> Any:

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
            result = self.graph.compute()
            status = "success"
        except Exception:
            result = None
            status = "fail"
            _, exc_value, _ = sys.exc_info()
            self.error = str(exc_value)

        end = time()
        self.run_time = int(end - start)
        self.status = status
        self.last_run = datetime.now(timezone.utc)

        self.logger.info(f"Job {self.name} finished with status {status}")

        client.close()
        return result

    def visualize(self, **kwargs):
        return self.graph.visualize(**kwargs)


class Trigger(RegistryObject):
    prefix = "grizly:trigger:"

    # def __init__(self, *args, **kwargs):
    #     super(Trigger, self).__init__(*args, **kwargs)

    @property
    def created_at(self) -> datetime:
        return self.deserialize(self.con.hget(self.name_with_prefix, "created_at"), type="datetime")

    @property
    def is_triggered(self) -> bool:
        return self.deserialize(self.con.hget(self.name_with_prefix, "is_triggered"))

    @is_triggered.setter
    def is_triggered(self, value: bool):
        self.con.hset(self.name_with_prefix, "is_triggered", self.serialize(value))

    @property
    def jobs(self) -> List[Union["Job", None]]:
        job_names = self.deserialize(self.con.hget(self.name_with_prefix, "jobs"))
        return [Job(name=job) for job in job_names]

    @property
    def last_run(self) -> datetime:
        return self.deserialize(self.con.hget(self.name_with_prefix, "last_run"), type="datetime")

    @last_run.setter
    def last_run(self, value: datetime):
        self.con.hset(self.name_with_prefix, "last_run", self.serialize(value))

    @property
    def next_run(self) -> datetime:
        start_date = self.last_run or self.created_at
        cron_str = self.value
        cron = croniter(cron_str, start_date)
        next_run = cron.get_next(datetime).replace(tzinfo=timezone.utc)
        return next_run

    @property
    def type(self) -> Literal["cron", "listener"]:
        return self.deserialize(self.con.hget(self.name_with_prefix, "type"))

    @property
    def value(self) -> str:
        return self.deserialize(self.con.hget(self.name_with_prefix, "value"))

    @value.setter
    def value(self, value: str):
        self.con.hset(self.name_with_prefix, "value", self.serialize(value))

    def add_job(self, job_name: str):
        if not self.exists:
            raise ValueError(f"Trigger {self.name} does not exist.")
        job_names = [job.name for job in self.jobs]
        if job_name in job_names:
            raise ValueError(f"Job {job_name} already registered with trigger {self.name}")
        else:
            job_names.append(job_name)
            self.con.hset(name=self.name_with_prefix, key="jobs", value=self.serialize(job_names))

    def register(self, type: Literal["cron", "listener"], value: str):
        # VALIDATIONS
        valid_types = ["cron", "listener"]
        if type not in valid_types:
            raise ValueError(f"Invalid value {type} in type. Valid values: {valid_types}")
        if type == "cron" and not croniter.is_valid(value):
            raise ValueError(f"Invalid cron string {value}")

        mapping = {
            "type": self.serialize(type),
            "value": self.serialize(value),
            "is_triggered": "null",
            "jobs": self.serialize([]),
            "created_at": self.serialize(datetime.now(timezone.utc)),
        }
        self.con.hset(name=self.name_with_prefix, key=None, value=None, mapping=mapping)
        return self

    def remove_job(self, job_name: str):
        job_names = [job.name for job in self.jobs if job.name != job_name]
        self.con.hset(name=self.name_with_prefix, key="jobs", value=self.serialize(job_names))
