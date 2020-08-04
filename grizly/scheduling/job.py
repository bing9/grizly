from datetime import datetime, timezone
import json
import logging
import os
import sys
from time import time
from typing import Any, Dict, List

import dask
from distributed import Client, Future
from distributed.protocol.serialize import deserialize, serialize
from redis import Redis

from . import trigger as _trigger
from ..utils import none_safe_loads


class Job:
    prefix = "grizly:job:"

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
        con = Redis(host="10.125.68.177", port=80, db=0)
        return con

    @property
    def owner(self):
        return none_safe_loads(self.con.hget(self.name_with_prefix, "owner"))

    @owner.setter
    def owner(self, value):
        self.con.hset(self.name_with_prefix, "owner", value)

    @property
    def trigger_name(self):
        return none_safe_loads(self.con.hget(self.name_with_prefix, "trigger_name"))

    @trigger_name.setter
    def trigger_name(self, value):
        """Removes job from old trigger and adds to new one"""
        self.trigger.remove_job(job_name=value)
        _trigger.Trigger(name=value).add_job(job_name=self.name)
        self.con.hset(self.name_with_prefix, "trigger_name", value)

    @property
    def type(self):
        return none_safe_loads(self.con.hget(self.name_with_prefix, "type"))

    @property
    def last_run(self):
        last_run = none_safe_loads(self.con.hget(self.name_with_prefix, "last_run"))
        if last_run is not None:
            return datetime.strptime(last_run, "%Y-%m-%d %H:%M:%S.%f")

    @last_run.setter
    def last_run(self, value):
        self.con.hset(self.name_with_prefix, "last_run", value)

    @property
    def run_time(self):
        return none_safe_loads(self.con.hget(self.name_with_prefix, "run_time"))

    @run_time.setter
    def run_time(self, value):
        self.con.hset(self.name_with_prefix, "run_time", value)

    @property
    def status(self):
        return none_safe_loads(self.con.hget(self.name_with_prefix, "status"))

    @status.setter
    def status(self, value):
        self.con.hset(self.name_with_prefix, "status", value)

    @property
    def error(self):
        return none_safe_loads(self.con.hget(self.name_with_prefix, "error"))

    @error.setter
    def error(self, value):
        self.con.hset(self.name_with_prefix, "error", value)

    @property
    def created_at(self):
        return none_safe_loads(self.con.hget(self.name_with_prefix, "created_at"))

    @property
    def tasks(self):
        header = json.loads(self.con.hget(self.name_with_prefix, "header"))
        frames = [self.con.hget(self.name_with_prefix, "frames")]
        tasks = deserialize(header, frames)
        return tasks

    @tasks.setter
    def tasks(self, tasks):
        serialized = serialize(tasks)
        header = json.dumps(serialized[0])
        self.con.hset(self.name_with_prefix, "header", header)
        self.con.hset(self.name_with_prefix, "frames", serialized[1][0])

    @property
    def trigger(self) -> _trigger.Trigger:
        if self.trigger_name is not None:
            return _trigger.Trigger(name=self.trigger_name)

    @property
    def graph(self):
        return dask.delayed()(self.tasks, name=self.name + "_graph")

    def visualize(self, **kwargs):
        return self.graph.visualize(**kwargs)

    def register(
        self, owner: str = None, trigger_name: str = None, tasks: List[dask.delayed] = None, type: str = "regular"
    ):
        mapping = {
            "owner": owner or "",
            "trigger_name": trigger_name or "",
            "type": type,
            "last_run": "",
            "run_time": "",
            "status": "",
            "error": "",
            "created_at": str(datetime.utcnow()),
        }
        self.con.hset(name=self.name_with_prefix, key=None, value=None, mapping=mapping)
        if trigger_name is not None:
            _trigger.Trigger(name=trigger_name).add_job(job_name=self.name)
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
    ) -> None:

        priority = priority or 1
        if not client:
            self.scheduler_address = scheduler_address or os.getenv("GRIZLY_DEV_DASK_SCHEDULER_ADDRESS")
            client = Client(self.scheduler_address)
        else:
            self.scheduler_address = client.scheduler.address

        if not client and not self.scheduler_address:
            raise ValueError("distributed.Client/scheduler address was not provided")

        self.logger.info(f"Submitting job {self.name}...")
        self.status = "running"
        self.last_run = datetime.utcnow().__str__()
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
        self.last_run = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")

        self.logger.info(f"Job {self.name} finished with status {status}")

        client.close()

    def cancel(self, scheduler_address=None):
        if not scheduler_address:
            scheduler_address = self.scheduler_address
        client = Client(scheduler_address)
        f = Future(self.name + "_graph", client=client)
        f.cancel(force=True)
        client.close()

