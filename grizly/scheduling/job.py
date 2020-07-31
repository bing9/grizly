from datetime import datetime
import json
import logging
import os
import sys
from time import time
from typing import Any, Dict

import dask
from distributed import Client, Future
from redis import Redis

from . import trigger as _trigger
from ..tools.s3 import S3
from ..utils import get_path


class Job:
    def __init__(
        self, name: str, logger: logging.Logger = None,
    ):
        self.name = name
        self.con = Redis(host="10.125.68.177", port=80, db=0)
        self.key = f"job {self.name}"
        self.logger = logger or logging.getLogger(__name__)

    def __repr__(self):
        return f"{self.__class__.__name__}(name='{self.name}')"

    @property
    def trigger_name(self):
        return self.con.hget(self.key, "trigger_name").decode("utf-8")

    @property
    def type(self):
        return self.con.hget(self.key, "type").decode("utf-8")

    @property
    def inputs(self):
        return json.loads(self.con.hget(self.key, "inputs").decode("utf-8"))

    @property
    def last_run(self):
        return self.con.hget(self.key, "last_run").decode("utf-8")

    @last_run.setter
    def last_run(self, value):
        return self.con.hset(self.key, "last_run", value)

    @property
    def run_time(self):
        return self.con.hget(self.key, "run_time").decode("utf-8")

    @run_time.setter
    def run_time(self, value):
        return self.con.hset(self.key, "run_time", value)

    @property
    def status(self):
        return self.con.hget(self.key, "status").decode("utf-8")

    @status.setter
    def status(self, value):
        return self.con.hset(self.key, "status", value)

    @property
    def error(self):
        return self.con.hget(self.key, "error").decode("utf-8")

    @error.setter
    def error(self, value):
        return self.con.hset(self.key, "error", value)

    @property
    def created_at(self):
        return self.con.hget(self.key, "created_at").decode("utf-8")

    @property
    def trigger(self) -> _trigger.Trigger:
        return _trigger.Trigger(name=self.trigger_name)

    @property
    def source_type(self):
        if self.inputs["artifact"]["main"].lower().startswith("https://github.com"):
            return "github"
        elif self.inputs["artifact"]["main"].lower().startswith("s3://"):
            return "s3"
        else:
            raise NotImplementedError(f"""Source {self.inputs["artifact"]["main"]} not supported""")

    @property
    def tasks(self):
        GRIZLY_WORKFLOWS_HOME = os.getenv("GRIZLY_WORKFLOWS_HOME") or get_path()
        sys.path.insert(0, GRIZLY_WORKFLOWS_HOME)
        file_dir = os.path.join(GRIZLY_WORKFLOWS_HOME, "tmp")

        def _download_script_from_s3(url, file_dir):
            # TODO: This should load script to the memory not download it
            bucket = url.split("/")[2]
            file_name = url.split("/")[-1]
            s3_key = "/".join(url.split("/")[3:-1])
            s3 = S3(bucket=bucket, file_name=file_name, s3_key=s3_key, file_dir=file_dir)
            s3.to_file()

            return s3.file_name

        if self.source_type == "s3":
            file_name = _download_script_from_s3(url=self.inputs["artifact"]["main"], file_dir=file_dir)
            module = __import__("tmp." + file_name[:-3], fromlist=[None])
            try:
                tasks = module.tasks
            except AttributeError:
                raise AttributeError("Please specify tasks in your script")

            # os.remove(file_name)
            return tasks
        else:
            raise NotImplementedError()

    @property
    def graph(self):
        return dask.delayed()(self.tasks, name=self.name + "_graph")

    def visualize(self, **kwargs):
        return self.graph.visualize(**kwargs)

    def register(self, trigger: _trigger.Trigger, type: str, inputs: Dict[str, Any] = None):
        mapping = {
            "trigger_name": trigger.name,
            "type": type,
            "inputs": json.dumps(inputs),
            "last_run": "",
            "run_time": "",
            "status": "",
            "error": "",
            "created_at": datetime.utcnow().__str__(),
        }
        self.con.hset(name=self.key, key=None, value=None, mapping=mapping)
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

        self.logger.info(f"Job {self.name} finished with status {status}")

        client.close()

    def cancel(self, scheduler_address=None):
        if not scheduler_address:
            scheduler_address = self.scheduler_address
        client = Client(scheduler_address)
        f = Future(self.name + "_graph", client=client)
        f.cancel(force=True)
        client.close()
