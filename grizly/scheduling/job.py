from distributed import Client, Future, progress
import dask
import logging
from typing import Any, Dict, List, Literal
import os
import sys
from time import time
import traceback

from ..tools.sqldb import SQLDB
from ..tools.s3 import S3
from ..utils import get_path
from .tables import JobRegistryTable


class Job:
    def __init__(
        self,
        name: str,
        owner: str,
        tasks: List[dask.delayed] = None,
        source: Dict[str, Any] = None,
        type: Literal["JOB", "SCHEDULE", "LISTENER"] = None,
        trigger: Dict[str, Any] = None,
        notification: Dict[str, Any] = None,
        env: str = None,
        logger: logging.Logger = None,
    ):
        self.name = name
        self.owner = owner
        self.source = source
        self.type = type
        self.tasks = tasks or self._get_tasks()
        self.graph = dask.delayed()(self.tasks, name=self.name + "_graph")
        self.trigger = trigger
        self.notification = notification
        self.env = env or "local"
        self.logger = logger or logging.getLogger(__name__)

    @property
    def source_type(self):
        if self.source["main"].lower().startswith("https://github.com"):
            return "github"
        elif self.source["main"].lower().startswith("s3://"):
            return "s3"
        else:
            raise NotImplementedError(f"Source {self.source} not supported")

    def __repr__(self):
        pass

    def register(self, **kwargs):
        JobRegistryTable(logger=self.logger, **kwargs).register(job=self)

    def visualize(self, **kwargs):
        return self.graph.visualize(**kwargs)

    def submit(
        self,
        client: Client = None,
        scheduler_address: str = None,
        priority: int = None,
        resources: Dict[str, Any] = None,
    ) -> None:

        priority = priority or 1
        if not client:
            client = Client(scheduler_address)

        self.scheduler_address = client.scheduler.address

        # computation = client.compute(self.graph, retries=3, priority=priority, resources=resources)
        # progress(computation)
        # dask.distributed.fire_and_forget(computation)
        self.logger.info(f"Submitting job {self.name}...")
        # computation = client.compute(self.graph)
        # progress(computation)
        start = time()
        # dask.diagnostics .ProgressBar()
        try:
            self.graph.compute()
            self.logger.info(f"Job {self.name} finished with status 'success'")
        except Exception as e:
            exc_type, exc_value, exc_tb = sys.exc_info()
            error_value = str(exc_value)
            error_type = type(exc_value)
            error_message = traceback.format_exc()
            self.logger.exception(f"Job {self.name} finished with status 'fail'")

        end = time()
        run_time = int(end - start)
        self.logger.info(f"Job {self.name} run time {str(run_time)}")

        if not client:  # if cient is provided, we assume the user will close it
            client.close()

    def cancel(self, scheduler_address=None):
        if not scheduler_address:
            scheduler_address = self.scheduler_address
        client = Client(scheduler_address)
        f = Future(self.name + "_graph", client=client)
        f.cancel(force=True)
        client.close()

    def _get_tasks(self):
        GRIZLY_WORKFLOWS_HOME = os.getenv("GRIZLY_WORKFLOWS_HOME") or get_path()
        sys.path.insert(0, GRIZLY_WORKFLOWS_HOME)
        file_dir = os.path.join(GRIZLY_WORKFLOWS_HOME, "tmp")

        def _download_script_from_s3(url, file_dir):
            bucket = url.split("/")[2]
            file_name = url.split("/")[-1]
            s3_key = "/".join(url.split("/")[3:-1])
            s3 = S3(bucket=bucket, file_name=file_name, s3_key=s3_key, file_dir=file_dir)
            s3.to_file()

            return s3.file_name

        if self.source_type == "s3":
            file_name = _download_script_from_s3(url=self.source["main"], file_dir=file_dir)
            module = __import__("tmp." + file_name[:-3], fromlist=[None])
            try:
                tasks = module.tasks
            except AttributeError:
                raise AttributeError("Please specify tasks in your script")

            # os.remove(file_name)
            return tasks
        else:
            raise NotImplementedError()
