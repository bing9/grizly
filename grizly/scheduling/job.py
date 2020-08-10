from datetime import datetime, timezone
from functools import wraps
import json
import logging
import os
import sys
from time import time
from typing import Any, Dict, List, Literal, Union

from croniter import croniter
import dask
from dask.delayed import Delayed
from distributed import Client, Future
from distributed.protocol.serialize import serialize as dask_serialize
from distributed.protocol.serialize import deserialize as dask_deserialize
from redis import Redis

from ..config import Config
from ..exceptions import JobNotFoundError

config = Config().get_service("scheduling")


def _check_if_exists(raise_error=True):
    """Checks if the job exists in the registry

    Parameters
    ----------
    raise_error : bool, optional
        Whether to raise error if job doesn't exist, by default True

    """

    def deco_wrap(f):
        @wraps(f)
        def wrapped(self, *args, **kwargs):
            if not self.exists:
                if raise_error:
                    raise JobNotFoundError
                else:
                    self.logger.warning("Job not found in the registry")

            return f(self, *args, **kwargs)

        return wrapped

    return deco_wrap


class Job:
    registry_prefix = "grizly:registry:"
    job_runs_prefix = "grizly:job_runs:"

    def __init__(
        self,
        name: str,
        env: Literal["dev", "prod"] = "prod",
        redis_host: Union[str, None] = None,
        redis_port: Union[int, None] = None,
        logger: logging.Logger = None,
        **kwargs,
    ):
        self.name = name
        self.registry_name = self.registry_prefix + name
        self.env = env
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.logger = logger or logging.getLogger(__name__)

    def __eq__(self, other):
        return self.name == other.name

    def __repr__(self):
        return f"{self.__class__.__name__}(name='{self.name}')"

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

    @property
    def created_at(self) -> datetime:
        return self.deserialize(self.con.hget(self.registry_name, "created_at"), type="datetime")

    # @property
    # def error(self) -> str:
    #     return self.deserialize(self.con.hget(self.registry_name, "error"))

    # @error.setter
    # def error(self, error: str):
    #     self.con.hset(self.registry_name, "error", self.serialize(error))

    @property
    def exists(self):
        return self.con.exists(self.registry_name)

    @property
    def getall(self):
        return self.con.hgetall(self.registry_name)

    @property
    def graph(self) -> Delayed:
        return dask.delayed()(self.tasks, name=self.name + "_graph")

    # @property
    # def last_run(self) -> datetime:
    #     return self.deserialize(self.con.hget(self.registry_name, "last_run"), type="datetime")

    # @last_run.setter
    # def last_run(self, last_run: datetime):
    #     self.con.hset(self.registry_name, "last_run", self.serialize(last_run))

    @property
    def owner(self) -> str:
        return self.deserialize(self.con.hget(self.registry_name, "owner"))

    @owner.setter
    @_check_if_exists()
    def owner(self, owner: str):
        self.con.hset(self.registry_name, "owner", self.serialize(owner))

    # @property
    # def run_time(self) -> int:
    #     return self.deserialize(self.con.hget(self.name_with_prefix, "run_time"))

    # @run_time.setter
    # def run_time(self, run_time: int):
    #     self.con.hset(self.name_with_prefix, "run_time", self.serialize(run_time))

    # @property
    # def status(self) -> Literal["fail", "running", "success"]:
    #     return self.deserialize(self.con.hget(self.name_with_prefix, "status"))

    # @status.setter
    # def status(self, status: Literal["fail", "running", "success"]):
    #     self.con.hset(self.name_with_prefix, "status", self.serialize(status))

    @property
    def tasks(self) -> List[Delayed]:
        return self.deserialize(self.con.hget(self.registry_name, "tasks"), type="dask")

    @tasks.setter
    @_check_if_exists()
    def tasks(self, tasks: List[Delayed]):
        self.con.hset(self.registry_name, "tasks", self.serialize(tasks))

    # DOWNSTREAM/UPSTREAM

    @property
    def downstream(self) -> List["Job"]:
        downstream_job_names = self.deserialize(self.con.hget(self.registry_name, "downstream"))
        downstream_jobs = [Job(job_name) for job_name in downstream_job_names]
        return downstream_jobs

    @downstream.setter
    @_check_if_exists()
    def downstream(self, new_job_names: List[str]):
        """
        Overwrite the list of downstream jobs.
        """
        # 1. Remove from downstream jobs of all the jobs on the previous upstream jobs list
        old_downstream_jobs = self.downstream
        for downstream_job in old_downstream_jobs:
            downstream_job.remove_upstream_jobs(self.name)
        # 2. Add as a downstream job to the jobs in new_job_names
        for new_downstream_job_name in new_job_names:
            new_downstream_job = Job(new_downstream_job_name)
            new_downstream_job.add_upstream_jobs(self.name)
        # 3. Update upstream jobs with the new job
        self.con.hset(self.registry_name, "upstream", self.serialize(new_job_names))

    @_check_if_exists()
    def add_downstream_jobs(self, job_names: Union[List[str], str]):
        """Add downstream jobs

        Parameters
        ----------
        job_names : str or list
            names of the downstream jobs to add
        """

        if isinstance(job_names, str):
            job_names = [job_names]

        # build the new list
        downstream_job_names = self.deserialize(self.con.hget(self.registry_name, "downstream"))
        new_downstream_job_names = downstream_job_names + job_names

        # update Redis
        self.logger.info(f"Adding downstream jobs: {job_names}...")
        self.con.hset(name=self.registry_name, key="downstream", value=self.serialize(new_downstream_job_names))

        # add the job as an upstream of the specified jobs
        for downstream_job_name in job_names:
            downstream_job = Job(downstream_job_name)
            if not downstream_job.exists:
                raise ValueError(f"Job {downstream_job_name} does not exist")
            if self not in downstream_job.upstream:
                downstream_job.add_upstream_jobs(self.name)

    @_check_if_exists()
    def remove_downstream_jobs(self, job_names: Union[str, List[str]]):
        self.logger.info(f"Removing downstream jobs: {job_names}...")

        if isinstance(job_names, str):
            job_names = [job_names]

        # remove the job as an upstream of the specified jobs
        for downstream_job_name in job_names:
            downstream_job = Job(downstream_job_name)
            downstream_job.remove_upstream_jobs(self.name)

        # build the new list
        downstream_job_names = self.deserialize(self.con.hget(self.registry_name, "downstream"))
        for job_name in job_names:
            try:
                downstream_job_names.remove(job_name)
            except ValueError:
                self.logger.warning(f"Job {job_name} was not found in {self.name}'s downstream jobs'")

        # update Redis
        self.con.hset(name=self.registry_name, key="downstream", value=self.serialize(downstream_job_names))

    @property
    def upstream(self) -> List["Job"]:
        upstream_job_names = self.deserialize(self.con.hget(self.registry_name, "upstream"))
        upstream_jobs = [Job(job_name) for job_name in upstream_job_names]
        return upstream_jobs

    @upstream.setter
    @_check_if_exists()
    def upstream(self, new_job_names: List[str]):
        """
        Overwrite the list of upstream jobs.
        """
        # 1. Remove from downstream jobs of all the jobs on the previous upstream jobs list
        old_upstream_jobs = self.upstream
        for upstream_job in old_upstream_jobs:
            upstream_job.remove_downstream_jobs(self.name)
        # 2. Add as a downstream job to the jobs in new_job_names
        for new_upstream_job_name in new_job_names:
            new_upstream_job = Job(new_upstream_job_name)
            new_upstream_job.add_downstream_jobs(self.name)
        # 3. Update upstream jobs with the new job
        self.con.hset(self.registry_name, "upstream", self.serialize(new_job_names))

    @_check_if_exists()
    def add_upstream_jobs(self, job_names: Union[List[str], str]):
        """Add upstream jobs

        Parameters
        ----------
        job_names : str or list
            names of the upstream jobs to add
        """

        if isinstance(job_names, str):
            job_names = [job_names]

        # build the new list
        prev_upstream_job_names = self.deserialize(self.con.hget(self.registry_name, "upstream"))
        new_upstream_job_names = prev_upstream_job_names + job_names

        # update Redis
        self.logger.info(f"Adding upstream jobs: {job_names}...")
        self.con.hset(self.registry_name, "upstream", self.serialize(new_upstream_job_names))

        # add the job as a downstream of the specified jobs
        for upstream_job_name in job_names:
            upstream_job = Job(upstream_job_name)
            if not upstream_job.exists:
                raise ValueError(f"Job {upstream_job_name} does not exist")
            if self not in upstream_job.downstream:
                upstream_job.add_downstream_jobs(self.name)

    @_check_if_exists()
    def remove_upstream_jobs(self, job_names: Union[str, List[str]]):
        self.logger.info(f"Removing upstream jobs: {job_names}...")
        if isinstance(job_names, str):
            job_names = [job_names]

        # remove the job from the downstream jobs of the specified jobs
        for upstream_job_name in job_names:
            upstream_job = Job(upstream_job_name)
            upstream_job.remove_downstream_jobs(self.name)

        # build the new list
        upstream_job_names = self.deserialize(self.con.hget(self.registry_name, "upstream"))
        for job_name in job_names:
            try:
                upstream_job_names.remove(job_name)
            except ValueError:
                self.logger.warning(f"Job {job_name} was not found in {self.name}'s upstream jobs'")

        # update Redis
        self.con.hset(name=self.registry_name, key="upstream", value=self.serialize(upstream_job_names))

    # DOWNSTREAM/UPSTREAM END

    def cancel(self, scheduler_address: Union[str, None] = None):
        if not scheduler_address:
            scheduler_address = self.scheduler_address
        client = Client(scheduler_address)
        f = Future(self.name + "_graph", client=client)
        f.cancel(force=True)
        client.close()

    def register(
        self,
        tasks: List[Delayed],
        cron: Union[str, None] = None,
        owner: Union[str, None] = None,
        upstream: List[str] = [],
        if_exists: Literal["fail", "replace"] = "fail",
    ) -> "Job":
        if if_exists == "fail" and self.exists:
            raise ValueError(f"Job {self.name} already exists")
        if self.name in upstream:
            raise ValueError(f"Job cannot be its own upstream job !!!")

        mapping = {
            "owner": self.serialize(owner),
            "upstream": self.serialize(upstream),
            "downstream": self.serialize([]),
            # "last_run": "null",
            # "run_time": "null",
            # "status": "null",
            # "error": "null",
            "tasks": self.serialize(tasks),
            "created_at": self.serialize(datetime.now(timezone.utc)),
        }
        if not (cron or upstream):
            raise ValueError("One of ['cron', 'upstream'] is required")

        self.con.hset(name=self.registry_name, key=None, value=None, mapping=mapping)
        # add the job as downstream in all upstream jobs
        for upstream_job_name in upstream:
            upstream_job = Job(name=upstream_job_name)
            upstream_job.add_downstream_jobs(self.name)
        return self

    @_check_if_exists(raise_error=False)
    def remove(self):
        self.con.delete(self.registry_name)

    @_check_if_exists()
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

    @_check_if_exists()
    def visualize(self, **kwargs):
        return self.graph.visualize(**kwargs)

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
