from abc import ABC, abstractmethod
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
from rq import Queue
from rq_scheduler import Scheduler

from ..config import Config
from ..exceptions import JobNotFoundError, JobRunNotFoundError


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
                    self.logger.warning(f"{self.hash_name} not found in the registry")

            return f(self, *args, **kwargs)

        return wrapped

    return deco_wrap


class RedisDB:
    submit_queue_name = "submit"
    system_queue_name = "system"

    def __init__(
        self, redis_host: str = None, redis_port: int = None, logger: logging.Logger = None,
    ):
        self.logger = logger or logging.getLogger(__name__)
        self.config = Config().get_service("scheduling")
        self.redis_host = (
            redis_host
            or os.getenv("GRIZLY_REDIS_HOST")
            or self.config.get("redis_host")
            or "localhost"
        )
        self.redis_port = (
            redis_port or os.getenv("GRIZLY_REDIS_PORT") or self.config.get("redis_port") or 6379
        )

    @property
    def con(self):
        con = Redis(host=self.redis_host, port=self.redis_port, db=0)
        return con

    def add_trigger(self, name: str, type: str, value: str):
        Trigger(name=name).register()

    def get_triggers(self) -> List["Trigger"]:
        triggers = []
        prefix = Trigger.prefix
        for trigger_name_with_prefix in self.con.keys(f"{prefix}*"):
            trigger_name = trigger_name_with_prefix.decode("utf-8")[len(prefix) :]
            triggers.append(Trigger(trigger_name, logger=self.logger,))
        return triggers

    def add_job(
        self,
        name: str,
        owner: str = None,
        trigger_names: list = [],
        tasks: List[dask.delayed] = None,
    ):
        Job(name=name).register(owner=owner, tasks=tasks)

    def get_jobs(self) -> List["Job"]:
        jobs = []
        prefix = Job.prefix
        job_hash_names = [val.decode("utf-8") for val in self.con.keys(f"{prefix}*")]
        for job_hash_name in job_hash_names:
            job_name = job_hash_name[len(prefix) :]
            jobs.append(Job(job_name, logger=self.logger,))
        return jobs

    def get_job_runs(self, job_name: Union[str, None] = None) -> List["JobRun"]:
        job_runs = []

        if job_name is not None:
            prefix = f"{JobRun.prefix}:{job_name}"
            job_run_hash_names = [val.decode("utf-8") for val in self.con.keys(f"{prefix}*")]
            for job_run_hash_name in job_run_hash_names:
                job_run_id = job_run_hash_name[len(f"{prefix}") :]
                job_runs.append(JobRun(job_name, job_run_id, logger=self.logger,))
        else:
            jobs = self.get_jobs()
            for job in jobs:
                job_runs.extend(job.runs)
        return job_runs

    def _check_if_jobs_exist(
        self, job_names: Union[List[str], str],
    ):
        self._check_if_exists(values=job_names, object_type="job")

    def _check_if_exists(
        self, values: Union[List[str], str], object_type: Literal["job"] = "job",
    ):
        """Iterate through list of objects and check if they exist - if not raise error"""
        if isinstance(values, str):
            values = [values]
        if object_type == "job":
            for job_name in values:
                job = Job(job_name)
                if not job.exists:
                    raise JobNotFoundError


class RedisObject(ABC):
    prefix = "grizly:"

    def __init__(
        self, name: Union[str, None], logger: logging.Logger = None, **kwargs,
    ):
        self.name = name or ""
        self.hash_name = self.prefix + self.name
        self.db = RedisDB(**kwargs)
        self.logger = logger or logging.getLogger(__name__)

    def __eq__(self, other):
        return self.hash_name == other.hash_name

    def __repr__(self):
        return f"{self.__class__.__name__}(name='{self.name}')"

    @abstractmethod
    def info(self):
        pass

    @property
    def created_at(self) -> datetime:
        return self.deserialize(self.con.hget(self.hash_name, "created_at"), type="datetime",)

    @property
    def con(self):
        con = self.db.con
        return con

    @property
    def exists(self):
        return self.con.exists(self.hash_name)

    def getall(self):  # to be removed
        return self.con.hgetall(self.hash_name)

    def remove(self):
        self.con.delete(self.hash_name)

    @staticmethod
    def serialize(value: Any) -> str:
        if isinstance(value, datetime):
            value = str(value)

        if isinstance(value, list) and all(isinstance(i, Delayed) for i in value) and value != []:
            value = str(dask_serialize(value))

        return json.dumps(value)

    @staticmethod
    def deserialize(value: Any, type: Union[Literal["datetime", "dask"], None] = None,) -> Any:
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

    def _add_values(self, key: str, new_values: Union[List[str], str]):
        if isinstance(new_values, str):
            new_values = [new_values]

        # remove duplicates
        new_values = list(set(new_values))

        # load existing values
        out_values = self.deserialize(self.con.hget(name=self.hash_name, key=key))
        added_values = []

        # append existing values
        for new_value in new_values:
            if new_value in out_values:
                self.logger.warning(
                    f"{new_value} already exists in" f" the list of {self.name}'s {key}"
                )
            else:
                out_values.append(new_value)
                added_values.append(new_value)

        # update Redis
        if added_values:
            self.logger.info(f"Adding {added_values} to {key}...")
            self.con.hset(
                name=self.hash_name, key=key, value=self.serialize(out_values),
            )
        return added_values

    def _remove_values(self, key: str, values: Union[List[str], str]):
        if isinstance(values, str):
            values = [values]

        # remove duplicates
        values = list(set(values))

        # load existing values
        out_values = self.deserialize(self.con.hget(name=self.hash_name, key=key))
        removed_values = []

        # remove values
        for value in values:
            try:
                out_values.remove(value)
                removed_values.append(value)
            except ValueError:
                self.logger.warning(f"{value} was not found in {self.name}'s {key}'")

        # update Redis
        if removed_values:
            self.logger.info(f"Removing {removed_values} from {key}...")
            self.con.hset(
                name=self.hash_name, key=key, value=self.serialize(out_values),
            )
        return removed_values


class JobRun(RedisObject):
    prefix = "grizly:runs:jobs:"

    def __init__(self, job_name: str, id: Union[int, None] = None, *args, **kwargs):
        super().__init__(name=None, *args, **kwargs)
        self.job_name = job_name
        if id is not None:
            self._id = id
            self.hash_name = f"{self.prefix}{self.job_name}:{self._id}"
            if not self.exists:
                raise JobRunNotFoundError
        else:
            self._id = self.con.incr(f"{self.prefix}{self.job_name}:id")
            self.hash_name = f"{self.prefix}{self.job_name}:{self._id}"
            self.register()

    def info(self):
        pass

    @property
    def duration(self) -> int:
        return self.deserialize(self.con.hget(self.hash_name, "duration"))

    @duration.setter
    @_check_if_exists()
    def duration(self, duration: int):
        self.con.hset(
            self.hash_name, "duration", self.serialize(duration),
        )

    @property
    def error(self) -> str:
        return self.deserialize(self.con.hget(self.hash_name, "error"))

    @error.setter
    @_check_if_exists()
    def error(self, error: str):
        self.con.hset(
            self.hash_name, "error", self.serialize(error),
        )

    @property
    def finished_at(self) -> datetime:
        return self.deserialize(self.con.hget(self.hash_name, "finished_at"))

    @finished_at.setter
    @_check_if_exists()
    def finished_at(self, finished_at: datetime):
        self.con.hset(
            self.hash_name, "finished_at", self.serialize(finished_at),
        )

    @property
    def name(self) -> str:
        return self.deserialize(self.con.hget(self.hash_name, "name"))

    @name.setter
    @_check_if_exists()
    def name(self, name: str):
        self.con.hset(
            self.hash_name, "name", self.serialize(name),
        )

    @property
    def status(self,) -> Literal["fail", "running", "success", None]:
        return self.deserialize(self.con.hget(self.hash_name, "status"))

    @status.setter
    @_check_if_exists()
    def status(self, status: Literal["fail", "running", "success"]):
        self.con.hset(
            self.hash_name, "status", self.serialize(status),
        )

    def register(self):

        mapping = {
            "id": self.serialize(self._id),
            "name": "null",
            "created_at": self.serialize(datetime.now(timezone.utc)),
            "finished_at": "null",
            "duration": "null",
            "status": "null",
            "error": "null",
        }
        self.con.hset(
            name=self.hash_name, key=None, value=None, mapping=mapping,
        )
        return self


class Job(RedisObject):
    prefix = "grizly:registry:jobs:"

    def info(self):
        pass

    @property
    def cron(self) -> str:
        return self.deserialize(self.con.hget(self.hash_name, "cron"))

    @cron.setter
    @_check_if_exists()
    def cron(self, cron: str):
        self.con.hset(
            self.hash_name, "cron", self.serialize(cron),
        )

    @property
    def graph(self) -> Delayed:
        return dask.delayed()(self.tasks, name=self.name + "_graph")

    @property
    def last_run(self) -> JobRun:
        return self.runs[-1]

    @property
    def owner(self) -> str:
        return self.deserialize(self.con.hget(self.hash_name, "owner"))

    @owner.setter
    @_check_if_exists()
    def owner(self, owner: str):
        self.con.hset(
            self.hash_name, "owner", self.serialize(owner),
        )

    @property
    def runs(self) -> List[JobRun]:
        return self.db.get_job_runs(job_name=self.name)

    @property
    def tasks(self) -> List[Delayed]:
        return self.deserialize(self.con.hget(self.hash_name, "tasks"), type="dask",)

    @tasks.setter
    @_check_if_exists()
    def tasks(self, tasks: List[Delayed]):
        self.con.hset(
            self.hash_name, "tasks", self.serialize(tasks),
        )

    # TRIGGERS
    @property
    def triggers(self) -> List["Trigger"]:
        trigger_names = self.deserialize(self.con.hget(self.hash_name, "triggers"))
        triggers = [Trigger(name=trigger_name) for trigger_name in trigger_names]
        return triggers

    @triggers.setter
    def triggers(self, triggers):
        # 1. Remove job from previous triggers
        old_triggers = self.triggers
        for trigger in old_triggers:
            trigger.remove_jobs(self.name)
        # 2. Add job to new triggers
        for new_trigger_name in triggers:
            new_trigger = Trigger(new_trigger_name)
            new_trigger.add_jobs(self.name)
        # 3. Update job with new triggers
        self.con.hset(
            self.hash_name, "triggers", self.serialize(triggers),
        )

    def add_triggers(self, trigger_names: str):

        added_trigger_names = self._add_values(key="triggers", new_values=trigger_names)

        for trigger_name in added_trigger_names:
            trigger = Trigger(name=trigger_name)
            if self not in trigger.jobs:
                trigger.add_jobs(self.name)

    def remove_triggers(self, trigger_names: str):
        removed_trigger_names = self._remove_values(key="triggers", values=trigger_names)

        # remove the job from old triggers
        for trigger_name in removed_trigger_names:
            trigger = Trigger(trigger_name)
            if self in trigger.jobs:
                trigger.remove_jobs(self.name)

    # TRIGGERS END

    # DOWNSTREAM/UPSTREAM

    @property
    def downstream(self) -> List["Job"]:
        downstream_job_names = self.deserialize(self.con.hget(self.hash_name, "downstream"))
        downstream_jobs = [Job(job_name) for job_name in downstream_job_names]
        return downstream_jobs

    @downstream.setter
    @_check_if_exists()
    def downstream(self, new_job_names: List[str]):
        """
        Overwrite the list of downstream jobs.
        """
        self.db._check_if_jobs_exist(new_job_names)
        # 1. Remove from downstream jobs of all the jobs on the previous
        #    upstream jobs list
        old_downstream_jobs = self.downstream
        for downstream_job in old_downstream_jobs:
            downstream_job.remove_upstream_jobs(self.name)
        # 2. Add as a downstream job to the jobs in new_job_names
        for new_downstream_job_name in new_job_names:
            new_downstream_job = Job(new_downstream_job_name)
            new_downstream_job.add_upstream_jobs(self.name)
        # 3. Update upstream jobs with the new job
        self.con.hset(
            self.hash_name, "upstream", self.serialize(new_job_names),
        )

    @_check_if_exists()
    def add_downstream_jobs(self, job_names: Union[List[str], str]):
        """Add downstream jobs

        Parameters
        ----------
        job_names : str or list
            names of the downstream jobs to add
        """
        self.db._check_if_jobs_exist(job_names)

        added_job_names = self._add_values(key="downstream", new_values=job_names)

        # add the job as an upstream of the specified jobs
        for job_name in added_job_names:
            downstream_job = Job(name=job_name)
            if self not in downstream_job.upstream:
                downstream_job.add_upstream_jobs(self.name)

    @_check_if_exists()
    def remove_downstream_jobs(self, job_names: Union[str, List[str]]):

        removed_job_names = self._remove_values(key="downstream", values=job_names)

        # remove the job as an upstream of the specified jobs
        for job_name in removed_job_names:
            downstream_job = Job(job_name)
            if self in downstream_job.upstream:
                downstream_job.remove_upstream_jobs(self.name)

    @property
    def upstream(self) -> List["Job"]:
        upstream_job_names = self.deserialize(self.con.hget(self.hash_name, "upstream"))
        upstream_jobs = [Job(job_name) for job_name in upstream_job_names]
        return upstream_jobs

    @upstream.setter
    @_check_if_exists()
    def upstream(self, new_job_names: List[str]):
        """
        Overwrite the list of upstream jobs.
        """
        self.db._check_if_jobs_exist(new_job_names)
        # 1. Remove from downstream jobs of all the jobs on the previous
        #    upstream jobs list
        old_upstream_jobs = self.upstream
        for upstream_job in old_upstream_jobs:
            upstream_job.remove_downstream_jobs(self.name)
        # 2. Add as a downstream job to the jobs in new_job_names
        for new_upstream_job_name in new_job_names:
            new_upstream_job = Job(new_upstream_job_name)
            new_upstream_job.add_downstream_jobs(self.name)
        # 3. Update upstream jobs with the new job
        self.con.hset(
            self.hash_name, "upstream", self.serialize(new_job_names),
        )

    @_check_if_exists()
    def add_upstream_jobs(self, job_names: Union[List[str], str]):
        """Add upstream jobs

        Parameters
        ----------
        job_names : str or list
            names of the upstream jobs to add
        """
        self.db._check_if_jobs_exist(job_names)

        added_job_names = self._add_values(key="upstream", new_values=job_names)

        # add the job as a downstream of the specified jobs
        for job_name in added_job_names:
            upstream_job = Job(name=job_name)
            if self not in upstream_job.downstream:
                upstream_job.add_downstream_jobs(self.name)

    @_check_if_exists()
    def remove_upstream_jobs(self, job_names: Union[str, List[str]]):

        removed_job_names = self._remove_values(key="upstream", values=job_names)

        # remove the job from the downstream jobs of the specified jobs
        for job_name in removed_job_names:
            upstream_job = Job(job_name)
            if self in upstream_job.downstream:
                upstream_job.remove_downstream_jobs(self.name)

    # DOWNSTREAM/UPSTREAM END

    def cancel(self, scheduler_address: Union[str, None] = None) -> None:
        if not scheduler_address:
            scheduler_address = self.scheduler_address
        client = Client(scheduler_address)
        f = Future(self.name + "_graph", client=client)
        f.cancel(force=True)
        client.close()

    def register(
        self,
        tasks: List[Delayed],
        owner: Union[str, None] = None,
        crons: Union[List[str], str] = [],
        upstream: Union[List[str], str] = [],
        triggers: Union[List[str], str] = [],
        if_exists: Literal["fail", "replace"] = "fail",
        *args,
        **kwargs,
    ) -> "Job":
        if if_exists == "fail" and self.exists:
            raise ValueError(f"Job {self.name} already exists")

        # VALIDATIONS
        # cron
        if isinstance(crons, str):
            crons = [crons]
        for cron in crons:
            if not croniter.is_valid(cron):
                raise ValueError(f"Invalid cron string {cron}")
        # upstream
        if isinstance(upstream, str):
            upstream = [upstream]
        if self.name in upstream:
            raise ValueError("Job cannot be its own upstream job !!!")
        self.db._check_if_jobs_exist(upstream)
        # triggers
        if isinstance(triggers, str):
            triggers = [triggers]

        mapping = {
            "owner": self.serialize(owner),
            "crons": self.serialize(crons),
            "rq_job_ids": "null",
            "upstream": self.serialize(upstream),
            "downstream": self.serialize([]),
            "triggers": self.serialize(triggers),
            "tasks": self.serialize(tasks),
            "args": self.serialize(args),
            "kwargs": self.serialize(kwargs),
            "created_at": self.serialize(datetime.now(timezone.utc)),
        }
        if not (crons or upstream or triggers):
            raise ValueError("One of ['crons', 'upstream', 'triggers'] is required")

        self.con.hset(
            name=self.hash_name, key=None, value=None, mapping=mapping,
        )

        if crons:
            # TODO: put below in method
            rq_job_ids = []
            queue = Queue(RedisDB.submit_queue_name, connection=self.con)
            scheduler = Scheduler(queue=queue, connection=self.con)
            for cron in crons:
                rq_job = scheduler.cron(
                    cron,
                    func=self.submit,
                    kwargs=kwargs,
                    repeat=None,
                    queue_name=RedisDB.submit_queue_name,
                )
                rq_job_ids.append(rq_job.id)
            self.con.hset(
                name=self.hash_name, key="rq_job_ids", value=self.serialize(rq_job_ids),
            )

        # add the job as downstream in all upstream jobs
        for upstream_job_name in upstream:
            upstream_job = Job(name=upstream_job_name)
            upstream_job.add_downstream_jobs(self.name)

        # add the job in all triggers
        for trigger_name in triggers:
            trigger = Trigger(name=trigger_name)
            trigger.add_jobs(self.name)

        self.con.set(f"{JobRun.prefix}{self.name}:id", "0")

        self.logger.info(f"Job {self.name} successfully registered")
        return self

    def unregister(self, remove_job_runs: bool = False) -> None:

        # for cron in crons:
        #     queue = Queue(RedisDB.submit_queue_name, connection=self.con)
        #     scheduler = Scheduler(queue=queue, connection=self.con)
        #     scheduler.cron(
        #         cron,
        #         func=self.submit,
        #         kwargs=kwargs,
        #         repeat=None,
        #         queue_name=RedisDB.submit_queue_name,
        #     )

        # remove job from downstream in all upstream jobs
        for upstream_job in self.upstream:
            upstream_job.remove_downstream_jobs(self.name)

        # remove job from upstream in all downstream jobs
        for downstream_job in self.downstream:
            downstream_job.remove_upstream_jobs(self.name)

        # remove job from all triggers
        for trigger in self.triggers:
            trigger.remove_jobs(self.name)

        if remove_job_runs:
            # remove job run id increment
            self.con.delete(f"{JobRun.prefix}{self.name}:id")
            for job_run in self.db.get_job_runs(job_name=self.name):
                job_run.remove()

        self.logger.info(f"Job {self.name} successfully registered")
        return self

    @_check_if_exists()
    def submit(
        self,
        client: Client = None,
        scheduler_address: str = None,
        priority: int = None,
        resources: Dict[str, Any] = None,
        to_dask=True,
    ) -> Any:

        if self.last_run.status == "running":
            self.logger.warning(
                f"Job {self.name} is already running. To stop the process please use ..."
            )
        else:
            priority = priority or 1
            if to_dask:
                if not client:
                    self.scheduler_address = scheduler_address or os.getenv(
                        "GRIZLY_DEV_DASK_SCHEDULER_ADDRESS"
                    )
                    client = Client(self.scheduler_address)
                else:
                    self.scheduler_address = client.scheduler.address

                if not client and not self.scheduler_address:
                    raise ValueError("distributed.Client/scheduler address was not provided")

            self.logger.info(f"Submitting job {self.name}...")
            job_run = JobRun(job_name=self.name).register()
            job_run.status = "running"

            start = time()
            try:
                result = self.graph.compute()
                status = "success"
            except Exception:
                result = None
                status = "fail"
                _, exc_value, _ = sys.exc_info()
                job_run.error = str(exc_value)

            end = time()
            job_run.finished_at = datetime.now(timezone.utc)
            job_run.duration = int(end - start)
            job_run.status = status

            self.logger.info(f"Job {self.name} finished with status {status}")

            client.close()
            return result

    @_check_if_exists()
    def visualize(self, **kwargs):
        return self.graph.visualize(**kwargs)


class Trigger(RedisObject):
    prefix = "grizly:registry:triggers:"

    def info(self):
        pass

    @property
    def is_triggered(self) -> bool:
        return self.deserialize(self.con.hget(self.hash_name, "is_triggered"))

    @_check_if_exists()
    @is_triggered.setter
    def is_triggered(self, value: bool):
        self.con.hset(
            self.hash_name, "is_triggered", self.serialize(value),
        )

    @property
    def jobs(self) -> List[Union["Job", None]]:
        job_names = self.deserialize(self.con.hget(self.hash_name, "jobs"))
        return [Job(name=job) for job in job_names]

    # @property
    # def last_run(self) -> datetime:
    #     return self.deserialize(
    #         self.con.hget(self.hash_name, "last_run"), type="datetime"
    #     )

    # @last_run.setter
    # def last_run(self, value: datetime):
    #     self.con.hset(self.hash_name, "last_run", self.serialize(value))

    # @property
    # def next_run(self) -> datetime:
    #     start_date = self.last_run or self.created_at
    #     cron_str = self.value
    #     cron = croniter(cron_str, start_date)
    #     next_run = cron.get_next(datetime).replace(tzinfo=timezone.utc)
    #     return next_run

    # @property
    # def type(self) -> Literal["cron", "listener"]:
    #     return self.deserialize(self.con.hget(self.hash_name, "type"))

    # @property
    # def value(self) -> str:
    #     return self.deserialize(self.con.hget(self.hash_name, "value"))

    # @value.setter
    # def value(self, value: str):
    #     self.con.hset(self.hash_name, "value", self.serialize(value))

    def add_jobs(self, job_names: Union[List[str], str]):
        if not self.exists:
            self.register()

        self.db._check_if_jobs_exist(job_names)

        self._add_values(key="jobs", new_values=job_names)

    def register(self):

        mapping = {
            "is_triggered": "null",
            "jobs": self.serialize([]),
            "created_at": self.serialize(datetime.now(timezone.utc)),
        }
        self.con.hset(
            name=self.hash_name, key=None, value=None, mapping=mapping,
        )
        return self

    def remove_jobs(self, job_names: Union[List[str], str]):
        if not self.exists:
            return None

        self._remove_values(key="jobs", values=job_names)
