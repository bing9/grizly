from abc import ABC, abstractmethod
from datetime import datetime, timezone
from functools import wraps
import json
import logging
import os
import sys
from time import time
from typing import Any, Dict, List, Literal, Union, Optional

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
                    if self.__class__.__name__ == "Job":
                        raise JobNotFoundError(self)
                    elif self.__class__.__name__ == "JobRun":
                        raise JobRunNotFoundError(self)
                else:
                    self.logger.warning(f"{self} not found in the registry")

            return f(self, *args, **kwargs)

        return wrapped

    return deco_wrap


class RedisDB:
    submit_queue_name = "submit"
    system_queue_name = "system"

    def __init__(
        self,
        redis_host: Optional[str] = None,
        redis_port: Optional[int] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self.logger = logger or logging.getLogger(f"rq.worker.{__name__}")
        logging.basicConfig(
            format="%(asctime)s | %(levelname)s : %(message)s",
            level=logging.INFO,
            stream=sys.stderr,
        )
        self.config = Config().get_service("scheduling")
        self.redis_host = (
            redis_host
            or os.getenv("GRIZLY_REDIS_HOST")
            or self.config.get("redis_host")
            or "localhost"
        )
        self.redis_port = int(
            redis_port or os.getenv("GRIZLY_REDIS_PORT") or self.config.get("redis_port") or 6379
        )

    @property
    def con(self):
        con = Redis(host=self.redis_host, port=self.redis_port, db=0)
        return con

    def add_trigger(self, name: str):
        tr = Trigger(name=name, logger=self.logger, db=self)
        tr.register()

    def get_triggers(self) -> List["Trigger"]:
        triggers = []
        prefix = Trigger.prefix
        tr_hash_names = [val.decode("utf-8") for val in self.con.keys(f"{prefix}*")]
        for tr_hash_name in tr_hash_names:
            trigger_name = tr_hash_name[len(prefix) :]
            tr = Trigger(name=trigger_name, logger=self.logger, db=self)
            triggers.append(tr)
        return triggers

    def add_job(
        self,
        name: str,
        tasks: List[Delayed],
        owner: Optional[str] = None,
        crons: Union[List[str], str] = [],
        upstream: Union[List[str], str] = [],
        triggers: Union[List[str], str] = [],
        if_exists: Literal["fail", "replace"] = "fail",
        *args,
        **kwargs,
    ):
        job = Job(name=name, logger=self.logger, db=self)
        job.register(
            owner=owner,
            tasks=tasks,
            crons=crons,
            upstream=upstream,
            triggers=triggers,
            if_exists=if_exists,
            *args,
            **kwargs,
        )

    def get_jobs(self) -> List["Job"]:
        jobs = []
        prefix = Job.prefix
        job_hash_names = [val.decode("utf-8") for val in self.con.keys(f"{prefix}*")]
        for job_hash_name in job_hash_names:
            job_name = job_hash_name[len(prefix) :]
            job = Job(name=job_name, logger=self.logger, db=self)
            jobs.append(job)
        return jobs

    def get_job_runs(self, job_name: Optional[str] = None) -> List["JobRun"]:
        job_runs = []

        if job_name is not None:
            prefix = f"{JobRun.prefix}{job_name}:"
            job_run_hash_names = [val.decode("utf-8") for val in self.con.keys(f"{prefix}*")]
            for job_run_hash_name in job_run_hash_names:
                job_run_id = job_run_hash_name[len(f"{prefix}") :]
                if job_run_id != "id":
                    job_run = JobRun(
                        job_name=job_name, id=int(job_run_id), logger=self.logger, db=self
                    )
                    job_runs.append(job_run)
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
        """Iterate through list of names of objects and check if they exist - if not raise error"""
        if isinstance(values, str):
            values = [values]
        if object_type == "job":
            for job_name in values:
                job = Job(name=job_name, logger=self.logger, db=self)
                if not job.exists:
                    raise JobNotFoundError


class RedisObject(ABC):
    prefix = "grizly:"

    def __init__(
        self,
        name: Optional[str],
        logger: Optional[logging.Logger] = None,
        db: Optional[RedisDB] = None,
        redis_host: Optional[str] = None,
        redis_port: Optional[int] = None,
    ):
        # TODO: fix this workaround - we need this cause JobRun has name property
        if self.__class__.__name__ != "JobRun":
            self.name = name or ""
            self.hash_name = self.prefix + self.name
        self.db = db or RedisDB(logger=logger, redis_host=redis_host, redis_port=redis_port)
        self.logger = logger or logging.getLogger(__name__)
        logging.basicConfig(
            format="%(asctime)s | %(levelname)s : %(message)s",
            level=logging.INFO,
            stream=sys.stderr,
        )

    def __eq__(self, other):
        return self.hash_name == other.hash_name

    def __repr__(self):
        return f"{self.__class__.__name__}(name='{self.name}')"

    @abstractmethod
    def info(self):
        pass

    @abstractmethod
    def register(self):
        pass

    @abstractmethod
    def unregister(self):
        pass

    @property
    def created_at(self) -> datetime:
        return self._deserialize(self.con.hget(self.hash_name, "created_at"), type="datetime",)

    @property
    def con(self):
        con = self.db.con
        return con

    @property
    def exists(self):
        return self.con.exists(self.hash_name)

    def getall(self):  # to be removed or replaced git get_all()
        return self.con.hgetall(self.hash_name)

    @staticmethod
    def _serialize(value: Any) -> str:
        if isinstance(value, datetime):
            value = str(value)

        if isinstance(value, list) and all(isinstance(i, Delayed) for i in value) and value != []:
            value = str(dask_serialize(value))

        return json.dumps(value)

    @staticmethod
    def _deserialize(value: Any, type: Optional[Literal["datetime", "dask"]] = None,) -> Any:
        if value is None:
            return None
        else:
            value = json.loads(value)
            if value is not None:
                if type == "datetime":
                    value = datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f%z")
                elif type == "dask":
                    value = dask_deserialize(*eval(value))

            return value

    def _add_values(self, key: str, new_values: Union[List[str], str]):
        if isinstance(new_values, str):
            new_values = [new_values]

        # remove duplicates
        new_values = list(set(new_values))

        # load existing values
        out_values = self._deserialize(self.con.hget(name=self.hash_name, key=key))
        added_values = []

        # append existing values
        for new_value in new_values:
            if new_value in out_values:
                self.logger.warning(f"'{new_value}' already exists in {self}.{key}")
            else:
                out_values.append(new_value)
                added_values.append(new_value)

        # update Redis
        if added_values:
            self.logger.info(f"Adding {added_values} to {self}.{key}...")
            self.con.hset(
                name=self.hash_name, key=key, value=self._serialize(out_values),
            )
        return added_values

    def _remove_values(self, key: str, values: Union[List[str], str]):
        if isinstance(values, str):
            values = [values]

        # remove duplicates
        values = list(set(values))

        # load existing values
        out_values = self._deserialize(self.con.hget(name=self.hash_name, key=key))
        removed_values = []

        # remove values
        for value in values:
            try:
                out_values.remove(value)
                removed_values.append(value)
            except ValueError:
                self.logger.warning(f"Value '{value}' was not found in {self}.{key}")

        # update Redis
        if removed_values:
            self.logger.info(f"Removing {removed_values} from {self}.{key}...")
            self.con.hset(
                name=self.hash_name, key=key, value=self._serialize(out_values),
            )
        return removed_values


class JobRun(RedisObject):
    prefix = "grizly:runs:jobs:"

    def __init__(self, job_name: str, id: Optional[int] = None, *args, **kwargs):
        super().__init__(name=None, *args, **kwargs)
        self.job_name = job_name
        if id is not None:
            self._id = id
            self.hash_name = f"{self.prefix}{self.job_name}:{self._id}"
            # if not self.exists:
            #     raise JobRunNotFoundError(self)
        else:
            self._id = int(self.con.incr(f"{self.prefix}{self.job_name}:id"))
            self.hash_name = f"{self.prefix}{self.job_name}:{self._id}"
            self.register()

    def __repr__(self):
        return f"{self.__class__.__name__}(job_name='{self.job_name}', id='{self._id}')"

    def __lt__(self, other):
        return self._id < other._id

    @_check_if_exists()
    def info(self):
        s = (
            f"id: {self._id}\n"
            f"name: {self.name}\n"
            f"created_at: {self.created_at}\n"
            f"finished_at: {self.finished_at}\n"
            f"duration: {self.duration}\n"
            f"status: {self.status}\n"
            f"error: {self.error}"
        )
        print(s)

    @property
    def duration(self) -> int:
        return self._deserialize(self.con.hget(self.hash_name, "duration"))

    @duration.setter
    @_check_if_exists()
    def duration(self, duration: int):
        self.con.hset(
            self.hash_name, "duration", self._serialize(duration),
        )

    @property
    def error(self) -> str:
        return self._deserialize(self.con.hget(self.hash_name, "error"))

    @error.setter
    @_check_if_exists()
    def error(self, error: str):
        self.con.hset(
            self.hash_name, "error", self._serialize(error),
        )

    @property
    def finished_at(self) -> datetime:
        return self._deserialize(self.con.hget(self.hash_name, "finished_at"), type="datetime")

    @finished_at.setter
    @_check_if_exists()
    def finished_at(self, finished_at: datetime):
        self.con.hset(
            self.hash_name, "finished_at", self._serialize(finished_at),
        )

    @property
    def name(self) -> str:
        return self._deserialize(self.con.hget(self.hash_name, "name"))

    @name.setter
    @_check_if_exists()
    def name(self, name: str):
        self.con.hset(
            self.hash_name, "name", self._serialize(name),
        )

    @property
    def status(self) -> Literal["fail", "running", "success", None]:
        return self._deserialize(self.con.hget(self.hash_name, "status"))

    @status.setter
    @_check_if_exists()
    def status(self, status: Literal["fail", "running", "success"]):
        self.con.hset(
            self.hash_name, "status", self._serialize(status),
        )

    def register(self):

        mapping = {
            "id": self._serialize(self._id),
            "name": "null",
            "created_at": self._serialize(datetime.now(timezone.utc)),
            "finished_at": "null",
            "duration": "null",
            "status": "null",
            "error": "null",
        }
        self.con.hset(
            name=self.hash_name, key=None, value=None, mapping=mapping,
        )
        return self

    def unregister(self):
        self.con.delete(self.hash_name)


class Job(RedisObject):
    prefix = "grizly:registry:jobs:"

    @_check_if_exists()
    def info(self):
        s = (
            f"name: {self.name}\n"
            f"owner: {self.owner}\n"
            f"created_at: {self.created_at}\n"
            f"crons: {self.crons}\n"
            f"downstream: {self.downstream}\n"
            f"upstream: {self.upstream}\n"
            f"triggers: {self.triggers}"
        )
        print(s)

    @property
    def crons(self) -> List[str]:
        return self._deserialize(self.con.hget(self.hash_name, "crons"))

    @crons.setter
    @_check_if_exists()
    def crons(self, crons: Union[List[str], str]):
        # VALIDATIONS
        if isinstance(crons, str):
            crons = [crons]
        for cron in crons:
            if not croniter.is_valid(cron):
                raise ValueError(f"Invalid cron string {cron}")

        self.__remove_from_scheduler()
        self._rq_job_ids = []
        self._rq_job_ids = self.__add_to_scheduler(crons)

        self.con.hset(
            self.hash_name, "crons", self._serialize(crons),
        )

    @property
    def _rq_job_ids(self):
        return self._deserialize(self.con.hget(self.hash_name, "rq_job_ids"))

    @_rq_job_ids.setter
    def _rq_job_ids(self, _rq_job_ids: List[str]):
        self.con.hset(
            self.hash_name, "rq_job_ids", self._serialize(_rq_job_ids),
        )

    @property
    def graph(self) -> Delayed:
        return dask.delayed()(self.tasks, name=self.name + "_graph")

    @property
    def last_run(self) -> Optional[JobRun]:
        _id = self._deserialize(self.con.get(f"{JobRun.prefix}{self.name}:id"))
        if _id:
            return JobRun(job_name=self.name, id=_id, logger=self.logger, db=self.db)

    @property
    def owner(self) -> str:
        return self._deserialize(self.con.hget(self.hash_name, "owner"))

    @owner.setter
    @_check_if_exists()
    def owner(self, owner: str):
        self.con.hset(
            self.hash_name, "owner", self._serialize(owner),
        )

    @property
    def runs(self) -> List[JobRun]:
        return self.db.get_job_runs(job_name=self.name)

    @property
    def tasks(self) -> List[Delayed]:
        return self._deserialize(self.con.hget(self.hash_name, "tasks"), type="dask",)

    @tasks.setter
    @_check_if_exists()
    def tasks(self, tasks: List[Delayed]):
        self.con.hset(
            self.hash_name, "tasks", self._serialize(tasks),
        )

    # TRIGGERS
    @property
    def triggers(self) -> List["Trigger"]:
        trigger_names = self._deserialize(self.con.hget(self.hash_name, "triggers"))
        triggers = [
            Trigger(name=trigger_name, logger=self.logger, db=self.db)
            for trigger_name in trigger_names
        ]
        return triggers

    @triggers.setter
    def triggers(self, triggers: Union[List[str], str]):
        if isinstance(triggers, str):
            triggers = [triggers]

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
            self.hash_name, "triggers", self._serialize(triggers),
        )

    def add_triggers(self, trigger_names: Union[List[str], str]):

        added_trigger_names = self._add_values(key="triggers", new_values=trigger_names)

        for trigger_name in added_trigger_names:
            trigger = Trigger(name=trigger_name, logger=self.logger, db=self.db)
            if self not in trigger.jobs:
                trigger.add_jobs(self.name)

    def remove_triggers(self, trigger_names: Union[List[str], str]):
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
        downstream_job_names = self._deserialize(self.con.hget(self.hash_name, "downstream"))
        downstream_jobs = [Job(job_name) for job_name in downstream_job_names]
        return downstream_jobs

    @downstream.setter
    @_check_if_exists()
    def downstream(self, new_job_names: Union[List[str], str]):
        """
        Overwrite the list of downstream jobs.
        """
        self.db._check_if_jobs_exist(new_job_names)
        if isinstance(new_job_names, str):
            new_job_names = [new_job_names]
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
            self.hash_name, "downstream", self._serialize(new_job_names),
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
            downstream_job = Job(name=job_name, logger=self.logger, db=self.db)
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
        upstream_job_names = self._deserialize(self.con.hget(self.hash_name, "upstream"))
        upstream_jobs = [Job(job_name) for job_name in upstream_job_names]
        return upstream_jobs

    @upstream.setter
    @_check_if_exists()
    def upstream(self, new_job_names: Union[List[str], str]):
        """
        Overwrite the list of upstream jobs.
        """
        self.db._check_if_jobs_exist(new_job_names)
        if isinstance(new_job_names, str):
            new_job_names = [new_job_names]
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
            self.hash_name, "upstream", self._serialize(new_job_names),
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
            upstream_job = Job(name=job_name, logger=self.logger, db=self.db)
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

    def cancel(self, scheduler_address: Optional[str] = None) -> None:
        if not scheduler_address:
            scheduler_address = self.scheduler_address
        client = Client(scheduler_address)
        f = Future(self.name + "_graph", client=client)
        f.cancel(force=True)
        client.close()

    def register(
        self,
        tasks: List[Delayed],
        owner: Optional[str] = None,
        crons: Union[List[str], str] = [],
        upstream: Union[List[str], str] = [],
        triggers: Union[List[str], str] = [],
        if_exists: Literal["fail", "replace"] = "fail",
        *args,
        **kwargs,
    ) -> "Job":
        if self.exists:
            if if_exists == "fail":
                raise ValueError(f"{self} already exists")
            else:
                self.unregister(remove_job_runs=True)

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
            "owner": self._serialize(owner),
            "crons": self._serialize(crons),
            "rq_job_ids": self._serialize([]),
            "upstream": self._serialize(upstream),
            "downstream": self._serialize([]),
            "triggers": self._serialize(triggers),
            "tasks": self._serialize(tasks),
            "args": self._serialize(args),
            "kwargs": self._serialize(kwargs),
            "created_at": self._serialize(datetime.now(timezone.utc)),
        }
        # if not (crons or upstream or triggers):
        #     raise ValueError("One of ['crons', 'upstream', 'triggers'] is required")

        self.con.hset(
            name=self.hash_name, key=None, value=None, mapping=mapping,
        )

        self._rq_job_ids = self.__add_to_scheduler(crons, *args, **kwargs)

        # add the job as downstream in all upstream jobs
        for upstream_job_name in upstream:
            upstream_job = Job(name=upstream_job_name, logger=self.logger, db=self.db)
            upstream_job.add_downstream_jobs(self.name)

        # add the job in all triggers
        for trigger_name in triggers:
            trigger = Trigger(name=trigger_name, logger=self.logger, db=self.db)
            trigger.add_jobs(self.name)

        self.con.set(f"{JobRun.prefix}{self.name}:id", "0")

        self.logger.info(f"{self} successfully registered")
        return self

    def unregister(self, remove_job_runs: bool = False) -> None:

        # remove from rq scheduler
        self.__remove_from_scheduler()
        self._rq_job_ids = []

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
            # remove job runs
            for job_run in self.db.get_job_runs(job_name=self.name):
                job_run.unregister()
            self.logger.info(f"{self}'s runs have been removed from registry")

        self.con.delete(self.hash_name)

        self.logger.info(f"{self} successfully removed from registry")

    @_check_if_exists()
    def submit(
        self,
        client: Client = None,
        scheduler_address: str = None,
        priority: int = None,
        resources: Dict[str, Any] = None,
        to_dask=True,
    ) -> Any:

        if self.last_run and self.last_run.status == "running":
            self.logger.warning(
                f"Job {self.name} is already running. To stop the process please use ..."
            )
        else:
            priority = priority or 1
            if to_dask:
                if client is None:
                    self.scheduler_address = scheduler_address or os.getenv(
                        "GRIZLY_DASK_SCHEDULER_ADDRESS"
                    )
                    client = Client(self.scheduler_address)
                else:
                    self.scheduler_address = client.scheduler.address

            self.logger.info(f"Submitting {self}...")
            job_run = JobRun(job_name=self.name, logger=self.logger, db=self.db)
            job_run.status = "running"

            start = time()
            try:
                result = self.graph.compute()
                status = "success"
                self.logger.info(f"{self} finished with status {status}")
                self.__submit_downstream_jobs()
            except Exception:
                result = None
                status = "fail"
                _, exc_value, _ = sys.exc_info()
                job_run.error = str(exc_value)
                self.logger.info(f"{self} finished with status {status}")
            finally:
                end = time()
                job_run.finished_at = datetime.now(timezone.utc)
                job_run.duration = int(end - start)
                job_run.status = status

                if to_dask:
                    client.close()
                return result

    @_check_if_exists()
    def visualize(self, **kwargs):
        return self.graph.visualize(**kwargs)

    def __add_to_scheduler(self, crons: List[str], *args, **kwargs):
        rq_job_ids = []
        if crons:
            queue = Queue(RedisDB.submit_queue_name, connection=self.con)
            scheduler = Scheduler(queue=queue, connection=self.con)
            for cron in crons:
                rq_job = scheduler.cron(
                    cron,
                    func=self.submit,
                    args=args,
                    kwargs=kwargs,
                    repeat=None,
                    queue_name=queue.name,
                )
                self.logger.debug(
                    f"{self} with cron '{cron}' has been added to rq sheduler with id {rq_job.id}"
                )
                rq_job_ids.append(rq_job.id)
            self.logger.info(f"{self} has been added to the scheduler")

        return rq_job_ids

    def __remove_from_scheduler(self):
        rq_job_ids = self._rq_job_ids
        if rq_job_ids:
            queue = Queue(RedisDB.submit_queue_name, connection=self.con)
            scheduler = Scheduler(queue=queue, connection=self.con)
            for rq_job_id in rq_job_ids:
                scheduler.cancel(rq_job_id)
                self.logger.debug(f"Rq job {rq_job_id} removed from the scheduler")

            self.logger.info(f"{self} has been removed from the scheduler")

    def __submit_downstream_jobs(self):
        self.logger.info(f"Enqueueing {self}.downstream...")
        self.logger.info(f"Host: {self.db.redis_host}, Port: {self.db.redis_port}")
        queue = Queue(RedisDB.submit_queue_name, connection=self.con)
        for job in self.downstream:
            # TODO: should read downstream *args ad **kwargs from registry
            queue.enqueue(job.submit)
            self.logger.info(f"{job} has been enqueued")


class Trigger(RedisObject):
    prefix = "grizly:registry:triggers:"

    def info(self):
        pass

    @property
    def is_triggered(self) -> bool:
        return self._deserialize(self.con.hget(self.hash_name, "is_triggered"))

    @_check_if_exists()
    @is_triggered.setter
    def is_triggered(self, value: bool):
        self.con.hset(
            self.hash_name, "is_triggered", self._serialize(value),
        )

    @property
    def jobs(self) -> List[Optional["Job"]]:
        job_names = self._deserialize(self.con.hget(self.hash_name, "jobs"))
        return [Job(name=job, logger=self.logger, db=self.db) for job in job_names]

    # @property
    # def last_run(self) -> datetime:
    #     return self._deserialize(
    #         self.con.hget(self.hash_name, "last_run"), type="datetime"
    #     )

    # @last_run.setter
    # def last_run(self, value: datetime):
    #     self.con.hset(self.hash_name, "last_run", self._serialize(value))

    def add_jobs(self, job_names: Union[List[str], str]):
        if not self.exists:
            self.register()

        self.db._check_if_jobs_exist(job_names)

        self._add_values(key="jobs", new_values=job_names)

    def register(self):

        mapping = {
            "is_triggered": "null",
            "jobs": self._serialize([]),
            "created_at": self._serialize(datetime.now(timezone.utc)),
        }
        self.con.hset(
            name=self.hash_name, key=None, value=None, mapping=mapping,
        )
        return self

    def remove_jobs(self, job_names: Union[List[str], str]):
        if not self.exists:
            return None

        self._remove_values(key="jobs", values=job_names)

    def unregister(self) -> None:

        # remove trigger from all jobs
        for job in self.jobs:
            job.remove_triggers(self.name)

        self.con.delete(self.hash_name)

        self.logger.info(f"{self} successfully removed from registry")
