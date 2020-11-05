from __future__ import annotations

import json
import logging
import os
import sys
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from functools import wraps
from time import time
from typing import Any, List, Literal, Optional, Union, Dict

import dask
from croniter import croniter
from dask.delayed import Delayed
from distributed import Client, Future
from distributed.protocol.serialize import deserialize as dask_deserialize
from distributed.protocol.serialize import serialize as dask_serialize
from redis import Redis
from rq import Queue
from rq.job import Job as RqJob
from rq.job import NoSuchJobError
from rq_scheduler import Scheduler

from ..config import Config
from ..exceptions import JobAlreadyRunningError, JobNotFoundError, JobRunNotFoundError
from ..utils.functions import dict_diff
from ..store import Store

SubmitCondition = Literal["success", "fail", "result_change"]


def _check_if_exists(raise_error=True):
    """Checks if the job exists in the registry Parameters
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


class SchedulerDB:
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
        return Redis(host=self.redis_host, port=self.redis_port, db=0)

    @property
    def _con(self):
        return self.con

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
        crons: Union[List[str], str] = None,
        upstream: Dict[str, SubmitCondition] = None,
        triggers: Union[List[str], str] = None,
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
        return sorted(jobs, key=lambda job: job.name)

    def get_job_runs(self, job_name: Optional[str] = None) -> List[JobRun]:

        if job_name is None:
            jobs = self.get_jobs()
            job_runs = [run for job in jobs for run in job.runs]
        else:
            prefix = f"{JobRun.prefix}{job_name}:"
            job_run_hash_names = [val.decode("utf-8") for val in self.con.keys(f"{prefix}*")]

            job_runs = []
            for job_run_hash_name in job_run_hash_names:
                job_run_id = job_run_hash_name[len(f"{prefix}") :]
                if job_run_id != "id":
                    job_run = JobRun(
                        job_name=job_name, id=int(job_run_id), logger=self.logger, db=self
                    )
                    job_runs.append(job_run)
        return sorted(job_runs)

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


class SchedulerObject(ABC):
    prefix = "grizly:"

    def __init__(
        self,
        name: Optional[str],
        logger: Optional[logging.Logger] = None,
        db: Optional[SchedulerDB] = None,
        redis_host: Optional[str] = None,
        redis_port: Optional[int] = None,
    ):
        # TODO: fix this workaround - we need this cause JobRun has name property
        if self.__class__.__name__ != "JobRun":
            self.name = name or ""
            self.hash_name = self.prefix + self.name
        self.db = db or SchedulerDB(logger=logger, redis_host=redis_host, redis_port=redis_port)
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

    @property
    def con(self):
        return self.db.con

    @property
    def _con(self):
        return self.con

    @property
    def created_at(self) -> datetime:
        return self._get("created_at", _type="datetime")

    @property
    def exists(self):
        return self.con.exists(self.hash_name)

    @property
    def time_now(self) -> datetime:
        return datetime.now(timezone.utc)

    @abstractmethod
    def info(self):
        pass

    @abstractmethod
    def register(self):
        pass

    @abstractmethod
    def unregister(self):
        pass

    def getall(self):  # to be removed or replaced git get_all()
        return self.con.hgetall(self.hash_name)

    @property
    def meta(self) -> Store:
        deserialized_data = {"name": self.name}
        for key, value in self.con.hgetall(self.hash_name).items():
            key = key.decode()
            if key == "tasks":
                try:
                    deserialized_data[key] = self._deserialize(value, _type="dask")
                except:
                    self.logger.warning("Tasks could not be deserialized")
                    deserialized_data[key] = []
            else:
                try:
                    deserialized_data[key] = self._deserialize(value, _type="datetime")
                except (TypeError, ValueError):
                    deserialized_data[key] = self._deserialize(value)

        return Store(deserialized_data)

    @staticmethod
    def _serialize(value: Any) -> str:
        if isinstance(value, datetime):
            value = str(value)

        if isinstance(value, list) and all(isinstance(i, Delayed) for i in value) and value != []:
            value = str(dask_serialize(value))

        return json.dumps(value)

    @staticmethod
    def _deserialize(value: Any, _type: Optional[Literal["datetime", "dask"]] = None,) -> Any:
        if value is None:
            return None
        else:
            value = json.loads(value)
            if value is not None:
                if _type == "datetime":
                    value = datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f%z")
                elif _type == "dask" and value != []:
                    value = dask_deserialize(*eval(value))

            return value

    def _get(self, key: str, _type: Optional[Literal["datetime", "dask"]] = None,) -> Any:
        raw_value = self.con.hget(self.hash_name, key)
        deserialized_value = self._deserialize(value=raw_value, _type=_type)
        self.logger.debug(f"{self} : {key} : {deserialized_value}")
        return deserialized_value

    def _add_values_to_list(self, key: str, new_values: Union[List[str], str]):
        if isinstance(new_values, str):
            new_values = [new_values]

        # remove duplicates
        new_values = list(set(new_values))

        # load existing values
        out_values = self._get(key=key)
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

    def _get_dict_data_diff(self, key: str, new: Dict[str, str]) -> Dict[str, str]:
        """Filter out unchaged key/values pairs between redis data in 'key' and data 'data'

        Parameters
        ----------
        key : str
            Redis key
        new : Dict[str, str]
            Dictionary containing key/value pairs to be compared with redis 'key'
        """

        # load existing data
        existing = self._get(key=key)

        # filter out unchanged key/value pairs
        changed = dict_diff(existing, new, by="any")
        if not changed:
            self.logger.warning(f"No values to be changed in {self}.{key}")
            return {}

        not_changed = dict_diff(changed, new)

        if not_changed:
            self.logger.warning(f"{list(not_changed.keys())} remained unchanged in {self}.{key}")

        return changed

    def _update_dict_values(self, key: str, new: Dict[str, str]):
        """Update redis 'key' with 'new' data

        Parameters
        ----------
        key : str
            Redis key
        new : Dict[str, str]
            Dictionary containing key/value pairs to be added/updated in 'key'
        """
        if new:
            # load existing data
            existing = self._get(key=key)

            out = {**existing, **new}

            # update Redis
            self.logger.info(f"Updating {list(new.keys())} in {self}.{key}...")
            self.con.hset(
                name=self.hash_name, key=key, value=self._serialize(out),
            )

    def _remove_values(self, key: str, values: Union[List[str], str]):
        # TODO: NEEDS TO BE REFOCTORED - split to getter and setter methods for dict and list
        if isinstance(values, str):
            values = [values]

        # remove duplicates
        values = list(set(values))

        # load existing values
        existing = self._get(key=key)
        removed_values = []

        # remove values
        for value in values:
            try:
                if isinstance(existing, list):
                    existing.remove(value)
                else:
                    del existing[value]
                removed_values.append(value)
            except (ValueError, KeyError):
                self.logger.warning(f"Value '{value}' was not found in {self}.{key}")

        # update Redis
        if removed_values:
            self.logger.info(f"Removing {removed_values} from {self}.{key}...")
            self.con.hset(
                name=self.hash_name, key=key, value=self._serialize(existing),
            )
        return removed_values


class JobRun(SchedulerObject):
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
        d = self.meta
        s = (
            f"id: {self._id}\n"
            f"name: {d.name}\n"
            f"created_at: {d.created_at}\n"
            f"finished_at: {d.finished_at}\n"
            f"duration: {d.duration}\n"
            f"status: {d.status}\n"
            f"error: {d.error}\n"
            f"result: {d.result}"
        )
        print(s)

    @property
    def duration(self) -> int:
        return self._get("duration")

    @duration.setter
    @_check_if_exists()
    def duration(self, duration: int):
        self.con.hset(
            self.hash_name, "duration", self._serialize(duration),
        )

    @property
    def error(self) -> str:
        return self._get("error")

    @error.setter
    @_check_if_exists()
    def error(self, error: str):
        self.con.hset(
            self.hash_name, "error", self._serialize(error),
        )

    @property
    def finished_at(self) -> datetime:
        return self._get("finished_at", _type="datetime")

    @finished_at.setter
    @_check_if_exists()
    def finished_at(self, finished_at: datetime):
        self.con.hset(
            self.hash_name, "finished_at", self._serialize(finished_at),
        )

    @property
    def name(self) -> str:
        return self._get("name")

    @name.setter
    @_check_if_exists()
    def name(self, name: str):
        self.con.hset(
            self.hash_name, "name", self._serialize(name),
        )

    @property
    def result(self) -> List[Any]:
        return self._get("result")

    @result.setter
    @_check_if_exists()
    def result(self, result: List[Any]):
        self.con.hset(
            self.hash_name, "result", self._serialize(result),
        )

    @property
    def status(self) -> Literal["fail", "running", "success", None]:
        return self._get("status")

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
            "result": "null",
        }
        self.con.hset(
            name=self.hash_name, key=None, value=None, mapping=mapping,
        )
        return self

    def unregister(self):
        self.con.delete(self.hash_name)


class Job(SchedulerObject):
    prefix = "grizly:registry:jobs:"
    scheduler_address = None

    @_check_if_exists()
    def info(self):
        """Print a concise summary of the Job"""
        s = (
            f"name: {self.name}\n"
            f"owner: {self.owner}\n"
            f"description: {self.description}\n"
            f"timeout: {self.timeout}\n"
            f"created_at: {self.created_at}\n"
            f"crons: {self.crons}\n"
            f"downstream: {self.downstream}\n"
            f"upstream: {self.upstream}\n"
            f"triggers: {self.triggers}"
        )
        print(s)

    @property
    def crons(self) -> List[str]:
        """Job's cron strings"""
        return self._get("crons")

    @crons.setter
    @_check_if_exists()
    def crons(self, crons: Union[List[str], str]):
        # VALIDATIONS
        if isinstance(crons, str):
            crons = [crons]
        elif crons is None:
            crons = []
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
    def description(self) -> str:
        return self._get("description")

    @description.setter
    @_check_if_exists()
    def description(self, description: str):
        """Job's description"""
        self.con.hset(
            self.hash_name, "description", self._serialize(description),
        )

    @property
    def graph(self) -> Delayed:
        """Job's dask graph"""
        return dask.delayed()(self.tasks, name=self.name + "_graph")

    @property
    def last_run(self) -> Optional[JobRun]:
        """Job's last run (JobRun object)"""
        _id = self._deserialize(self.con.get(f"{JobRun.prefix}{self.name}:id"))
        if _id:
            return JobRun(job_name=self.name, id=_id, logger=self.logger, db=self.db)

    @property
    def owner(self) -> str:
        """Job's owner"""
        return self._get("owner")

    @owner.setter
    @_check_if_exists()
    def owner(self, owner: str):
        self.con.hset(
            self.hash_name, "owner", self._serialize(owner),
        )

    @property
    def runs(self) -> List[JobRun]:
        """List of historical Job's runs"""
        return self.db.get_job_runs(job_name=self.name)

    @property
    def tasks(self) -> List[Delayed]:
        """List of Job's tasks"""
        return self._get("tasks", _type="dask")

    @tasks.setter
    @_check_if_exists()
    def tasks(self, tasks: List[Delayed]):
        self.con.hset(
            self.hash_name, "tasks", self._serialize(tasks),
        )

    @property
    def timeout(self) -> int:
        """Time after which job should stop running"""
        return self._get("timeout")

    @timeout.setter
    @_check_if_exists()
    def timeout(self, timeout: int):
        self.con.hset(
            self.hash_name, "timeout", self._serialize(timeout),
        )
        # need to reschedule to refresh job in rq
        self.crons = self.crons

    @property
    def _result_ttl(self) -> int:
        return self._get("_result_ttl")

    @_result_ttl.setter
    def _result_ttl(self, _result_ttl: int):
        self.con.hset(
            self.hash_name, "_result_ttl", self._serialize(_result_ttl),
        )

    @property
    def _rq_job_ids(self) -> List[str]:
        return self._get("_rq_job_ids")

    @_rq_job_ids.setter
    def _rq_job_ids(self, _rq_job_ids: List[str]):
        self.con.hset(
            self.hash_name, "_rq_job_ids", self._serialize(_rq_job_ids),
        )

    # TRIGGERS
    @property
    def triggers(self) -> List["Trigger"]:
        """List of Job's triggers"""
        trigger_names = self._get("triggers")
        triggers = [
            Trigger(name=trigger_name, logger=self.logger, db=self.db)
            for trigger_name in trigger_names
        ]
        return triggers

    @triggers.setter
    def triggers(self, triggers: Union[List[str], str]):
        if isinstance(triggers, str):
            triggers = [triggers]
        elif triggers is None:
            triggers = []

        # 1. Remove job from previous triggers
        old_triggers = self.triggers
        for trigger in old_triggers:
            trigger.remove_jobs(self.name)
        # 2. Add job to new triggers
        for new_trigger_name in triggers:
            new_trigger = Trigger(new_trigger_name, logger=self.logger, db=self.db)
            new_trigger.add_jobs(self.name)
        # 3. Update job with new triggers
        self.con.hset(
            self.hash_name, "triggers", self._serialize(triggers),
        )

    def add_triggers(self, trigger_names: Union[List[str], str]):
        """Add triggers to the Job.

        Parameters
        ----------
        trigger_names : Union[List[str], str]
            Name or list of names of triggers
        """
        added_trigger_names = self._add_values_to_list(key="triggers", new_values=trigger_names)

        for trigger_name in added_trigger_names:
            trigger = Trigger(name=trigger_name, logger=self.logger, db=self.db)
            if self not in trigger.jobs:
                trigger.add_jobs(self.name)

    def remove_triggers(self, trigger_names: Union[List[str], str]):
        """Remove triggers from the Job.

        Parameters
        ----------
        trigger_names : Union[List[str], str]
            Name or list of names of triggers
        """
        removed_trigger_names = self._remove_values(key="triggers", values=trigger_names)

        # remove the job from old triggers
        for trigger_name in removed_trigger_names:
            trigger = Trigger(trigger_name, logger=self.logger, db=self.db)
            if self in trigger.jobs:
                trigger.remove_jobs(self.name)

    # TRIGGERS END

    # DOWNSTREAM/UPSTREAM

    @property
    def downstream(self) -> Dict[str, SubmitCondition]:
        """Dictionary where keys are downstream jobs and values are conditions on
        which upstream job should submit downstream jobs"""
        return self._get("downstream")

    @property
    def downstream_jobs(self) -> List["Job"]:
        """List of downstream jobs"""
        downstream_job_names = list(self.downstream.keys())
        downstream_jobs = [
            Job(job_name, db=self.db, logger=self.logger) for job_name in downstream_job_names
        ]
        return downstream_jobs

    @downstream.setter
    @_check_if_exists()
    def downstream(self, jobs_with_conditions: Dict[str, SubmitCondition]):
        """Overwrite the dictionary of downstream jobs/submit conditions"""
        jobs_with_conditions = jobs_with_conditions or dict()
        self.db._check_if_jobs_exist(list(jobs_with_conditions.keys()))
        # 1. Remove current job from previous downstream jobs
        old_downstream_jobs = self.downstream_jobs
        for downstream_job in old_downstream_jobs:
            downstream_job.remove_upstream_jobs(self.name)
        # 2. Update new downstream jobs
        for new_downstream_job_name, condition in jobs_with_conditions.items():
            new_downstream_job = Job(new_downstream_job_name, db=self.db, logger=self.logger)
            new_downstream_job.update_upstream_jobs({self.name: condition})
        # 3. Update current job
        self.con.hset(
            self.hash_name, "downstream", self._serialize(jobs_with_conditions),
        )

    @_check_if_exists()
    def update_downstream_jobs(self, jobs_with_conditions: Dict[str, SubmitCondition]):
        """Update downstream jobs.

        jobs_with_conditions : Dict[str, SubmitCondition]
            Dictionary where keys are names of downstream jobs to be updated and values
            are conditions on which upstream job should submit downstream jobs
        """
        self.db._check_if_jobs_exist(list(jobs_with_conditions.keys()))

        # update current job
        changed_downstream = self._get_dict_data_diff(key="downstream", new=jobs_with_conditions)
        self._update_dict_values(key="downstream", new=changed_downstream)

        # update downstream jobs
        for job_name, condition in changed_downstream.items():
            downstream_job = Job(name=job_name, logger=self.logger, db=self.db)
            if downstream_job.upstream.get(self.name) != condition:
                downstream_job.update_upstream_jobs({self.name: condition})

    @_check_if_exists()
    def remove_downstream_jobs(self, job_names: Union[str, List[str]]):
        """Remove downstream jobs

        Parameters
        ----------
        job_names : str or list
            Name or list of names of downstream jobs to remove
        """

        removed_job_names = self._remove_values(key="downstream", values=job_names)

        # remove the job as an upstream of the specified jobs
        for job_name in removed_job_names:
            downstream_job = Job(job_name, db=self.db, logger=self.logger)
            if self.name in downstream_job.upstream:
                downstream_job.remove_upstream_jobs(self.name)

    @property
    def upstream(self) -> Dict[str, SubmitCondition]:
        """Dictionary where keys are upstream jobs and values are conditions on
        which upstream jobs should submit downstream job"""
        jobs_with_conditions = self._get("upstream")
        return jobs_with_conditions

    @property
    def upstream_jobs(self) -> List["Job"]:
        """List of upstream jobs"""
        upstream_job_names = list(self.upstream.keys())
        upstream_jobs = [
            Job(job_name, db=self.db, logger=self.logger) for job_name in upstream_job_names
        ]
        return upstream_jobs

    @upstream.setter
    @_check_if_exists()
    def upstream(self, jobs_with_conditions: Dict[str, SubmitCondition]):
        """Overwrite the dictionary of upstream jobs/submit conditions"""
        jobs_with_conditions = jobs_with_conditions or dict()
        self.db._check_if_jobs_exist(list(jobs_with_conditions.keys()))
        # 1. Remove from downstream jobs of all the jobs on the previous
        #    upstream jobs list
        old_upstream_jobs = self.upstream_jobs
        for upstream_job in old_upstream_jobs:
            upstream_job.remove_downstream_jobs(self.name)
        # 2. Update upstream jobs
        for new_upstream_job_name, condition in jobs_with_conditions.items():
            new_upstream_job = Job(new_upstream_job_name, db=self.db, logger=self.logger)
            new_upstream_job.update_downstream_jobs({self.name: condition})
        # 3. Update upstream jobs with the new job
        self.con.hset(
            self.hash_name, "upstream", self._serialize(jobs_with_conditions),
        )

    @_check_if_exists()
    def update_upstream_jobs(self, jobs_with_conditions: Dict[str, SubmitCondition]):
        """Update upstream jobs

        Parameters
        ----------
        jobs_with_conditions : Dict[str, SubmitCondition]
            Dictionary where keys are names of upstream jobs to be updated and values
            are conditions on which upstream jobs should submit downstream job
        """
        self.db._check_if_jobs_exist(list(jobs_with_conditions.keys()))

        # update current job
        changed_upstream = self._get_dict_data_diff(key="upstream", new=jobs_with_conditions)
        self._update_dict_values(key="upstream", new=changed_upstream)

        # update upstream jobs
        for job_name, condition in changed_upstream.items():
            upstream_job = Job(name=job_name, logger=self.logger, db=self.db)
            if upstream_job.downstream.get(self.name) != condition:
                upstream_job.update_downstream_jobs({self.name: condition})

    @_check_if_exists()
    def remove_upstream_jobs(self, job_names: Union[str, List[str]]):
        """Remove upstream jobs

        Parameters
        ----------
        job_names : str or list
            Name or list of names of upstream jobs to remove
        """

        removed_job_names = self._remove_values(key="upstream", values=job_names)

        # remove the job from the downstream jobs of the specified jobs
        for job_name in removed_job_names:
            upstream_job = Job(job_name, db=self.db, logger=self.logger)
            if self.name in upstream_job.downstream:
                upstream_job.remove_downstream_jobs(self.name)

    # DOWNSTREAM/UPSTREAM END

    def _cancel(self, scheduler_address: Optional[str] = None) -> None:
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
        description: Optional[str] = None,
        timeout: int = 3600,
        crons: Union[List[str], str] = None,
        upstream: Dict[str, SubmitCondition] = None,
        triggers: Union[List[str], str] = None,
        if_exists: Literal["fail", "replace"] = "fail",
        *args,
        **kwargs,
    ) -> "Job":
        """Register new job

        Returns
        -------
        Job
        """
        if self.exists:
            if if_exists == "fail":
                raise ValueError(f"{self} already exists")
            else:
                self.unregister(remove_job_runs=True)

        # VALIDATIONS
        # cron
        crons = crons or []
        if isinstance(crons, str):
            crons = [crons]
        for cron in crons:
            if not croniter.is_valid(cron):
                raise ValueError(f"Invalid cron string {cron}")
        # upstream
        upstream = upstream or dict()
        if self.name in upstream:
            raise ValueError("Job cannot be its own upstream job !!!")
        self.db._check_if_jobs_exist(list(upstream.keys()))
        # triggers
        triggers = triggers or []
        if isinstance(triggers, str):
            triggers = [triggers]

        mapping = {
            "owner": self._serialize(owner),
            "description": self._serialize(description),
            "timeout": self._serialize(timeout),
            "crons": self._serialize(crons),
            "upstream": self._serialize(upstream),
            "downstream": self._serialize(dict()),
            "triggers": self._serialize(triggers),
            "tasks": self._serialize(tasks),
            "args": self._serialize(args),
            "kwargs": self._serialize(kwargs),
            "created_at": self._serialize(datetime.now(timezone.utc)),
            "_rq_job_ids": self._serialize([]),
        }
        # if not (crons or upstream or triggers):
        #     raise ValueError("One of ['crons', 'upstream', 'triggers'] is required")

        self.con.hset(
            name=self.hash_name, key=None, value=None, mapping=mapping,
        )

        self._rq_job_ids = self.__add_to_scheduler(crons, *args, **kwargs)

        # add the job as downstream in all upstream jobs
        for upstream_job_name, condition in upstream.items():
            upstream_job = Job(name=upstream_job_name, logger=self.logger, db=self.db)
            upstream_job.update_downstream_jobs({self.name: condition})

        # add the job in all triggers
        for trigger_name in triggers:
            trigger = Trigger(name=trigger_name, logger=self.logger, db=self.db)
            trigger.add_jobs(self.name)

        self.con.set(f"{JobRun.prefix}{self.name}:id", "0")

        self.logger.info(f"{self} successfully registered")
        return self

    def unregister(self, remove_job_runs: bool = False) -> None:
        """Unregister existing job

        Parameters
        ----------
        remove_job_runs : bool, optional
            Whether to remove all job's runs history, by default False
        """

        # remove from rq scheduler
        self.__remove_from_scheduler()
        self._rq_job_ids = []

        # remove job from downstream in all upstream jobs
        for upstream_job in self.upstream_jobs:
            upstream_job.remove_downstream_jobs(self.name)

        # remove job from upstream in all downstream jobs
        for downstream_job in self.downstream_jobs:
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

    def _is_running(self):
        if self.last_run and self.last_run.status == "running":
            # and self.params == self.last_run.params -- TODO: add __eq__()
            # and check in running jobs registry using __eq__() rather than comparing to last run
            return True
        return False

    @_check_if_exists()
    def submit(
        self,
        client: Client = None,
        scheduler_address: str = None,
        priority: int = 1,
        to_dask: bool = True,
    ) -> Any:

        if self._is_running():
            msg = f"Job {self.name} is already running. Please use Job.stop() or Job.restart()"
            raise JobAlreadyRunningError(msg)

        if to_dask:
            if client is None:
                self.scheduler_address = scheduler_address or os.getenv(
                    "GRIZLY_DASK_SCHEDULER_ADDRESS"
                )
                client = Client(scheduler_address)
            else:
                self.scheduler_address = client.scheduler.address

        self.logger.info(f"Submitting job {self.name}...")
        job_run = JobRun(job_name=self.name, logger=self.logger, db=self.db)
        job_run.status = "running"

        start = time()
        try:
            result = self.graph.compute()
            job_run.status = "success"
        except Exception:
            result = [None]
            job_run.status = "fail"
            _, exc_value, _ = sys.exc_info()
            job_run.error = str(exc_value)

        job_run.result = result

        self.logger.info(f"Job {self} finished with status {job_run.status}")
        end = time()
        job_run.finished_at = datetime.now(timezone.utc)
        job_run.duration = int(end - start)

        conditions_flags = self.__check_conditions(job_run)

        for condition, flag in conditions_flags.items():
            if flag:
                self.__submit_downstream_jobs(condition=condition)

        if to_dask:
            client.close()

        return result

    def __check_conditions(self, job_run: JobRun) -> Dict[SubmitCondition, bool]:
        # result_change
        # if it was the first run then result_change is True
        if job_run._id == 1:
            result_change_flag = True
        else:
            prev_run = self.runs[-2]
            result_change_flag = prev_run.result != job_run.result

        conditions_flags = {
            "success": job_run.status == "success",
            "fail": job_run.status == "fail",
            "result_change": result_change_flag,
        }

        return conditions_flags

    def __submit_downstream_jobs(self, condition: SubmitCondition):
        jobs = [
            Job(job_name) for job_name, job_cond in self.downstream.items() if job_cond == condition
        ]
        if jobs:
            self.logger.info(
                f"Enqueueing {self} downstream jobs with submit condition {condition}..."
            )
            queue = Queue(
                SchedulerDB.submit_queue_name, connection=self.con, default_timeout=self.timeout
            )
            for job in jobs:
                # TODO: should read downstream *args ad **kwargs from registry
                rq_job = queue.enqueue(
                    job.submit,
                    scheduler_address=self.scheduler_address,
                    result_ttl=job._result_ttl,
                    job_timeout=self.timeout,
                )
                job._rq_job_ids = list(set(job._rq_job_ids) | {rq_job.id})
                self.logger.debug(f"{job} has been added to rq scheduler with id {rq_job.id}")
                self.logger.info(f"{job} has been enqueued")
        else:
            self.logger.debug(f"No {self} downstream jobs with condition '{condition}' found")

    @_check_if_exists()
    def visualize(self, **kwargs):
        return self.graph.visualize(**kwargs)

    def __add_to_scheduler(self, crons: List[str], *args, **kwargs):
        rq_job_ids = []
        if crons:
            queue = Queue(
                SchedulerDB.submit_queue_name, connection=self.con, default_timeout=self.timeout
            )
            scheduler = Scheduler(queue=queue, connection=self.con)
            for cron in crons:
                rq_job = scheduler.cron(
                    cron,
                    func=self.submit,
                    args=args,
                    kwargs=kwargs,
                    repeat=None,
                    queue_name=queue.name,
                    timeout=self.timeout,
                )
                self.logger.debug(
                    f"{self} with cron '{cron}' has been added to rq sheduler with id {rq_job.id}"
                )
                rq_job_ids.append(rq_job.id)
            self.logger.debug(f"{self} has been added to the rq scheduler")

        return rq_job_ids

    def __remove_from_scheduler(self):
        rq_job_ids = self._rq_job_ids
        if rq_job_ids:
            queue = Queue(SchedulerDB.submit_queue_name, connection=self.con)
            scheduler = Scheduler(queue=queue, connection=self.con)
            for rq_job_id in rq_job_ids:
                try:
                    scheduler.cancel(rq_job_id)
                    RqJob.fetch(rq_job_id, connection=self.con).delete()
                    self.logger.debug(f"Rq job {rq_job_id} removed from the rq scheduler")
                except NoSuchJobError:
                    pass

            self.logger.debug(f"{self} has been removed from the rq scheduler")


class Trigger(SchedulerObject):
    prefix = "grizly:registry:triggers:"

    def info(self):
        pass

    @property
    def is_triggered(self) -> bool:
        return self._get("is_triggered")

    @is_triggered.setter
    @_check_if_exists()
    def is_triggered(self, value: bool):
        self.con.hset(
            self.hash_name, "is_triggered", self._serialize(value),
        )

    @property
    def jobs(self) -> List[Optional["Job"]]:
        job_names = self._get("jobs")
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

        self._add_values_to_list(key="jobs", new_values=job_names)

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
