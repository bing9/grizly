from distributed import Client, Future, progress
import dask
import logging
from typing import Any, Dict, List, Literal
import os
import sys
from time import time
import traceback
import json

from ..tools.qframe import QFrame, join
from ..config import Config
from ..tools.s3 import S3
from ..utils import get_path
from .tables import JobRegistryTable, JobNTriggersTable, JobRunsTable
from functools import cached_property


class Trigger:
    """placeholder for type"""

    pass


class Job:
    def __init__(
        self, name: str, job_registry_record: tuple = None, logger: logging.Logger = None,
    ):
        self.name = name
        self.logger = logger or logging.getLogger(__name__)
        self.config = Config().get_service(service="schedule")
        self.job_registry_record = job_registry_record
        self.job_runs_record = job_runs_record

    @cached_property
    def job_registry_record(self):
        if self.job_registry_record is None:
            return JobRegistryTable(logger=self.logger)._get_job(job_name=self.name)
        else:
            self.job_registry_record

    @cached_property
    def job_runs_record(self):
        if self.job_runs_record is None:
            return JobRunsTable(logger=self.logger)._get_last_job_run(self.id)
        else:
            self.job_runs_record

    @property
    def id(self):
        return self.job_registry_record[0]

    @property
    def type(self):
        if self.id:
            return self.job_registry_record[2]

    @property
    def inputs(self):
        inputs = self.job_registry_record[3]

        def nonesafe_loads(obj):
            """To avoid errors if json is None"""
            if obj is not None:
                return json.loads(obj)

        return nonesafe_loads(inputs)

    @property
    def created_at(self):
        return self.job_registry_record[4]

    @property
    def last_run(self):
        if self.id:
            return self.job_runs_record[2]

    @property
    def status(self):
        if self.id:
            return self.job_runs_record[4]
        else:
            self.logger.warning("You need to register a job before calling job.status")

    @property
    def trigger_type(self):
        dsn = self.config.get("dsn")
        schema = self.config.get("schema")
        job_triggers_table = self.config.get("job_triggers_table")
        job_n_triggers_table = self.config.get("job_n_triggers_table")
        qf1 = QFrame(dsn=dsn).from_table(table=job_triggers_table, schema=schema)
        qf2 = QFrame(dsn=dsn).from_table(table=job_n_triggers_table, schema=schema)
        qf2.query(f"job_id = {self.id}")
        on = "sq1.id = sq2.trigger_id"
        qf_join = join(qframes=[qf1, qf2], join_type="INNER JOIN", on=on)
        df = qf_join.to_df()
        return df.loc[0, "type"]

    @property
    def trigger_value(self):
        dsn = self.config.get("dsn")
        schema = self.config.get("schema")
        job_triggers_table = self.config.get("job_triggers_table")
        job_n_triggers_table = self.config.get("job_n_triggers_table")
        qf1 = QFrame(dsn=dsn).from_table(table=job_triggers_table, schema=schema)
        qf2 = QFrame(dsn=dsn).from_table(table=job_n_triggers_table, schema=schema)
        qf2.query(f"job_id = {self.id}")
        on = "sq1.id = sq2.trigger_id"
        qf_join = join(qframes=[qf1, qf2], join_type="INNER JOIN", on=on)
        df = qf_join.to_df()
        return df.loc[0, "value"]

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
            file_name = _download_script_from_s3(
                url=self.inputs["artifact"]["main"], file_dir=file_dir
            )
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

    def __repr__(self):
        return f"Job({self.name})"

    def update_status(self, status):
        _id = JobRunsTable(logger=self.logger)._get_last_job_run_id(job_id=self.id)
        job_run = JobRun(id=_id, job_id=self.id)
        job_run.update(status=status)

    def register(
        self, triggers: List[Trigger], type: str, inputs: Dict[str, Any] = None,
    ):
        job_id = JobRegistryTable(logger=self.logger).register(
            name=self.name, type=type, inputs=inputs
        )
        # trigger_id = JobTriggersTable(logger=self.logger).register(trigger=triggers[0])
        JobNTriggersTable(logger=self.logger).register(job_id=job_id, trigger_id=triggers[0].id)
        return self

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
            self.scheduler_address = scheduler_address or os.getenv(
                "GRIZLY_DEV_DASK_SCHEDULER_ADDRESS"
            )
            client = Client(self.scheduler_address)
        else:
            self.scheduler_address = client.scheduler.address

        if not client or self.scheduler_address:
            raise ValueError("distributed.Client/scheduler address was not provided")

        self.logger.info(f"Submitting job {self.name}...")
        job_run = JobRun(job_id=self.id, status="running")
        job_run.register()
        start = time()
        try:
            self.graph.compute()
            _status = "success"
        # TODO: Catch and save errors in status table
        except Exception as e:
            _status = "fail"
            # exc_type, exc_value, exc_tb = sys.exc_info()
            # error_value = str(exc_value)
            # error_type = type(exc_value)
            # error_message = traceback.format_exc()
            # self.logger.exception(f"Job {self.name} finished with status 'fail'")

        end = time()
        run_time = int(end - start)
        job_run.update(status=_status, run_time=run_time)

        self.logger.info(f"Job {self.name} finished with status {job_run.status}")

        if not client:  # if cient is provided, we assume the user will close it
            client.close()

    def cancel(self, scheduler_address=None):
        if not scheduler_address:
            scheduler_address = self.scheduler_address
        client = Client(scheduler_address)
        f = Future(self.name + "_graph", client=client)
        f.cancel(force=True)
        client.close()


class JobRun:
    def __init__(
        self,
        id: int = None,
        job_id: int = None,
        run_time: int = None,
        status: str = None,
        logger: logging.Logger = None,
    ):
        self.id = id
        self.job_id = job_id
        self.run_time = run_time
        self.status = status
        self.logger = logger or logging.getLogger(__name__)

    def register(self):
        self.id = JobRunsTable(logger=self.logger).register(job_run=self)
        return self

    def update(self, **kwargs):
        self.run_time = kwargs.get("run_time") or self.run_time
        self.status = kwargs.get("status") or self.status
        JobRunsTable(logger=self.logger).update(id=self.id, **kwargs)

