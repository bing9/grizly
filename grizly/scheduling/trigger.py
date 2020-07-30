from functools import cached_property
from .tables import JobTriggersTable
from .job import Job
from ..config import Config
from ..tools.qframe import QFrame, join
import logging
from typing import List


class Trigger:
    def __init__(
        self, name: str, logger: logging.Logger = None,
    ):
        self.name = name
        self.logger = logger or logging.getLogger(__name__)
        self.config = Config().get_service(service="schedule")

    @cached_property
    def jobtriggers_table_entry(self):
        return JobTriggersTable(logger=self.logger)._get_trigger(self.name)

    @property
    def id(self):
        return self.jobtriggers_table_entry[0]

    @property
    def type(self):
        return self.jobtriggers_table_entry[2]

    @property
    def value(self):
        return self.jobtriggers_table_entry[3]

    @property
    def is_triggered(self):
        return self.jobtriggers_table_entry[4]

    def set(self, triggered: bool):
        JobTriggersTable(logger=self.logger).update(id=self.id, is_triggered=triggered)

    def register(self, type: str, value: str):
        JobTriggersTable(logger=self.logger).register(name=self.name, type=type, value=value)
        return self

    def get_jobs(self) -> List[Job]:
        job_registry_table = self.config.get("job_registry_table")
        jobntriggers_table = self.config.get("job_n_triggers_table")
        dsn = self.config.get("dsn")
        schema = self.config.get("schema")
        registry_qf = QFrame(dsn=dsn).from_table(table=job_registry_table, schema=schema)
        jobntriggers_qf = (
            QFrame(dsn=dsn)
            .from_table(table=jobntriggers_table, schema=schema)
            .query(f"trigger_id = {self.id}")
        )

        joined = join(
            [registry_qf, jobntriggers_qf], join_type="inner join", on="sq1.id = sq2.job_id"
        )
        records = joined.to_records()
        jobs = []
        for record in records:
            job = Job(name=record[0])
            jobs.append(job)
        return jobs
