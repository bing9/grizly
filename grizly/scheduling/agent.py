#!/usr/local/bin/python3

from croniter import croniter
from datetime import datetime, timedelta
import logging
import json
from typing import List

from grizly.tools.qframe import QFrame, join
from grizly.config import Config
from grizly.scheduling.job import Job
from grizly.scheduling.trigger import Trigger

from rq import Queue
from redis import Redis

config = Config().get_service(service="schedule")
dsn = config.get("dsn")
schema = config.get("schema")


def get_jobs() -> List[Job]:
    job_registry_table = config.get("job_registry_table")
    registry_qf = QFrame(dsn=dsn).from_table(
        table=job_registry_table, schema=schema, columns=["name"]
    )
    records = registry_qf.to_reords()
    jobs = []
    for record in records:
        job = Job(name=record[0])
        jobs.append(job)
    return jobs


def get_triggers() -> List[Trigger]:
    job_trigger_table = config.get("job_triggers_table")
    qf = QFrame(dsn=dsn).from_table(table=job_trigger_table, schema=schema, columns=["name"])
    records = qf.to_reords()
    triggers = []
    for record in records:
        trigger = Trigger(name=record[0])
        triggers.append(trigger)
    return triggers


def run():
    redis = Redis()
    checks_queue = Queue("checks_queue", connection=redis)
    submit_queue = Queue("submit_queue", connection=redis)
    logger = logging.getLogger(__name__)

    logger.info("Loading jobs...")
    jobs = get_jobs()
    logger.info("Jobs loaded successfully")

    logger.info("Checking scheduled jobs...")
    periodic_jobs = [job for job in jobs if job.trigger_type == "cron"]
    for job in periodic_jobs:
        last_run = None
        start_date = last_run or datetime.now()
        cron_str = job.trigger_value()
        cron = croniter(cron_str, start_date)
        next_run = cron.get_next(datetime)
        if next_run < datetime.now() + timedelta(minutes=1):
            if job.type == "regular":
                submit_queue.enqueue(job.submit())
            elif job.type == "listener":
                checks_queue.enqueue(job.submit())

    logger.info("Submitting triggered jobs...")
    for trigger in triggers_table:
        if trigger.is_triggered:
            jobs_to_run = Trigger.get_jobs()
            for job in jobs_to_run:
                submit_queue.enqueue(job)
            trigger.set(False)


if __name__ == "__main__":
    jobs = get_jobs()
    print(jobs)
    # run()
