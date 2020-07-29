#!/usr/local/bin/python3

from croniter import croniter
from datetime import datetime, timedelta
import logging
from typing import List

from grizly.tools.qframe import QFrame
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
    records = registry_qf.to_records()
    jobs = []
    for record in records:
        job = Job(name=record[0])
        jobs.append(job)
    return jobs


def get_triggers() -> List[Trigger]:
    job_trigger_table = config.get("job_triggers_table")
    qf = QFrame(dsn=dsn).from_table(table=job_trigger_table, schema=schema, columns=["name"])
    records = qf.to_records()
    triggers = []
    for record in records:
        trigger = Trigger(name=record[0])
        triggers.append(trigger)
    return triggers


def run():
    redis = Redis(host="10.125.68.177", port=80)
    checks_queue = Queue("checks_queue", connection=redis)
    submit_queue = Queue("submit_queue", connection=redis)
    logger = logging.getLogger("distributed.worker").getChild("agent")

    logger.info("Loading jobs...")
    jobs = get_jobs()
    logger.info("Jobs loaded successfully")

    logger.info("Loading triggers...")
    triggers = get_triggers()
    logger.info("Triggers loaded successfully")

    logger.info("Checking scheduled jobs...")
    periodic_jobs = [job for job in jobs if job.trigger_type == "cron"]
    for job in periodic_jobs:
        last_run = None
        start_date = last_run or datetime.now()
        cron_str = job.trigger_value
        cron = croniter(cron_str, start_date)
        next_run = cron.get_next(datetime)
        if next_run < datetime.now() + timedelta(minutes=1):
            if job.type == "regular":
                submit_queue.enqueue(job.submit)
                logger.info(f"Job {job.name} has been successfully submitted to submit queue")
            elif job.type == "listener":
                checks_queue.enqueue(job.submit)
                logger.info(f"Job {job.name} has been successfully submitted to chcks queue")

    logger.info("Submitting triggered jobs...")
    for trigger in triggers:
        if trigger.is_triggered:
            jobs_to_run = trigger.get_jobs()
            for job in jobs_to_run:
                submit_queue.enqueue(job.submit)
                logger = logger.info(
                    f"Job {job.name} has been successfully submitted to submit queue"
                )
            trigger.set(False)


if __name__ == "__main__":
    run()
