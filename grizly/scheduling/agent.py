#!/usr/local/bin/python3

from croniter import croniter
from datetime import datetime, timedelta
import logging
import json

from grizly.tools.qframe import QFrame, join
from grizly.config import Config
from grizly.scheduling.job import Job


def get_scheduled_jobs():
    config = Config().get_service(service="schedule")
    dsn = config.get("dsn")
    schema = config.get("schema")
    job_registry_table = config.get("job_registry_table")
    job_status_table = config.get("job_status_table")

    registry_qf = QFrame(dsn=dsn).from_table(table=job_registry_table, schema=schema)
    registry_qf.query("trigger ->> 'class' = 'Schedule'")

    status_qf = QFrame(dsn=dsn).from_table(table=job_status_table, schema=schema)
    status_qf.select(["job_id", "status", "run_date"]).groupby(["job_id", "status"])["run_date"].agg("max")

    qf = join([registry_qf, status_qf], join_type="left join", on="sq1.id=sq2.job_id")

    return qf.to_records()

def run():
    results = get_scheduled_jobs()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    logger.info(datetime.today().__str__())
    logger.warning("Loading jobs...")
    records = get_scheduled_jobs()
    logger.info("Jobs loaded successfully")
    logger.debug(records)
    logger.info("Checking scheduled jobs...")

    def nonesafe_loads(obj):
        """To avoid errors if json is None"""
        if obj is not None:
            return json.loads(obj)

    for _, name, owner, type, notification, trigger, source, source_type, _, _, status, last_run in records:
        cron_str = nonesafe_loads(trigger)["cron"]
        start_date = last_run or datetime.now()
        cron = croniter(cron_str, start_date)
        next_run = cron.get_next(datetime)
        logger.warning(f"{next_run}, {name}")

        if status != "submitted" and next_run < datetime.now() + timedelta(minutes=1):
            job = Job(
                name=name,
                owner=owner,
                source=nonesafe_loads(source),
                trigger=nonesafe_loads(trigger),
                notification=nonesafe_loads(notification),
                env="prod",
                logger=logging.getLogger("distributed.worker.test"),
            )
            logger.warning(f"Submitting job {name}...")
            job.submit()
