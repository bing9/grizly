import importlib
import os
from croniter import croniter
from datetime import datetime, timezone, timedelta
import logging

from grizly.tools.qframe import QFrame, join
from grizly.tools.s3 import S3
from grizly.config import Config
from grizly.scheduling.job import Job


# TODO: get_tasks should be a part of Job class
def get_tasks(source, source_type):
    file_dir = os.getcwd()

    def _download_script_from_s3(url, file_dir):
        bucket = url.split("/")[2]
        file_name = url.split("/")[-1]
        s3_key = "/".join(url.split("/")[3:-1])
        s3 = S3(bucket=bucket, file_name=file_name, s3_key=s3_key, file_dir=file_dir)
        s3.to_file()

        return s3.file_name

    if source_type == "s3":
        file_name = _download_script_from_s3(url=eval(source)["main"], file_dir=file_dir)
        module = importlib.import_module(file_name[:-3])
        try:
            tasks = module.tasks
        except AttributeError:
            raise AttributeError("Please specify tasks in your script")

        os.remove(file_name)
        return tasks
    else:
        raise NotImplementedError()


def get_scheduled_jobs():
    config = Config().get_service(service="schedule")
    dsn = config.get("dsn")
    schema = config.get("schema")
    job_registry_table = config.get("job_registry_table")
    job_status_table = config.get("job_status_table")

    registry_qf = QFrame(dsn=dsn).from_table(table=job_registry_table, schema=schema)
    registry_qf.query("trigger ->> 'class' = 'Schedule'")

    status_qf = QFrame(dsn=dsn).from_table(table=job_status_table, schema=schema)
    status_qf.select(["job_id", "run_date"]).groupby(["job_id"])["run_date"].agg("max")

    qf = join([registry_qf, status_qf], join_type="left join", on="sq1.id=sq2.job_id")

    return qf.to_records()


if __name__ == "__main__":
    records = get_scheduled_jobs()
    print(records)
    for _, name, owner, type, notification, trigger, source, source_type, _, _, last_run in records:
        cron_str = eval(trigger)["cron"]
        start_date = last_run or datetime.now()
        cron = croniter(cron_str, start_date)
        next_run = cron.get_next(datetime)

        if next_run < start_date + timedelta(minutes=1):
            tasks = get_tasks(source=source, source_type=source_type)
            print(name, tasks)
            job = Job(
                name=name,
                owner=owner,
                source=source,
                source_type=source_type,
                tasks=tasks,
                trigger=trigger,
                notification=notification,
                env="prod",
                logger=logging.getLogger("distributed.worker.test"),
            )
            job.submit()
