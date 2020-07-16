import importlib
import os

from ..tools.qframe import QFrame
from ..tools.s3 import S3
from ..config import Config


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
        file_name = _download_script_from_s3(url=source, file_dir=file_dir)
        module = importlib.import_module(file_name[:-3])
        try:
            tasks = module.tasks
        except AttributeError:
            raise AttributeError("Please specify tasks in your script")

        os.remove(file_name)
        return tasks
    else:
        raise NotImplementedError()


config = Config().get_service(service="schedule")
dsn = config.get("dsn")
schema = config.get("schema")
job_registry_table = config.get("job_registry_table")
job_status_table = config.get("job_status_table")


qf = QFrame(dsn=dsn).from_table(table=job_registry_table, schema=schema)

qf.query("trigger ->> 'class' = 'Schedule'")

records = qf.to_records()
for record in records:
    if type == "SCHEDULE":
        tasks = get_tasks()
        result = Job(tasks).submit()
        if result:
            jobs_to_run = map_trigger_to_jobs
            queque.put(jobs_to_run)

