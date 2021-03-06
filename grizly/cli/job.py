#!/usr/bin/env python3

# import click
# import sys
# import os
# import importlib
# from typing import Any, Dict, List

# from ..scheduling.job import Job
# from ..tools.s3 import S3

# DEV_SCHEDULER_ADDRESS = os.getenv("GRIZLY_DEV_DASK_SCHEDULER_ADDRESS")
# PROD_SCHEDULER_ADDRESS = os.getenv("GRIZLY_DASK_SCHEDULER_ADDRESS")
# WORKFLOWS_HOME = os.getenv("GRIZLY_WORKFLOWS_HOME")

# sys.path.insert(0, WORKFLOWS_HOME)


# @click.group(hidden=True)
# def job():
#     """
#     Run, schedule, and monitor jobs

#     \b
#     Usage:
#         $ grizly job [COMMAND] [WORKFLOW_NAME] [PARAMS]
#     \b
#     Arguments:
#         run         Initialize a manual run
#         cancel      Remove a running or failed job from the scheduler
#     \b
#     Examples:
#         $ grizly job run "Sales Daily News"
#         Running job Sales Daily News...
#     \b
#         $ grizly job cancel "Sales Daily News"
#         Cancelling job Sales Daily News...

#     """
#     pass


# @job.command(hidden=True)
# @click.argument("job_name", type=str)
# @click.option("--local", "-l", is_flag=True, default=False)
# @click.option("--dev", "-d", is_flag=True, default=False)
# def run(job_name, local, dev):
#     """Manually initiate a job run"""

#     print(f"Running job {job_name}...")

#     wf = get_job(job_name)
#     if local:
#         scheduler_address = None
#     elif dev:
#         scheduler_address = DEV_SCHEDULER_ADDRESS
#     else:
#         scheduler_address = PROD_SCHEDULER_ADDRESS
#     wf.submit(scheduler_address=scheduler_address)

#     print(f"Workflow has been successfully submitted to {scheduler_address or 'localhost:8786'}")


# @job.command(hidden=True)
# @click.argument("job_name", type=str)
# @click.option("--local", "-l", is_flag=True, default=False)
# @click.option("--dev", "-d", is_flag=True, default=False)
# def cancel(job_name, local, dev):
#     """Remove a running or finished job from the scheduler"""

#     print(f"Cancelling job {job_name}...")

#     wf = get_job(job_name)
#     if local:
#         scheduler_address = None
#     elif dev:
#         scheduler_address = DEV_SCHEDULER_ADDRESS
#     else:
#         scheduler_address = PROD_SCHEDULER_ADDRESS
#     wf.cancel(scheduler_address=scheduler_address)

#     print("Workflow has been successfully cancelled")


# def get_tasks(source, source_type):
#     file_dir = os.getcwd()

#     def _download_script_from_s3(url, file_dir):
#         bucket = url.split("/")[2]
#         file_name = url.split("/")[-1]
#         s3_key = "/".join(url.split("/")[3:-1])
#         s3 = S3(bucket=bucket, file_name=file_name, s3_key=s3_key, file_dir=file_dir)
#         s3.to_file()

#         return s3.file_name

#     if source_type == "s3":
#         file_name = _download_script_from_s3(url=source, file_dir=file_dir)
#         module = importlib.import_module(file_name[:-3])
#         try:
#             tasks = module.tasks
#         except AttributeError:
#             raise AttributeError("Please specify tasks in your script")

#         os.remove(file_name)
#         return tasks
#     else:
#         raise NotImplementedError()


# def get_job(job_name, source):
#     """get job source"""
#     if source.lower().startswith("https://github.com"):
#         _get_job_from_github(source)
#     pass


# @job.command(hidden=True)
# @click.argument("name", type=str)
# @click.argument("owner", type=str)
# @click.argument("source", type=str)
# @click.argument("trigger", type=Dict[str, Any])
# @click.option("--source_type", "-st", is_flag=True, default=False)
# @click.option("--notification", "-n", is_flag=True, default=False)
# def schedule(name, owner, source, trigger, source_type=None, notification=None):
#     """Schedule a job"""

#     source_type = source_type or "local"
#     tasks = get_tasks(source=source, source_type=source_type)
#     job = Job(
#         name=name,
#         owner=owner,
#         source=source,
#         source_type=source_type,
#         tasks=tasks,
#         trigger=trigger,
#         notification=notification,
#         env="prod",
#     )

#     job.register()

#     print(f"Job {name} has been successfully scheduled")


# grizly job schedule "My Job" "github.com/my_job" "* * * * *" --notification={recipients=[a, b]}
