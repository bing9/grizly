#!/usr/bin/env python3

import click
import sys
import os
import importlib

DEV_SCHEDULER_ADDRESS = os.getenv("GRIZLY_DEV_DASK_SCHEDULER_ADDRESS")
PROD_SCHEDULER_ADDRESS = os.getenv("GRIZLY_DASK_SCHEDULER_ADDRESS")
WORKFLOWS_HOME = os.getenv("GRIZLY_WORKFLOWS_HOME")

sys.path.insert(0, WORKFLOWS_HOME)


def get_job(job_name):
    script_name = job_name.lower().replace(" ", "_")
    folder_name = script_name
    if "check" in script_name:
        folder_name = script_name.replace("_control_check", "")
    module_path = f"jobs.{folder_name}.{script_name}"
    module = importlib.import_module(module_path)
    wf = module.generate_job(logger_name=script_name)
    return wf


# def get_job(job_name, source):
#     """get job source"""
#     if source.lower().startswith("https://github.com"):
#         _get_job_from_github(source)
#     pass


@click.group(hidden=True)
def job():
    """
    Run, schedule, and monitor jobs

    \b
    Usage:
        $ grizly job [COMMAND] [WORKFLOW_NAME] [PARAMS]
    \b
    Arguments:
        run         Initialize a manual run
        cancel      Remove a running or failed job from the scheduler
    \b
    Examples:
        $ grizly job run "Sales Daily News"
        Running job Sales Daily News...
    \b
        $ grizly job cancel "Sales Daily News"
        Cancelling job Sales Daily News...

    """
    pass


@job.command(hidden=True)
@click.argument("job_name", type=str)
@click.option("--local", "-l", is_flag=True, default=False)
@click.option("--dev", "-d", is_flag=True, default=False)
def run(job_name, local, dev):
    """Manually initiate a job run"""

    print(f"Running job {job_name}...")

    wf = get_job(job_name)
    if local:
        scheduler_address = None
    elif dev:
        scheduler_address = DEV_SCHEDULER_ADDRESS
    else:
        scheduler_address = PROD_SCHEDULER_ADDRESS
    wf.submit(scheduler_address=scheduler_address)

    print(f"Workflow has been successfully submitted to {scheduler_address or 'localhost:8786'}")


@job.command(hidden=True)
@click.argument("job_name", type=str)
@click.option("--local", "-l", is_flag=True, default=False)
@click.option("--dev", "-d", is_flag=True, default=False)
def cancel(job_name, local, dev):
    """Remove a running or finished job from the scheduler"""

    print(f"Cancelling job {job_name}...")

    wf = get_job(job_name)
    if local:
        scheduler_address = None
    elif dev:
        scheduler_address = DEV_SCHEDULER_ADDRESS
    else:
        scheduler_address = PROD_SCHEDULER_ADDRESS
    wf.cancel(scheduler_address=scheduler_address)

    print("Workflow has been successfully cancelled")


@job.command(hidden=True)
@click.argument("job_name", type=str)
@click.argument("job_name", type=str)
@click.argument("source", type=str)
@click.argument("cron", type=str)
def schedule(job_name, source, cron):
    """Schedule a job"""

    wf = get_job(job_name)
    wf.register(name=job_name, schedule_type="schedule", cron=cron)
    # notification: recipients, cc

    print(f"Job {job_name} has been successfully scheduled")


# grizly job schedule "My Job" "github.com/my_job" "* * * * *" --notification={recipients=[a, b]}
