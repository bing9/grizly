#!/usr/bin/env python3

import click
import sys
import os
import importlib

DEV_SCHEDULER_ADDRESS = os.getenv("GRIZLY_DEV_DASK_SCHEDULER_ADDRESS")
PROD_SCHEDULER_ADDRESS = os.getenv("GRIZLY_DASK_SCHEDULER_ADDRESS")
WORKFLOWS_HOME = os.getenv("GRIZLY_WORKFLOWS_HOME")

sys.path.insert(0, WORKFLOWS_HOME)


def get_workflow(workflow_name):
    script_name = workflow_name.lower().replace(" ", "_")
    module_path = f"workflows.{script_name}.{script_name}"
    module = importlib.import_module(module_path)
    wf = module.generate_workflow(logger_name=script_name)
    return wf


@click.group(hidden=True)
def workflow():
    """
    Run, schedule, and monitor workflows

    \b
    Usage:
        $ grizly workflow [COMMAND] [WORKFLOW_NAME] [PARAMS]
    \b
    Arguments:
        run         Initialize a manual run
        cancel      Remove a running or failed workflow from the scheduler
    \b
    Examples:
        $ grizly workflow run "Sales Daily News"
        Running workflow Sales Daily News...
    \b
        $ grizly workflow cancel "Sales Daily News"
        Cancelling workflow Sales Daily News...

    """
    pass


@workflow.command(hidden=True)
@click.argument("workflow_name", type=str)
@click.option("--dev", "-d", is_flag=True, default=False)
def run(workflow_name, dev):
    """Manually initiate a workflow run"""
    
    print(f"Running workflow {workflow_name}...")

    wf = get_workflow(workflow_name)
    if dev:
        scheduler_address = DEV_SCHEDULER_ADDRESS
    else:
        scheduler_address = PROD_SCHEDULER_ADDRESS
    wf.submit(scheduler_address=scheduler_address)

    print(f"Workflow has been successfully submitted to {scheduler_address}")


@workflow.command(hidden=True)
@click.argument("workflow_name", type=str)
@click.option("--dev", "-d", is_flag=True, default=False)
def cancel(workflow_name, dev):
    """Remove a running or finished workflow from the scheduler"""
    
    print(f"Cancelling workflow {workflow_name}...")
    
    wf = get_workflow(workflow_name)
    if dev:
        scheduler_address = DEV_SCHEDULER_ADDRESS
    else:
        scheduler_address = PROD_SCHEDULER_ADDRESS
    wf.cancel(scheduler_address=scheduler_address)

    print("Workflow has been successfully cancelled")

