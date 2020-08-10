from datetime import datetime, timezone
import json
import pytest

import dask
from hypothesis import given
import redis

from hypothesis.strategies import integers, text, lists

from ..grizly.scheduling.job import Job


@dask.delayed
def add(x, y):
    return x + y


task1 = add(1, 2)
tasks = [task1]

con = redis.Redis(host="pytest_redis")


@dask.delayed
def add(x, y):
    return x + y


task1 = add(1, 2)
tasks = [task1]

con = redis.Redis(host="pytest_redis")


@given(text(), text())
def test_job_owner(name, owner):
    job = Job(name)
    job.register(tasks=tasks, owner=owner)
    assert job.owner == owner
    job.remove()


@given(text())
def test_job_tasks(name):
    job = Job(name)
    job.register(tasks=tasks)
    assert job.tasks == tasks
    job.remove()


@given(text())
def test_register_job(name):
    """test if job with any name is registred"""
    job = Job(name)
    job.register(tasks=tasks)
    assert job.exists
    job.remove()

    assert not job.exists

# @given(text(), text())
# def test_job_trigger_names(job_name, trigger_name):
#     job = Job(job_name)
#     job.register(tasks=tasks)
#     assert job.trigger_names == []

#     with pytest.raises(ValueError):
#         job.trigger_names = ["not_found_trigger"]

#     tr = Trigger(name=trigger_name)
#     tr.register(type="cron", value="*/3 * * * *")
#     job.trigger_names = [trigger_name]
#     assert job.trigger_names == [trigger_name]
#     job.remove()
#     tr.remove()

@given(text())
def test_job_add_downstream_jobs(name):
    j = Job(name)


@given(text())
def test_job_remove_upstream_jobs(name):
    j = Job(name)
    