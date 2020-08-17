from datetime import datetime, timezone
import json
import pytest

import dask
from hypothesis import given
import redis

from hypothesis.strategies import integers, text, lists

from ..grizly.scheduling.registry import Job
from ..grizly.exceptions import JobNotFoundError


@dask.delayed
def add(x, y):
    return x + y


task1 = add(1, 2)
tasks = [task1]

con = redis.Redis(host="pytest_redis")

# PROPERTIES
# ----------


@given(text())
def test_job_exists(name):
    job = Job(name)
    job.register(tasks=tasks, crons="* * * * *", if_exists="replace")
    assert job.exists

    job.remove()
    assert not job.exists


@given(text(), text(), text())
def test_job_owner(name, owner_1, owner_2):
    job = Job(name)
    job.register(tasks=tasks, crons="* * * * *", owner=owner_1, if_exists="replace")
    assert job.owner == owner_1

    job.owner = owner_2
    assert job.owner == owner_2

    job.remove()


@given(text())
def test_job_tasks(name):
    job = Job(name)
    job.register(tasks=tasks, crons="* * * * *", if_exists="replace")
    assert job.tasks == tasks

    job.remove()


@given(text(), text())
def test_job_downstream(job_name, downstream_job_name):
    job = Job(job_name)
    job.register(tasks=tasks, crons="* * * * *", if_exists="replace")
    assert job.downstream == []

    # trying to set not existing job as downstream should raise error
    with pytest.raises(JobNotFoundError):
        job.downstream = ["not_found_job"]

    downstream_job = Job(downstream_job_name)
    downstream_job.register(tasks=tasks, crons="* * * * *", if_exists="replace")
    job.downstream = [downstream_job_name]
    assert job.downstream == [downstream_job]

    job.remove()
    downstream_job.remove()


@given(text(), text())
def test_job_upstream(job_name, upstream_job_name):
    job = Job(job_name)
    job.register(tasks=tasks, crons="* * * * *", if_exists="replace")
    assert job.upstream == []

    # trying to set not existing job as downstream should raise error
    with pytest.raises(JobNotFoundError):
        job.upstream = ["not_found_job"]

    upstream_job = Job(upstream_job_name)
    upstream_job.register(tasks=tasks, crons="* * * * *", if_exists="replace")
    job.upstream = [upstream_job_name]
    assert job.upstream == [upstream_job]

    job.remove()
    upstream_job.remove()


# METHODS
# -------


@given(text())
def test_job_register_cron(name):
    """test if job with any name is registred"""
    job = Job(name)
    job.register(tasks=tasks, crons="* * * * *", if_exists="replace")
    assert job.exists

    job.remove()


@given(text(), text())
def test_job_register_upstream(job_name, upstream_job_name):
    upstream_job = Job(upstream_job_name)
    upstream_job.register(tasks=tasks, crons="* * * * *", if_exists="replace")

    job = Job(job_name)
    # trying to set a job as it's own upstream should raise error
    if job_name == upstream_job_name:
        with pytest.raises(ValueError):
            job.register(tasks=tasks, upstream=[upstream_job_name], if_exists="replace")
    else:
        job.register(tasks=tasks, upstream=[upstream_job_name], if_exists="replace")
        assert job.upstream == [upstream_job]

        job.remove()
    upstream_job.remove()


@given(text())
def test_job_add_downstream_jobs(name):
    j = Job(name)


@given(text())
def test_job_remove_upstream_jobs(name):
    j = Job(name)

