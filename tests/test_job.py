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


def test_job_register():
    pass


# creating test jobs - they have different scopes so that they are not unregistered at the same time
@pytest.fixture(scope="module")
def job_1():
    job_1 = Job(name="test1")
    job_1.register(tasks=tasks, crons="* * * * *", if_exists="replace")
    yield job_1
    job_1.unregister()


@pytest.fixture(scope="session")
def job_2():
    job_2 = Job(name="test2")
    job_2.register(tasks=tasks, upstream="test1", if_exists="replace")
    yield job_2
    job_2.unregister()


# PROPERTIES
# ----------


def test_job_exists(job_1):
    assert job_1.exists


@given(text())
def test_job_owner(job_1, owner):
    job_1.owner = owner
    assert job_1.owner == owner


def test_job_tasks(job_1):
    assert dask.delayed(job_1.tasks).compute() == dask.delayed(tasks).compute()


def test_job_downstream(job_1, job_2):
    assert job_1.downstream == [job_2]

    # trying to set not existing job as downstream should raise error
    with pytest.raises(JobNotFoundError):
        job_1.downstream = ["not_found_job"]

    job_1.downstream = []
    assert job_1.downstream == []

    job_1.downstream = [job_2.name]
    assert job_1.downstream == [job_2]


def test_job_upstream(job_1, job_2):
    assert job_2.upstream == [job_1]

    # trying to set not existing job as downstream should raise error
    with pytest.raises(JobNotFoundError):
        job_1.upstream = ["not_found_job"]

    job_2.upstream = []
    assert job_2.upstream == []

    job_2.upstream = [job_1.name]
    assert job_2.upstream == [job_1]

