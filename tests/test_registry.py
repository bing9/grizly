from datetime import datetime, timezone
import json
import pytest

import dask
from hypothesis import given
import redis

from hypothesis.strategies import integers, text, lists

from ..grizly.scheduling.registry import Job, Trigger
from ..grizly.exceptions import JobNotFoundError


@dask.delayed
def add(x, y):
    return x + y


task1 = add(1, 2)
tasks = [task1]


# creating test jobs - they have different scopes so that they are not unregistered at the same time
@pytest.fixture(scope="session")
def job_with_cron():
    job_with_cron = Job(name="job_with_cron")
    job_with_cron.register(tasks=tasks, crons="* * * * *", if_exists="replace")
    yield job_with_cron
    # job_with_cron.unregister()


@pytest.fixture(scope="module")
def job_with_upstream(job_with_cron):
    job_with_upstream = Job(name="job_with_upstream")
    job_with_upstream.register(tasks=tasks, upstream=job_with_cron.name, if_exists="replace")
    yield job_with_upstream
    job_with_upstream.unregister()


@pytest.fixture(scope="session")
def trigger():
    trigger = Trigger(name="test_trigger")
    trigger.register()
    yield trigger
    trigger.unregister()


@pytest.fixture(scope="module")
def job_with_trigger(trigger):
    job_with_trigger = Job(name="job_with_trigger")
    job_with_trigger.register(tasks=tasks, triggers=trigger.name, if_exists="replace")
    yield job_with_trigger
    job_with_trigger.unregister()


# JOB PROPERTIES
# --------------


def test_job_cron(job_with_cron):
    assert job_with_cron.crons == ["* * * * *"]
    assert len(job_with_cron._rq_job_ids) == 1

    with pytest.raises(ValueError):
        job_with_cron.crons = ["invalid_cron_string"]

    job_with_cron.crons = []
    assert job_with_cron.crons == []

    job_with_cron.crons = "* * * * *"
    assert job_with_cron.crons == ["* * * * *"]


def test_job_exists(job_with_cron):
    assert job_with_cron.exists


@given(text())
def test_job_owner(job_with_cron, owner):
    assert job_with_cron.owner is None

    job_with_cron.owner = owner
    assert job_with_cron.owner == owner

    job_with_cron.owner = None
    assert job_with_cron.owner is None


def test_job_tasks(job_with_cron):
    tasks = job_with_cron.tasks
    assert len(tasks) == 1
    assert dask.delayed(tasks).compute() == [3]


def test_job_triggers(job_with_trigger, trigger):
    assert job_with_trigger.triggers == [trigger]

    job_with_trigger.triggers = []
    assert job_with_trigger.triggers == []
    assert job_with_trigger not in trigger.jobs

    job_with_trigger.triggers = trigger.name
    assert job_with_trigger.triggers == [trigger]
    assert job_with_trigger in trigger.jobs


def test_job_downstream(job_with_cron, job_with_upstream):
    assert job_with_cron.downstream == [job_with_upstream]

    # trying to set not existing job as downstream should raise error
    with pytest.raises(JobNotFoundError):
        job_with_cron.downstream = ["not_found_job"]

    job_with_cron.downstream = []
    assert job_with_cron.downstream == []
    assert job_with_cron not in job_with_upstream.upstream

    job_with_cron.downstream = [job_with_upstream.name]
    assert job_with_cron.downstream == [job_with_upstream]
    assert job_with_cron in job_with_upstream.upstream


def test_job_upstream(job_with_cron, job_with_upstream):
    assert job_with_upstream.upstream == [job_with_cron]

    # trying to set not existing job as downstream should raise error
    with pytest.raises(JobNotFoundError):
        job_with_cron.upstream = ["not_found_job"]

    job_with_upstream.upstream = []
    assert job_with_upstream.upstream == []
    assert job_with_upstream not in job_with_cron.downstream

    job_with_upstream.upstream = [job_with_cron.name]
    assert job_with_upstream.upstream == [job_with_cron]
    assert job_with_upstream in job_with_cron.downstream


# JOB METHODS
# -----------


def test_job_add_triggers():
    pass


def test_job_remove_triggers():
    pass


def test_job_add_downstream_jobs():
    pass


def test_job_remove_downstream_jobs():
    pass


def test_job_add_upstream_jobs():
    pass


def test_job_remove_upstream_jobs():
    pass


def test_job_register(job_with_cron):
    con = job_with_cron.con
    assert con.hgetall(job_with_cron.hash_name) != {}

    rq_job_ids = job_with_cron._rq_job_ids
    assert len(rq_job_ids) == 1
