from datetime import datetime, timezone
import json
import pytest

import dask
from hypothesis import given
import redis

from hypothesis.strategies import integers, text, lists

from ..grizly.scheduling.registry import Job, Registry, RegistryObject, Trigger


@dask.delayed
def add(x, y):
    return x + y


task1 = add(1, 2)
tasks = [task1]

con = redis.Redis(host="pytest_redis")


@given(text())
def test_registryobject_serialize(s):
    pass


# PROPERTIES TESTS
# checking if properites return right values
# and if the setters work


@given(text(), text())
def test_job_error(name, error):
    job = Job(name)
    job.register(tasks=tasks)
    assert job.error is None

    job.error = error
    assert job.error == error
    job.remove()


@given(text())
def test_job_last_run(name):
    job = Job(name)
    job.register(tasks=tasks)
    assert job.last_run is None

    last_run = datetime.now(timezone.utc)
    job.last_run = last_run
    assert job.last_run == last_run
    job.remove()


@given(text(), text())
def test_job_owner(name, owner):
    job = Job(name)
    job.register(tasks=tasks, owner=owner)
    assert job.owner == owner
    job.remove()


@given(text(), integers())
def test_job_run_time(name, run_time):
    job = Job(name)
    job.register(tasks=tasks)
    assert job.run_time is None

    job.run_time = run_time
    assert job.run_time == run_time
    job.remove()


def test_job_status():
    # this one should check only allowed statuses
    # and check if it fails in other cases
    pass


@given(text())
def test_job_tasks(name):
    job = Job(name)
    job.register(tasks=tasks)
    assert job.tasks == tasks
    job.remove()


@given(text(), text())
def test_job_trigger_names(job_name, trigger_name):
    job = Job(job_name)
    job.register(tasks=tasks)
    assert job.trigger_names == []

    with pytest.raises(ValueError):
        job.trigger_names = ["not_found_trigger"]

    tr = Trigger(name=trigger_name)
    tr.register(type="cron", value="*/3 * * * *")
    job.trigger_names = [trigger_name]
    assert job.trigger_names == [trigger_name]
    job.remove()
    tr.remove()


def test_job_type():
    # same as in test_job_status
    pass


# END PROPERTIES TESTS


@given(text())
def test_register_job(name):
    """test if job with any name is registred"""
    job = Job(name)
    job.register(tasks=tasks)
    assert job.exists
    job.remove()

    assert not job.exists


def test_add_trigger():
    name = "a_job_name2"
    job = Job(name)
    job.register(tasks=tasks)
    Registry().add_trigger("every_2_mins", "cron", "*/2 * * * *")
    Registry().add_trigger("every_3_mins", "cron", "*/3 * * * *")
    job.add_trigger("every_2_mins")
    job.add_trigger("every_3_mins")
    assert job.trigger_names == ["every_2_mins", "every_3_mins"]


def test_get_jobs():
    name = "a_job_name"
    registry = Registry()
    jobs = registry.get_jobs()
    assert isinstance(jobs[0], Job)
