from ..grizly.scheduling.registry import Job, Registry
import redis
import dask
import json
from hypothesis.strategies import text


@dask.delayed
def add(x, y):
    return x + y


task1 = add(1, 2)
tasks = [task1]

con = redis.Redis(host="pytest_redis")


@given(text())
def test_job_properties(name):
    job = Job(name)
    job.register(tasks=tasks)
    properties = (
        job.owner,
        job.trigger_names,
        job.type,
        job.last_run,
        job.run_time,
        job.status,
        job.error,
        job.created_at,
    )


@given(text())
def test_register_job(name):
    """test if job with any name is registred"""
    job = Job(name)
    job.register(tasks=tasks)
    assert job.exists


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
