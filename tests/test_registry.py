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
    job_with_cron.unregister(remove_job_runs=True)


@pytest.fixture(scope="module")
def job_with_upstream(job_with_cron):
    job_with_upstream = Job(name="job_with_upstream")
    job_with_upstream.register(
        tasks=tasks, upstream=job_with_cron.name, if_exists="replace")
    yield job_with_upstream
    job_with_upstream.unregister(remove_job_runs=True)


@pytest.fixture(scope="module")
def job_with_trigger(trigger):
    job_with_trigger = Job(name="job_with_trigger")
    job_with_trigger.register(
        tasks=tasks, triggers=trigger.name, if_exists="replace")
    yield job_with_trigger
    job_with_trigger.unregister(remove_job_runs=True)


@pytest.fixture(scope="module", params=["job_with_cron", "job_with_upstream", "job_with_trigger"])
def job(job_with_cron, job_with_upstream, job_with_trigger, request):
    return eval(request.param)


@pytest.fixture(scope="session")
def trigger():
    _trigger = Trigger(name="test_trigger")
    _trigger.register()
    yield _trigger
    _trigger.unregister()


@pytest.fixture(scope="module")
def job_run(job):
    job.submit(to_dask=False)
    yield job.last_run


@pytest.fixture(scope="session", params=["job_with_cron", "trigger"])
def redis_object(job_with_cron, trigger, request):
    return eval(request.param)


# RedisDB PROPERTIES
# ------------------
### TODO @marius

def test_redis_db_con():


def test_redis_db_add_trigger():


def test_redis_db_get_triggers():


def test_redis_db_add_job():


def test_redis_db_get_jobs():


def test_redis_db_get_job_runs():


def test_redis_db__check_if_jobs_exist():
    # private function


def test_redis_db__check_if_exists():
    # private function

    # RedisDB METHODS
    # ---------------

    # RedisObject PROPERTIES
    # ---------------------


def test_redis_object_exists(redis_object):
    assert redis_object.exists
    # assert False


# RedisObject METHODS
# -------------------


# Job PROPERTIES
# --------------


def test_job_cron(job_with_cron):
    assert job_with_cron.crons == ["* * * * *"]

    with pytest.raises(ValueError):
        job_with_cron.crons = ["invalid_cron_string"]

    job_with_cron.crons = []
    assert job_with_cron.crons == []
    assert len(job_with_cron._rq_job_ids) == 0

    job_with_cron.crons = "* * * * *"
    assert job_with_cron.crons == ["* * * * *"]
    assert len(job_with_cron._rq_job_ids) == 1


@given(text())  # hypothesis
def test_job_owner(job, owner):
    assert job.owner is None

    job.owner = owner
    assert job.owner == owner

    job.owner = None
    assert job.owner is None


def test_job_tasks(job):
    tasks = job.tasks
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

    # trying to set not existing job as upstream should raise error
    with pytest.raises(JobNotFoundError):
        job_with_cron.upstream = ["not_found_job"]

    job_with_upstream.upstream = []
    assert job_with_upstream.upstream == []
    assert job_with_upstream not in job_with_cron.downstream

    job_with_upstream.upstream = [job_with_cron.name]
    assert job_with_upstream.upstream == [job_with_cron]
    assert job_with_upstream in job_with_cron.downstream


# Job METHODS
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


# JobRun PROPERTIES
# -----------------


def test_job_run_duration(job_run):
    assert job_run.duration == 0


def test_job_run_id(job_run):
    assert job_run._id == 1
    # assert False


def test_job_run_error(job_run):
    assert job_run.error is None


def test_job_run_created_finished_at(job_run):
    assert job_run.created_at < job_run.finished_at


def test_job_run_name(job_run):
    assert job_run.name is None


def test_job_run_status(job_run):
    assert job_run.status == "success"


# JobRun METHODS
# --------------


# Trigger PROPERTIES
# ------------------
### TODO @marius

def test_trigger_is_triggered():


def test_trigger_if_exists():


def test_trigger_jobs():


def test_trigger_add_jobs():


def test_trigger_register():


def test_trigger_remove_jobs():


def test_trigger_unregister():

    # Trigger METHODS
    # ---------------
