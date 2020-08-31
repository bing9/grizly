from ..grizly.exceptions import JobNotFoundError
from ..grizly.scheduling.registry import Job, SchedulerDB, SchedulerObject, Trigger
import os
from hypothesis.strategies import integers, text, lists
from pytest_mock import MockerFixture
from unittest.mock import Mock, patch
import redis
from hypothesis import given
from redis import Redis
import dask
import time
import pytest
from unittest import mock
from typing import get_args
from os import name
import json
from datetime import datetime, timezone


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


# Just using job_with_cron (otherwise multiple execution)
@pytest.fixture(scope="module")
def job_run(job_with_cron):
    job_with_cron.submit(to_dask=False)
    job_run = job_with_cron.last_run
    yield job_run


@pytest.fixture(scope="session", params=["job_with_cron", "trigger"])
def scheduler_object(job_with_cron, trigger, request):
    return eval(request.param)


@pytest.fixture(scope="module")
def scheduler_db():
    return SchedulerDB()


# SchedulerDB PROPERTIES
# ------------------
def test_scheduler_db_con(scheduler_db):
    con = scheduler_db.con
    assert isinstance(con, Redis)


# SchedulerDB METHODS
# ---------------
@given(text())
def test_scheduler_db_add_trigger(scheduler_db, trigger_name):
    trigger_list_1 = scheduler_db.get_triggers()
    if Trigger(trigger_name) in trigger_list_1:
        Trigger(trigger_name).unregister()
    scheduler_db.add_trigger(trigger_name)
    trigger_list_2 = scheduler_db.get_triggers()
    assert Trigger(trigger_name) in trigger_list_2
    Trigger(trigger_name).unregister()


def test_scheduler_db_get_triggers(scheduler_db, trigger):
    triggers = scheduler_db.get_triggers()
    assert trigger in triggers


@given(text())
def test_scheduler_db_add_job(scheduler_db, job_name):
    job_list_1 = scheduler_db.get_jobs()
    if Job(job_name) in job_list_1:
        Job(job_name).unregister()
    scheduler_db.add_job(job_name, [], None, [], [], [], 'fail')
    job_list_2 = scheduler_db.get_jobs()
    assert Job(job_name) in job_list_2
    assert Job(job_name).exists

    """property check"""
    # TODO: property check leads to error (manual job deletion is needed)
    # assert Job(job_name).name is not None
    # assert Job(job_name).tasks is not None
    # assert Job(job_name).owner is not None
    # assert Job(job_name).crons is not None
    # assert Job(job_name).upstream is not None
    # assert Job(job_name).triggers is not None
    # assert Job(job_name).if_exists is not None

    Job(job_name).unregister()


def test_scheduler_db_get_jobs(scheduler_db, job):
    jobs = scheduler_db.get_jobs()
    assert job in jobs


def test_scheduler_db_get_job_runs(scheduler_db, job_run):
    job_runs = scheduler_db.get_job_runs()
    assert job_run in job_runs


def test_scheduler_db__check_if_jobs_exist():
    pass  # private method


def test_scheduler_db__check_if_exists():
    pass  # private method


# SchedulerObject PROPERTIES
# ---------------------
def test_scheduler_object_created_at(scheduler_object):
    return_value = scheduler_object.created_at
    assert return_value is not None


def test_scheduler_object_con(scheduler_object):
    return_value = scheduler_object.con
    assert return_value is not None


def test_scheduler_object_exists(scheduler_object):
    assert scheduler_object.exists


# SchedulerObject METHODS
# -------------------
def test_scheduler_object_getall(scheduler_object):
    return_value = scheduler_object.getall()
    assert return_value is not None


def test_scheduler_object__serialize():
    pass  # private method


def test_scheduler_object__deserialize():
    pass  # private method


def test_scheduler_object__add_values():
    pass  # private method


def test_scheduler_object__remove_values():
    pass  # private method


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


@given(text())
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
def test_job_add_remove_triggers():
    # TODO: Error check
    # test_job = Job(name="test_job_123")
    # test_trigger = Trigger(name="test_trigger_123")
    # test_trigger.register()
    # test_job.add_triggers(test_trigger.name)
    # assert test_trigger in test_job.triggers
    # test_job.remove_triggers(test_trigger.name)
    # assert test_trigger not in test_job.triggers
    # test_trigger.unregister()
    # test_job.unregister()
    pass


def test_job_remove_triggers():
    pass  # already tested in the test-function above


@given(text())
def test_job_add_remove_downstream_jobs(job_with_cron, d_name):
    d_job = Job(name=d_name)
    d_job.register(tasks=[])

    job_with_cron.add_downstream_jobs(d_job.name)
    downstrams1 = job_with_cron.downstream
    assert d_job in downstrams1

    job_with_cron.remove_downstream_jobs(d_job.name)
    downstrams2 = job_with_cron.downstream
    assert d_job not in downstrams2

    d_job.unregister()
    assert not d_job.exists


def test_job_add_upstream_jobs(job, job_with_upstream, scheduler_db):
    # TODO
    pass


def test_job_remove_upstream_jobs():
    # TODO
    pass


def test_job_cancel():
    # TODO
    pass


def test_job_register(job_with_cron):
    con = job_with_cron.con
    assert con.hgetall(job_with_cron.hash_name) != {}

    rq_job_ids = job_with_cron._rq_job_ids
    assert len(rq_job_ids) == 1


def test_job_unregister(job_with_cron):
    job_with_cron.unregister()
    con = job_with_cron.con
    assert con.hgetall(job_with_cron.hash_name) == {}

    job_with_cron.register(tasks=tasks, crons="* * * * *", if_exists="replace")
    con = job_with_cron.con
    assert con.hgetall(job_with_cron.hash_name) != {}


def test_job_submit(job_with_cron):
    # TODO
    pass


def test_job_visualize(job_with_cron):
    assert job_with_cron.visualize() is not None


def test_job__add_to_scheduler():
    pass  # private method


def test_job__remove_from_scheduler():
    pass  # private method


def test_job__submit_downstram_jobs():
    pass  # private method

# JobRun PROPERTIES
# -----------------


def test_job_run_duration(job_run):
    assert job_run.duration == 0


def test_job_run_id(job_run):
    assert job_run._id == 1


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
@given(integers())
def test_job_run_duration_setter(job_run, duration_int):
    if job_run.duration != 0:
        job_run.duration = 0
    job_run.duration = duration_int
    assert job_run.duration == duration_int
    job_run.duration = 0
    assert job_run.duration == 0


@given(text())
def test_job_run_error_setter(job_run, error_text):
    if job_run.error is not None:
        job_run.error = None
    job_run.error = error_text
    assert job_run.error == error_text
    job_run.error = None
    assert job_run.error is None


def test_job_run_finished_at_setter(job_run):
    value = job_run.finished_at
    job_finished_at = None
    assert job_finished_at is None
    job_finished_at = value
    assert job_finished_at == value


@given(text())
def test_job_run_name_setter(job_run, new_name):
    value = job_run.name
    job_run.name = new_name
    assert job_run.name == new_name
    job_run.name = value
    assert job_run.name == value


def test_job_run_status_setter(job_run):
    value = job_run.status
    job_run.status = 'fail'
    assert job_run.status == 'fail'
    job_run.status = 'running'
    assert job_run.status == 'running'
    job_run.status = value
    assert job_run.status == value


def test_job_run_unregister_register(job_run):
    job_run.unregister()
    con = job_run.con
    assert con.hgetall(job_run.hash_name) == {}

    job_run.register()
    assert con.hgetall(job_run.hash_name) != {}


# Trigger PROPERTIES
# ------------------
def test_trigger_is_triggered(trigger):
    assert trigger.is_triggered is None
    trigger.is_triggered = True
    assert trigger.is_triggered == True


def test_trigger_jobs(trigger, job_with_trigger):
    jobs = trigger.jobs
    assert job_with_trigger in jobs


# Trigger METHODS
# # ---------------
def test_trigger_add_remove_jobs(trigger, job_with_cron):
    trigger.add_jobs(job_with_cron.name)
    assert job_with_cron in trigger.jobs

    trigger.remove_jobs([job_with_cron.name])
    assert job_with_cron not in trigger.jobs


def test_trigger_register(trigger):
    # Already tested within the fixture
    assert isinstance(trigger.register(), Trigger)


def test_trigger_unregister(trigger):
    trigger.unregister()
    assert not trigger.exists
    trigger.register()
