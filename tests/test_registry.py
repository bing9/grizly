from datetime import datetime, timezone
import json
import pytest
import time
import dask
from redis import Redis
from hypothesis import given
import redis

from hypothesis.strategies import integers, text, lists

from ..grizly.scheduling.registry import Job, SchedulerDB, SchedulerObject, Trigger
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

# Just using job_with_cron (otherwise multiple execution)


@pytest.fixture(scope="module")
def job_run(job_with_cron):
    job_with_cron.submit(to_dask=False)
    job_run = job_with_cron.last_run
    yield job_run


@pytest.fixture(scope="session", params=["job_with_cron", "trigger"])
def redis_object(job_with_cron, trigger, request):
    return eval(request.param)


@pytest.fixture(scope="module")
def scheduler_db():
    return SchedulerDB()


# SchedulerDB PROPERTIES
# ------------------
### TODO @marius


def test_scheduler_db_con(scheduler_db):
    con = scheduler_db.con
    assert isinstance(con, Redis)


# SchedulerDB METHODS
# ---------------


def test_scheduler_db_add_trigger(scheduler_db, trigger):
    # TODO: Size of trigger-list will not increase
    # if else structure / unregister manual trigger (maybe fixture)
    length_1 = len(scheduler_db.get_triggers())
    scheduler_db.add_trigger('test123')
    length_2 = len(scheduler_db.get_triggers())

    trigger_list = scheduler_db.get_triggers()
    assert trigger_list == [trigger, Trigger("test123")]

    # assert length_1 == 1
    # assert length_1 + 1 == length_2

    Trigger("test123").unregister()
    # pass


def test_scheduler_db_get_triggers(scheduler_db, trigger):
    triggers = scheduler_db.get_triggers()
    assert trigger in triggers


def test_scheduler_db_add_job(scheduler_db):
    # TODO: Checking the properties (exists)
    length_1 = len(scheduler_db.get_jobs())
    test_name = 'test_job'+str(datetime.now())
    scheduler_db.add_job(test_name, [], None, [], [], [], 'fail')
    length_2 = len(scheduler_db.get_jobs())
    assert length_1 + 1 == length_2
    # TODO: assert job.exists
    Job(test_name).unregister()


def test_scheduler_db_get_jobs(scheduler_db, job):
    jobs = scheduler_db.get_jobs()
    assert job in jobs


def test_scheduler_db_get_job_runs(scheduler_db, job_run):
    # TODO: Checking the fixture job_run (only job_with_crons)
    job_runs = scheduler_db.get_job_runs()
    assert job_run in job_runs


def test_scheduler_db__check_if_jobs_exist():
    pass


def test_scheduler_db__check_if_exists():
    pass


# SchedulerObject PROPERTIES
# ---------------------


def test_redis_object_exists(redis_object):
    assert redis_object.exists
    # assert False


# SchedulerObject METHODS
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

def test_trigger_is_triggered(trigger):
    assert trigger.is_triggered is None
    trigger.is_triggered = True
    assert trigger.is_triggered == True


def test_trigger_jobs(trigger, job_with_trigger):
    # TODO: list object is not callable
    jobs = trigger.jobs
    assert job_with_trigger in jobs


def test_trigger_add_remove_jobs(trigger, job_with_cron):
    # TODO: how to access the name of the jobs?
    trigger.add_jobs(job_with_cron.name)
    assert job_with_cron in trigger.jobs

    trigger.remove_jobs([job_with_cron.name])
    assert job_with_cron not in trigger.jobs


def test_trigger_register(trigger):
    # TODO: Already tested within the fixture
    assert isinstance(trigger.register(), Trigger)


def test_trigger_unregister(trigger):
    trigger.unregister()
    assert not trigger.exists
    trigger.register()

# Trigger METHODS
# # ---------------
