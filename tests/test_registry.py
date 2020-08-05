from ..grizly.scheduling.registry import Job, Registry
import redis
import dask
import json

@dask.delayed
def add(x, y):
    return x + y

task1 = add(1, 2)
tasks = [task1]

con = redis.Redis(host="pytest_redis")

def test_register_job():
    name = "a_job_name"
    job = Job(name)
    job.register(tasks=tasks)
    name_with_prefix = "grizly:job:" + name
    saved_job_type = json.loads(con.hget(name_with_prefix, "type"))
    assert job.type == saved_job_type

def test_get_jobs():
    name = "a_job_name"
    registry = Registry()
    jobs = registry.get_jobs()
    assert isinstance(jobs[0], Job)