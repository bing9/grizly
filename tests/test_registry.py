from datetime import datetime, timezone
import json
import pytest

import dask
from hypothesis import given
import redis

from hypothesis.strategies import integers, text, lists

from ..grizly.scheduling.registry import Registry
from ..grizly.scheduling.job import Job


@dask.delayed
def add(x, y):
    return x + y


task1 = add(1, 2)
tasks = [task1]

con = redis.Redis(host="pytest_redis")


def test_get_jobs():
    name = "a_job_name"
    registry = Registry()
    jobs = registry.get_jobs()
    assert isinstance(jobs[0], Job)
