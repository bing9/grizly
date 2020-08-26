from rq import Queue
from rq_scheduler import Scheduler
from redis import Redis
from grizly.scheduling.system_jobs.agent import run

import logging
import os
import typer


logger = logging.getLogger(__name__)

REDIS_HOST = os.getenv("GRIZLY_REDIS_HOST", default="localhost")
REDIS_PORT = int(os.getenv("GRIZLY_REDIS_PORT", default=6379))


def register(
    host: str = typer.Option(REDIS_HOST, "-h"),
    port: int = typer.Option(REDIS_PORT, "-p"),
    ):
    """Register the agent as a cron job ran by rq-scheduler

    Parameters
    ----------
    host : Union[str, None], optional
        The Redis host at which to register the agent, by default REDIS_HOST
    port : Union[str, None], optional
        The Redis port at which to register the agent, by default REDIS_PORT
    """

    r = Redis(host=host, port=port)
    queue = Queue("system", connection=r)
    scheduler = Scheduler(queue=queue, connection=r)

    scheduler.cron(
        "* * * * *",
        func=run,
        queue_name=queue.name,      # In which queue the job should be put in
        use_local_timezone=False    # Interpret hours in the local timezone
    )

if __name__ == "__main__":
    typer.run(register)
