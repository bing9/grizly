import logging

from rq import Queue
from grizly.scheduling.registry import Registry

logger = logging.getLogger(__name__)
registry = Registry(redis_host="pytest_redis")
submit_queue = Queue("submit", connection=registry.con)


def run():

    try:

        logger.info("Loading triggers...")
        triggers = registry.get_triggers()
        logger.info("Triggers loaded successfully")

        for trigger in triggers:
            if trigger.is_triggered:
                for job in trigger.jobs:
                    submit_queue.enqueue(job.submit)
    except:
        raise ValueError("Something went wrong.")


if __name__ == "__main__":
    run()
