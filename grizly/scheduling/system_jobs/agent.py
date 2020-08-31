import logging

from rq import Queue
from grizly.scheduling.registry import SchedulerDB

logger = logging.getLogger(__name__)
schdb = SchedulerDB()
submit_queue = Queue(schdb.submit_queue_name, connection=schdb.con)


def run():

    logger.info("Loading triggers...")
    triggers = schdb.get_triggers()
    logger.info("Triggers loaded successfully")

    for trigger in triggers:
        if trigger.is_triggered:
            for job in trigger.jobs:
                submit_queue.enqueue(job.submit)


if __name__ == "__main__":
    run()
