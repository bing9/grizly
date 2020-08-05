import logging
from datetime import datetime, timedelta, timezone

from rq import Queue
from grizly.scheduling.registry import Trigger, Registry

logger = logging.getLogger("distributed.worker")
registry = Registry(env="dev")  # registry = Registry()
checks_queue = Queue("checks_queue", connection=registry.con)
submit_queue = Queue("submit_queue", connection=registry.con)


def submit_trigger(trigger):
    logger.info(f"Loading {trigger.name} jobs...")
    jobs = trigger.get_jobs()
    logger.info(f"{trigger.name}'s jobs loaded successfully")
    for job in jobs:
        if job.status != "running":
            if job.type == "regular":
                queue = submit_queue
            elif job.type == "listener":
                queue = checks_queue
            else:
                continue
            logger.info(f"Submitting {job.name} to {queue.name}...")
            queue.enqueue(job.submit)
            logger.info(f"Job {job.name} has been successfully submitted to {queue.name}")
            trigger.last_run = datetime.now(timezone.utc)
            trigger.is_triggered = False
        else:
            logger.debug(f"Job {job.name} is already running")


def run():

    logger.info("Loading triggers...")
    triggers = registry.get_triggers()
    logger.info("Triggers loaded successfully")

    for trigger in triggers:
        if trigger.type == "cron":
            # trigger only cron jobs
            next_run = trigger.next_run
            now = datetime.now(timezone.utc) + timedelta(minutes=1)
            logger.info(f"Checking whether {trigger.name} should run... next run: {next_run}")
            if next_run < now:
                trigger.is_triggered = True
        # listeners are triggered inside submit, so here we only check & submit them
        if trigger.is_triggered:
            submit_trigger(trigger)


if __name__ == "__main__":
    run()
