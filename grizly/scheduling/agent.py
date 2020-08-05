<<<<<<< HEAD
import logging
from datetime import datetime, timedelta, timezone

from croniter import croniter
from redis import Redis
from rq import Queue
from distributed import Client

from grizly.scheduling.trigger import Trigger
from grizly.scheduling.registry import Registry

logger = logging.getLogger("distributed.worker")


def run():
    redis = Redis(host="10.125.68.177", port=80)
    checks_queue = Queue("checks_queue", connection=redis)
    submit_queue = Queue("submit_queue", connection=redis)

    logger.info("Loading triggers...")
    triggers = Registry().get_triggers()
    logger.info("Triggers loaded successfully")

    for trigger in triggers:
        if trigger.type == "cron":
            start_date = trigger.last_run or trigger.created_at
            cron_str = trigger.value
            cron = croniter(cron_str, start_date)
            next_run = cron.get_next(datetime).replace(tzinfo=timezone.utc)
            now = datetime.now(timezone.utc) + timedelta(minutes=1)
            logger.info(f"trigger is {trigger.name} now is {now} and next run will be {next_run}")
            logger.info(now)
            if next_run < now:
                trigger.is_triggered = True
                trigger.last_run = now.strftime("%Y-%m-%d %H:%M:%S.%f")
        if trigger.is_triggered:
            logger.info(f"Loading {trigger.name} jobs...")
            jobs = trigger.get_jobs()
            logger.info(f"Jobs from {trigger.name} loaded successfully")
            for job in jobs:
                if job.status != "running":
                    if job.type == "regular":
                        logger.info(f"submitting {job.name}")
                        submit_queue.enqueue(job.submit)
                        logger.info(f"Job {job.name} has been successfully submitted to submit queue")
                    elif job.type == "listener":
                        checks_queue.enqueue(job.submit)
                        logger.info(f"Job {job.name} has been successfully submitted to chcks queue")
            trigger.last_run = datetime.utcnow().__str__()
            trigger.is_triggered = False


if __name__ == "__main__":
    run()
=======
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
>>>>>>> 85997caefaa2ccb6486c69bce3a3bef762efb3db
