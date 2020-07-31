import logging
from datetime import datetime, timedelta, timezone

from croniter import croniter
from redis import Redis
from rq import Queue
from distributed import Client

from grizly.scheduling.trigger import Trigger

logger = logging.getLogger("distributed.worker")


def get_triggers():
    redis_conn = Redis(host="10.125.68.177", port=80, db=0)
    triggers = []
    for trigger_name in redis_conn.keys("trigger:*"):
        if trigger_name is not None:
            triggers.append(Trigger(trigger_name.decode("utf-8"), logger=logger))
    return triggers


def run():
    redis = Redis(host="10.125.68.177", port=80)
    checks_queue = Queue("checks_queue", connection=redis)
    submit_queue = Queue("submit_queue", connection=redis)

    logger.info("Loading triggers...")
    triggers = get_triggers()
    logger.info("Triggers loaded successfully")

    for trigger in triggers:
        if trigger.type == "cron":
            start_date = trigger.last_run or trigger.created_at
            cron_str = trigger.value
            cron = croniter(cron_str, start_date)
            next_run = cron.get_next(datetime).replace(tzinfo=timezone.utc)
            logger.info(next_run)
            logger.info((datetime.now(timezone.utc) + timedelta(minutes=1)).__str__())
            if next_run < datetime.now(timezone.utc) + timedelta(minutes=1):
                trigger.is_triggered = 1
        if trigger.is_triggered == 1:
            logger.info(f"Loading {trigger.name} jobs...")
            jobs = trigger.get_jobs()
            logger.info(f"Jobs from {trigger.name} loaded successfully")
            for job in jobs:
                if job.status != "running":
                    if job.type == "regular":
                        submit_queue.enqueue(job.submit, None, "acoe.connect.te.com:8786")
                        logger.info(f"Job {job.name} has been successfully submitted to submit queue")
                    elif job.type == "listener":
                        checks_queue.enqueue(job.submit, None, "acoe.connect.te.com:8786")
                        logger.info(f"Job {job.name} has been successfully submitted to chcks queue")
            trigger.last_run = datetime.utcnow().__str__()
            trigger.is_triggered = 0


if __name__ == "__main__":
    run()
