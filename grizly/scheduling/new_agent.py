import logging
from datetime import datetime, timedelta, timezone

from croniter import croniter
from redis import Redis
from rq import Queue

from grizly.scheduling.trigger import Trigger


def get_triggers(redis_conn):
    return [Trigger(trigger_name) for trigger_name in redis_conn.keys("trigger*")]


def run():
    redis = Redis(host="10.125.68.177", port=80)
    checks_queue = Queue("checks_queue", connection=redis)
    submit_queue = Queue("submit_queue", connection=redis)

    triggers = get_triggers()

    for trigger in triggers:
        if trigger.type == "cron":
            start_date = job.last_run or job.created_at
            cron_str = job.trigger_value
            cron = croniter(cron_str, start_date)
            next_run = cron.get_next(datetime).replace(tzinfo=timezone.utc)
            if next_run < datetime.now(timezone.utc) + timedelta(minutes=1):
                if job.type == "regular":
                    submit_queue.enqueue(job.submit)
                    logger.info(f"Job {job.name} has been successfully submitted to submit queue")
                elif job.type == "listener":
                    checks_queue.enqueue(job.submit)
                    logger.info(f"Job {job.name} has been successfully submitted to chcks queue")


if __name__ == "__main__":

    run()
