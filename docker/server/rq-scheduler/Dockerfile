FROM acoeteam/grizly_notebook:latest

ARG REDIS_HOST=redis
ARG REDIS_PORT=6379

RUN pip install rq-scheduler

ENTRYPOINT ["/bin/bash", "-c", "rqscheduler --host ${REDIS_HOST} --port ${REDIS_PORT}"]
