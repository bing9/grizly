FROM grizly_notebook:latest

ARG REDIS_HOST=redis
ENV REDIS_HOST=$REDIS_HOST
ARG REDIS_PORT=6379
ENV REDIS_PORT=$REDIS_PORT

RUN pip install rq-scheduler

ENTRYPOINT ["/bin/bash", "-c", "rqscheduler --host ${REDIS_HOST} --port ${REDIS_PORT}"]
