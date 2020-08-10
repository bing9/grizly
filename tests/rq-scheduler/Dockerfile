FROM grizly_notebook:latest

ENV REDIS_HOST=redis

RUN pip install rq-scheduler

ENTRYPOINT ["/bin/bash", "-c", "rqscheduler --host ${REDIS_HOST}"]