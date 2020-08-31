FROM grizly_notebook:latest

EXPOSE 9181

RUN pip install rq-dashboard

ARG REDIS_HOST=redis

ENTRYPOINT ["/bin/bash", "-c", "rq-dashboard -H ${REDIS_HOST}"]
