FROM grizly_notebook:latest

EXPOSE 9181

RUN pip install rq-dashboard

ARG REDIS_HOST=redis
ENV REDIS_HOST=$REDIS_HOST

ENTRYPOINT ["/bin/bash", "-c", "rq-dashboard -H $REDIS_HOST"]
