FROM grizly_notebook:latest

ENV REDIS_HOST=redis
ENV REDIS_PORT=6379

EXPOSE 9181

RUN pip install rq-dashboard

ENTRYPOINT ["/bin/bash", "-c", "rq-dashboard -H ${REDIS_HOST} -p ${REDIS_PORT}"]
