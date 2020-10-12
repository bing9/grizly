FROM grizly_notebook:latest

ENV REDIS_HOST=redis
ARG HTTPS_PROXY
ENV HTTPS_PROXY=${HTTPS_PROXY}

EXPOSE 9181

RUN pip install rq-dashboard

ENTRYPOINT ["/bin/bash", "-c", "rq-dashboard -H ${REDIS_HOST}"]