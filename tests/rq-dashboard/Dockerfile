FROM grizly_notebook:latest

RUN pip install rq-dashboard

ENTRYPOINT ["/bin/bash", "-c", "rq-dashboard -H pytest_redis"]