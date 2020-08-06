FROM grizly_notebook:latest

RUN pip install rq
RUN pip install supervisor

# Spawn workers according to config
ENTRYPOINT ["/bin/bash", "-c", "supervisord -c /usr/src/workers/supervisord.conf -n"]