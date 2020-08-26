FROM grizly_notebook:latest

RUN pip install rq
RUN pip install rq-scheduler
RUN pip install setproctitle


WORKDIR ${HOME}
COPY rq-worker/settings.py /usr/src/settings.py

# Spawn workers according to config
ENTRYPOINT ["/bin/bash", "-c", "/usr/local/bin/rq worker --with-scheduler -P /usr/src -c settings"]
