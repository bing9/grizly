FROM acoeteam/grizly_notebook:latest

ENV SCHEDULER_ADDRESS=grizly_scheduler:8786

WORKDIR ${HOME}

ENTRYPOINT ["/bin/bash", "-c", "dask-worker ${SCHEDULER_ADDRESS} --nthreads 1 --memory-limit=1GB"]
