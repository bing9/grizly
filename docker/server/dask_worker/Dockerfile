FROM grizly_notebook:latest

ARG SCHEDULER_ADDRESS=dask_scheduler:8786
ENV SCHEDULER_ADDRESS=$SCHEDULER_ADDRESS

ARG MEMORY_LIMIT=1GB
ENV MEMORY_LIMIT=$MEMORY_LIMIT

WORKDIR ${HOME}

ENTRYPOINT ["/bin/bash", "-c", "dask-worker ${SCHEDULER_ADDRESS} --local-directory=/home/analyst --nthreads 1 --memory-limit=${MEMORY_LIMIT}"]
