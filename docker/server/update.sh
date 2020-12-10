#!/usr/bin/env bash
git pull --no-edit
cd ../
docker-compose build --no-cache --build-arg BUILD_DATE=$(date -u +'%Y-%m-%dT%H:%M:%SZ')
docker tag docker_grizly_notebook:latest grizly_notebook:latest
cd server/dask_worker
docker-compose build --no-cache
docker tag dask_worker:latest acoeteam/dask_worker
cd ../server/dask_scheduler
docker-compose build --no-cache
docker tag dask_scheduler:latest acoeteam/dask_scheduler
cd ../
docker-compose up --build --force-recreate --scale dask_worker=4 --scale rq-worker=5
