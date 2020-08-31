docker-compose build
docker tag dask_scheduler:latest acoeteam/dask_scheduler
docker push acoeteam/dask_scheduler
