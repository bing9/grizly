docker-compose build
docker tag dask_worker:latest acoeteam/dask_worker:latest
docker push acoeteam/dask_worker
