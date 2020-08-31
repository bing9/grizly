docker-compose build
docker tag dask_worker:latest acoeteam/dask_worker
docker push acoeteam/dask_worker
