# platform

## Installation and maintenance

Tutorial how to install Docker and how to keep it up to date can be found [here](./docs/internal/docker_user/getting_started.md).

## Tips and tricks

### To get shell

`> docker exec -it grizly_notebook bash`

### Clean clutter images

`> docker system prune`

### Running stuff on your local cluster

1. Start your env with `docker-compose up`
2. Whenever you want to test a workflow, run eg. `grizly workflow run "sales daily news" --local`
3. To clean up in case of failure, run `grizly workflow cancel "sales daily news" --local`
4. You can see your cluster's dashboard at `localhost:8787`
5. You can see the logs on the dashboard or in container logs (they are visible eg. in the terminal where you ran `docker-compose up`, and also if you run `docker logs CONTAINER`)
