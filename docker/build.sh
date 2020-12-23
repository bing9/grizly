docker pull acoeteam/base_notebook:latest
docker-compose build --no-cache --build-arg BUILD_DATE=$(date -u +'%Y-%m-%dT%H:%M:%SZ')
docker tag docker_grizly_notebook:latest grizly_notebook:latest