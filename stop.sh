DOCKER_COMMAND="docker"

docker-compose down
${DOCKER_COMMAND} volume prune -f
${DOCKER_COMMAND} system prune -f
${DOCKER_COMMAND} network prune -f