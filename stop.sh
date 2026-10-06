#!/bin/sh
set -eu

# Voir start.sh : on s'ancre sur le répertoire réel du script. Le nom de
# projet Compose (simulpix) est fixé par la clé `name:` de docker-compose.yml.
cd "$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"

docker_compose() {
  if docker compose version >/dev/null 2>&1; then
    docker compose "$@"
  else
    docker-compose "$@"
  fi
}

PROJECTS="simulpix simul-pix"

project_container_ids() {
  for project_name in $PROJECTS; do
    docker ps -aq --filter "label=com.docker.compose.project=${project_name}"
  done
}

wait_for_containers_gone() {
  attempts="${1:-30}"
  while [ "$attempts" -gt 0 ]; do
    if [ -z "$(project_container_ids)" ]; then
      return 0
    fi
    attempts=$((attempts - 1))
    sleep 1
  done
  return 1
}

remove_project_containers() {
  container_ids="$(project_container_ids)"
  if [ -n "$container_ids" ]; then
    docker rm -f $container_ids >/dev/null 2>&1 || true
  fi
}

remove_project_volumes() {
  for project_name in $PROJECTS; do
    docker volume ls -q --filter "label=com.docker.compose.project=${project_name}" | while IFS= read -r volume_name; do
      docker volume rm "$volume_name" >/dev/null 2>&1 || true
    done
  done
}

echo "[simulpix] stopping platform"
docker_compose stop
echo "[simulpix] removing containers and project volumes"
docker_compose down -v --remove-orphans
echo "[simulpix] removing remaining project containers"
remove_project_containers
if ! wait_for_containers_gone 30; then
  echo "[simulpix] warning: some project containers are still being removed" >&2
fi
echo "[simulpix] removing remaining project volumes"
remove_project_volumes
echo "[simulpix] resetting runtime state"
rm -f \
  runtime/current-run.env \
  runtime/current-run.json \
  runtime/current-network.json \
  runtime/observability/latest-replication.json
echo "[simulpix] platform stopped and cleaned"
