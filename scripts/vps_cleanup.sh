#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="${1:-$(pwd)}"

cd "${PROJECT_DIR}"
docker compose up -d --remove-orphans
docker image prune -af --filter "until=168h"
docker container prune -f

if [[ -d /var/lib/docker/containers ]]; then
  find /var/lib/docker/containers -name '*-json.log' -size +200M -exec truncate -s 0 {} \;
fi
