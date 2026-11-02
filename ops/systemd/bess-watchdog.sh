#!/bin/sh
# Restart BESS containers that Docker itself never will. A restart policy
# acts only when the process exits; a container whose healthcheck fails
# while the process hangs stays "unhealthy" forever. Also starts any bess-*
# container left exited. Does nothing while the stack's own unit is
# stopped, so `systemctl stop bess-cloud|bess-edge` still means stopped.
systemctl is-active --quiet bess-cloud || systemctl is-active --quiet bess-edge || exit 0

for c in $(docker ps -a --filter name=bess- --format '{{.Names}}'); do
    state=$(docker inspect -f '{{.State.Status}} {{if .State.Health}}{{.State.Health.Status}}{{end}}' "$c")
    case "$state" in
        "running unhealthy")
            logger -t bess-watchdog "$c unhealthy - restarting"
            docker restart "$c" >/dev/null ;;
        running*) ;;
        *)
            logger -t bess-watchdog "$c is '$state' - starting"
            docker start "$c" >/dev/null ;;
    esac
done

# Disk headroom: InfluxDB, Docker images and logs all grow on the root fs.
# Warn early, and reclaim build cache before it can fill the disk.
use=$(df --output=pcent / | tail -1 | tr -dc 0-9)
if [ "${use:-0}" -ge 85 ]; then
    logger -t bess-watchdog "root filesystem ${use}% full - pruning docker build cache"
    docker builder prune -f >/dev/null 2>&1 || true
fi

