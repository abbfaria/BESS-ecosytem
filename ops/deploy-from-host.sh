#!/bin/sh
# Deploy the working tree to both VMs from the VirtualBox host and verify.
# Same sync rules as the CI workflow (never touches .env, certs/ or data/).
#
#   ops/deploy-from-host.sh            # deploy + verify
#   ops/deploy-from-host.sh verify     # verify only
set -eu
REPO="$(cd "$(dirname "$0")/.." && pwd)"
KEY="${KEY:-$HOME/.ssh/id_ed25519_bess}"
CLOUD="${CLOUD:-cloud-user@127.0.0.1 -p 2222}"
EDGE="${EDGE:-edge-user@127.0.0.1 -p 2223}"
SSH="ssh -i $KEY -o BatchMode=yes -o ConnectTimeout=10"
DEST='~/Code/bess-ecosystem'

sync_to() {   # $1 = "user@host -p port", $2 = local dir, $3 = remote subdir
    port=$(echo "$1" | sed 's/.*-p //'); host=$(echo "$1" | cut -d' ' -f1)
    rsync -a --delete --exclude .env --exclude 'certs/' --exclude 'data/' --exclude '__pycache__/' \
        -e "ssh -i $KEY -p $port" "$2/" "$host:$DEST/$3/"
}

if [ "${1:-}" != verify ]; then
    echo "== cloud: sync + rebuild"
    sync_to "$CLOUD" "$REPO/cloud" cloud
    sync_to "$CLOUD" "$REPO/edge/src" edge/src
    sync_to "$CLOUD" "$REPO/ops" ops
    $SSH $CLOUD "cd $DEST/cloud && docker compose -p bess-cloud up -d --build"

    echo "== edge: sync + rebuild"
    sync_to "$EDGE" "$REPO/edge" edge
    sync_to "$EDGE" "$REPO/ops" ops
    $SSH $EDGE "cd $DEST/edge && docker compose -p bess-edge up -d --build"
fi

for vm in "$CLOUD" "$EDGE"; do
    echo "================ $vm"
    $SSH $vm '
        echo "-- clock (host vs guest)"; date -u
        echo "-- timers"; systemctl list-timers --no-pager "bess-*" 2>/dev/null | head -6
        echo "-- grub";   grep -E "^GRUB_(TIMEOUT|RECORDFAIL_TIMEOUT)=" /etc/default/grub || echo "GRUB timeout NOT set"
        echo "-- disk";   df -h / | tail -1
        echo "-- containers"; docker ps -a --filter name=bess- --format "{{.Names}}\t{{.Status}}"
        echo "-- cert expiry"
        for c in ~/Code/bess-ecosystem/*/certs/*.crt; do
            [ -f "$c" ] && printf "%s  %s\n" "$(openssl x509 -enddate -noout -in "$c" | cut -d= -f2)" "$c"
        done
        grep -h "^MQTT_HOST" ~/Code/bess-ecosystem/edge/.env 2>/dev/null || true'
done
date -u
