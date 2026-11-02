#!/bin/sh
# Install the BESS host units on a cloud or edge VM (run with sudo from
# this directory). Safe to re-run.
set -e
cd "$(dirname "$0")"
install -m 0755 bess-watchdog.sh /usr/local/bin/bess-watchdog.sh
install -m 0644 bess-timesync.service bess-timesync.timer \
                bess-watchdog.service bess-watchdog.timer \
                bess-maintenance.service bess-maintenance.timer /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now bess-timesync.timer bess-watchdog.timer bess-maintenance.timer
if [ ! -f /etc/docker/daemon.json ]; then
    # Log rotation only applies to containers created after this.
    install -m 0644 docker-daemon.json /etc/docker/daemon.json
    echo "Wrote /etc/docker/daemon.json - restart docker to apply (recreates containers)."
fi
