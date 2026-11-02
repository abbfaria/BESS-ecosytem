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

# Never wait at the boot menu. After the host died on 2026-11-02 the edge VM
# sat at GRUB with no countdown until someone pressed Enter, so the whole
# stack stayed down. Force a short, unconditional timeout (the recordfail
# variant covers boots that follow an unclean shutdown).
if [ -f /etc/default/grub ]; then
    sed -i -e '/^GRUB_TIMEOUT=/d' -e '/^GRUB_RECORDFAIL_TIMEOUT=/d' -e '/^GRUB_TIMEOUT_STYLE=/d' /etc/default/grub
    printf 'GRUB_TIMEOUT=3\nGRUB_TIMEOUT_STYLE=menu\nGRUB_RECORDFAIL_TIMEOUT=3\n' >> /etc/default/grub
    if command -v update-grub >/dev/null; then update-grub; fi
    # Debian's grubenv may still carry a recordfail flag from the crash.
    command -v grub-editenv >/dev/null && grub-editenv /boot/grub/grubenv unset recordfail || true
fi

