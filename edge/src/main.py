"""
Edge node: publishes the BESS state (see bess_model.py) to the cloud over
MQTT every TELEMETRY_INTERVAL_S seconds.

Connection is mTLS by default (CA_CERT / CLIENT_CERT / CLIENT_KEY);
MQTT_TLS=false allows a plain connection for local testing. paho's network
thread reconnects on its own after a dropped link.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

from .bess_model import state_at

DEVICE_ID   = os.environ.get("DEVICE_ID", "bess-edge-01")
MQTT_HOST   = os.environ.get("MQTT_HOST", "localhost")
MQTT_PORT   = int(os.environ.get("MQTT_PORT", "8883"))
MQTT_TLS    = os.environ.get("MQTT_TLS", "true").lower() == "true"
CA_CERT     = os.environ.get("CA_CERT", "/certs/ca.crt")
CLIENT_CERT = os.environ.get("CLIENT_CERT", "/certs/edge.crt")
CLIENT_KEY  = os.environ.get("CLIENT_KEY", "/certs/edge.key")
INTERVAL_S  = float(os.environ.get("TELEMETRY_INTERVAL_S", "10"))

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"),
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("edge")


def main() -> None:
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=DEVICE_ID)
    if MQTT_TLS:
        client.tls_set(ca_certs=CA_CERT, certfile=CLIENT_CERT, keyfile=CLIENT_KEY)
    client.on_connect = lambda c, u, f, rc, p: log.info("MQTT connected: %s", rc)
    client.on_disconnect = lambda c, u, f, rc, p: log.warning("MQTT disconnected: %s", rc)
    client.connect_async(MQTT_HOST, MQTT_PORT, keepalive=30)
    client.loop_start()

    topic = f"bess/{DEVICE_ID}/telemetry"
    log.info("Publishing to %s:%s topic %s every %ss", MQTT_HOST, MQTT_PORT, topic, INTERVAL_S)
    while True:
        state = state_at(datetime.now(timezone.utc), DEVICE_ID)
        if client.is_connected():
            client.publish(topic, json.dumps(state), qos=0)
        log.debug("%s", state)
        time.sleep(INTERVAL_S)


if __name__ == "__main__":
    main()
