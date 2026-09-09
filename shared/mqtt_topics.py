"""
MQTT topic definitions shared between Edge and Cloud.
All topics follow: bess/{device_id}/{category}
"""

TOPIC_PREFIX = "bess"

# ── Uplink (Edge → Cloud) ─────────────────────────────────────────────────
TOPIC_TELEMETRY    = "{prefix}/{device_id}/telemetry"
TOPIC_STATUS       = "{prefix}/{device_id}/status"
TOPIC_MARKET_ACK   = "{prefix}/{device_id}/market_ack"
TOPIC_CMD_ACK      = "{prefix}/{device_id}/cmd_ack"
TOPIC_LWT          = "{prefix}/{device_id}/lwt"
TOPIC_FAULT        = "{prefix}/{device_id}/fault"
TOPIC_BUFFER_STATS = "{prefix}/{device_id}/buffer_stats"

# ── Downlink (Cloud → Edge) ───────────────────────────────────────────────
TOPIC_SCHEDULE     = "{prefix}/{device_id}/schedule"
TOPIC_COMMAND      = "{prefix}/{device_id}/cmd"
TOPIC_MARKET_PUSH  = "{prefix}/{device_id}/market"

# ── Wildcard subscriptions (Cloud side) ───────────────────────────────────
TOPIC_ALL_TELEMETRY    = f"{TOPIC_PREFIX}/+/telemetry"
TOPIC_ALL_STATUS       = f"{TOPIC_PREFIX}/+/status"
TOPIC_ALL_CMD_ACK      = f"{TOPIC_PREFIX}/+/cmd_ack"
TOPIC_ALL_BUFFER_STATS = f"{TOPIC_PREFIX}/+/buffer_stats"
TOPIC_ALL_FAULT        = f"{TOPIC_PREFIX}/+/fault"
TOPIC_ALL_LWT          = f"{TOPIC_PREFIX}/+/lwt"


def build(template: str, device_id: str, prefix: str = TOPIC_PREFIX) -> str:
    return template.format(prefix=prefix, device_id=device_id)


# QoS levels
QOS_TELEMETRY  = 0   # high-frequency, loss acceptable
QOS_STATUS     = 1   # at-least-once
QOS_SCHEDULE   = 1   # guaranteed delivery
QOS_COMMAND    = 1   # guaranteed delivery
QOS_FAULT      = 1   # guaranteed delivery
QOS_LWT        = 1   # guaranteed delivery
