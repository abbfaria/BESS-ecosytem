"""
Cloud API REST router.

Endpoints:
  GET  /devices               — list all known devices
  GET  /devices/{id}/telemetry — latest telemetry from InfluxDB
  GET  /devices/{id}/history  — time-range query
  POST /devices/{id}/command  — issue mode command (via MQTT downlink)
  POST /devices/{id}/schedule — push schedule to edge
  GET  /market/prices         — current cached DAM prices
  GET  /stats                 — fleet-wide statistics
"""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Callable, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from .logging_config import get_logger

log = get_logger(__name__)

MQTT_BROKER_HOST = os.environ.get("MQTT_BROKER_HOST", "mosquitto")


# Path/query values are interpolated into Flux and MQTT topics, so they are
# restricted to the identifier alphabet the system itself uses.
_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
_VALID_MODES = {"SOLAR_PRIORITY", "GRID_CHARGE", "DISCHARGE_SELL", "BACKUP_MODE", "IDLE"}


def _check_id(value: str, what: str) -> str:
    if not _ID_RE.match(value):
        raise HTTPException(400, f"Invalid {what}")
    return value


class CommandRequest(BaseModel):
    mode:         str
    power_pct:    float = 0.0
    duration_min: int   = 60


class ScheduleSlot(BaseModel):
    hour:               int
    mode:               str
    charge_power_pct:   float = 0.0
    discharge_power_pct:float = 0.0
    price_uah_mwh:      float = 0.0


class ScheduleRequest(BaseModel):
    valid_date: str
    slots:      list[ScheduleSlot]


def create_router(
    get_writer:     Callable,
    get_subscriber: Callable,
) -> APIRouter:
    router = APIRouter()

    # ── Devices ───────────────────────────────────────────────────────────────

    @router.get("/devices", tags=["fleet"])
    async def list_devices() -> dict:
        sub = get_subscriber()
        devices = sub.stats().get("devices", []) if sub else []
        return {"devices": devices, "count": len(devices)}

    @router.get("/devices/{device_id}/telemetry", tags=["monitoring"])
    async def get_telemetry(device_id: str, limit: int = Query(1, ge=1, le=1000)) -> list:
        _check_id(device_id, "device_id")
        writer = get_writer()
        if not writer or not writer.is_connected:
            raise HTTPException(503, "InfluxDB unavailable")
        flux = f"""
from(bucket: "telemetry")
  |> range(start: -1h)
  |> filter(fn: (r) => r["_measurement"] == "telemetry")
  |> filter(fn: (r) => r["device_id"] == "{device_id}")
  |> last()
  |> limit(n: {limit})
"""
        rows = await writer.query(flux)
        return rows

    @router.get("/devices/{device_id}/history", tags=["monitoring"])
    async def get_history(
        device_id: str,
        hours:     int = Query(1, ge=1, le=168),
        field:     str = Query("battery_soc_pct"),
    ) -> list:
        _check_id(device_id, "device_id")
        _check_id(field, "field")
        writer = get_writer()
        if not writer or not writer.is_connected:
            raise HTTPException(503, "InfluxDB unavailable")
        flux = f"""
from(bucket: "telemetry")
  |> range(start: -{hours}h)
  |> filter(fn: (r) => r["_measurement"] == "telemetry")
  |> filter(fn: (r) => r["device_id"] == "{device_id}")
  |> filter(fn: (r) => r["_field"] == "{field}")
  |> aggregateWindow(every: 1m, fn: mean, createEmpty: false)
"""
        rows = await writer.query(flux)
        return rows

    # ── Commands ──────────────────────────────────────────────────────────────

    @router.post("/devices/{device_id}/command", tags=["control"], status_code=202)
    async def post_command(device_id: str, cmd: CommandRequest) -> dict:
        _check_id(device_id, "device_id")
        if cmd.mode not in _VALID_MODES:
            raise HTTPException(400, f"Invalid mode: {cmd.mode}")
        if not (0 <= cmd.power_pct <= 100 and 1 <= cmd.duration_min <= 1440):
            raise HTTPException(400, "power_pct must be 0-100 and duration_min 1-1440")

        cmd_id = f"cloud-{datetime.now(timezone.utc).timestamp():.0f}"
        payload = json.dumps({
            "cmd_id":       cmd_id,
            "ts":           datetime.now(timezone.utc).isoformat(),
            "device_id":    device_id,
            "mode":         cmd.mode,
            "power_pct":    cmd.power_pct,
            "duration_min": cmd.duration_min,
            "issued_by":    "cloud-api",
        })
        if not await _mqtt_publish(f"bess/{device_id}/cmd", payload):
            raise HTTPException(502, "Command could not be handed to the MQTT broker")
        # "queued" — the device's verdict arrives on cmd_ack; see
        # GET /devices/{id}/command/last.
        return {"queued": True, "cmd_id": cmd_id, "device_id": device_id, "mode": cmd.mode}

    @router.get("/devices/{device_id}/command/last", tags=["control"])
    async def last_command_ack(device_id: str) -> dict:
        _check_id(device_id, "device_id")
        sub = get_subscriber()
        ack = sub.last_acks.get(device_id) if sub else None
        if ack is None:
            raise HTTPException(404, "No command acknowledgement received yet")
        return ack

    @router.post("/devices/{device_id}/schedule", tags=["control"], status_code=202)
    async def push_schedule(device_id: str, req: ScheduleRequest) -> dict:
        _check_id(device_id, "device_id")
        if len(req.slots) != 24:
            raise HTTPException(400, "Schedule must have exactly 24 slots")
        payload = json.dumps({
            "schedule_id":   f"cloud-{device_id}-{req.valid_date}",
            "generated_ts":  datetime.now(timezone.utc).isoformat(),
            "valid_date":    req.valid_date,
            "device_id":     device_id,
            "slots":         [s.model_dump() for s in req.slots],
        })
        if not await _mqtt_publish(f"bess/{device_id}/schedule", payload):
            raise HTTPException(502, "Schedule could not be handed to the MQTT broker")
        return {"accepted": True, "device_id": device_id, "valid_date": req.valid_date}

    # ── Stats ─────────────────────────────────────────────────────────────────

    @router.get("/stats", tags=["fleet"])
    async def fleet_stats() -> dict:
        sub = get_subscriber()
        return {
            "ts":      datetime.now(timezone.utc).isoformat(),
            "subscriber": sub.stats() if sub else {},
        }

    return router


async def _mqtt_publish(topic: str, payload: str) -> bool:
    """Publish via aiomqtt to the broker's internal port; False on failure."""
    try:
        import aiomqtt  # type: ignore
        async with aiomqtt.Client(
            hostname=MQTT_BROKER_HOST, port=1883, identifier="cloud-api-pub"
        ) as client:
            await client.publish(topic, payload.encode(), qos=1)
        return True
    except Exception as exc:
        log.error("MQTT publish error", topic=topic, exc=str(exc))
        return False
