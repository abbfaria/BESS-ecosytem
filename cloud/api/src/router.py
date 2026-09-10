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
from datetime import datetime, timedelta, timezone
from typing import Callable, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from .logging_config import get_logger

log = get_logger(__name__)

MQTT_BROKER_HOST = os.environ.get("MQTT_BROKER_HOST", "mosquitto")


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
        valid_modes = {"SOLAR_PRIORITY", "GRID_CHARGE", "DISCHARGE_SELL",
                       "BACKUP_MODE", "IDLE"}
        if cmd.mode not in valid_modes:
            raise HTTPException(400, f"Invalid mode: {cmd.mode}")

        payload = json.dumps({
            "cmd_id":       f"cloud-{datetime.now(timezone.utc).timestamp():.0f}",
            "ts":           datetime.now(timezone.utc).isoformat(),
            "device_id":    device_id,
            "mode":         cmd.mode,
            "power_pct":    cmd.power_pct,
            "duration_min": cmd.duration_min,
            "issued_by":    "cloud-api",
        })
        await _mqtt_publish(f"bess/{device_id}/cmd", payload)
        return {"accepted": True, "device_id": device_id, "mode": cmd.mode}

    @router.post("/devices/{device_id}/schedule", tags=["control"], status_code=202)
    async def push_schedule(device_id: str, req: ScheduleRequest) -> dict:
        if len(req.slots) != 24:
            raise HTTPException(400, "Schedule must have exactly 24 slots")
        payload = json.dumps({
            "schedule_id":   f"cloud-{device_id}-{req.valid_date}",
            "generated_ts":  datetime.now(timezone.utc).isoformat(),
            "valid_date":    req.valid_date,
            "device_id":     device_id,
            "slots":         [s.model_dump() for s in req.slots],
        })
        await _mqtt_publish(f"bess/{device_id}/schedule", payload)
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


async def _mqtt_publish(topic: str, payload: str) -> None:
    """Fire-and-forget publish via aiomqtt to broker internal port."""
    try:
        import aiomqtt  # type: ignore
        async with aiomqtt.Client(
            hostname=MQTT_BROKER_HOST, port=1883, identifier="cloud-api-pub"
        ) as client:
            await client.publish(topic, payload.encode(), qos=1)
    except Exception as exc:
        log.error("MQTT publish error", topic=topic, exc=str(exc))
