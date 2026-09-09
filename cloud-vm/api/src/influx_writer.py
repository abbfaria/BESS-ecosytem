"""
InfluxDB 2.x writer for BESS telemetry, status, and fault events.

Uses the official influxdb-client-python async API.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Optional

try:
    from influxdb_client.client.influxdb_client_async import (  # type: ignore
        InfluxDBClientAsync,
    )
    from influxdb_client.client.write_api import ASYNCHRONOUS  # type: ignore
    from influxdb_client import Point, WritePrecision          # type: ignore
    _INFLUX_OK = True
except ImportError:
    _INFLUX_OK = False

from .logging_config import get_logger

log = get_logger(__name__)


class InfluxWriter:
    def __init__(self, url: str, token: str, org: str, bucket: str) -> None:
        self._url    = url
        self._token  = token
        self._org    = org
        self._bucket = bucket
        self._client = None
        self._write  = None
        self._ok     = False

    async def connect(self) -> None:
        if not _INFLUX_OK:
            log.warning("influxdb-client not installed — metrics will not be persisted")
            return
        try:
            self._client = InfluxDBClientAsync(
                url=self._url, token=self._token, org=self._org
            )
            self._write = self._client.write_api()
            # Quick health check
            await self._client.ping()
            self._ok = True
            log.info("InfluxDB connected", url=self._url, org=self._org)
        except Exception as exc:
            log.error("InfluxDB connection failed", exc=str(exc))

    async def close(self) -> None:
        if self._client:
            await self._client.close()

    @property
    def is_connected(self) -> bool:
        return self._ok

    # ── Write helpers ──────────────────────────────────────────────────────────

    async def write_telemetry(self, device_id: str, data: dict) -> None:
        if not self._ok:
            return
        try:
            p = (
                Point("telemetry")
                .tag("device_id", device_id)
                .tag("grid_status",   data.get("grid_status", "CONNECTED"))
                .tag("inverter_mode", data.get("inverter_mode", "SOLAR_PRIORITY"))
                .field("pv_power_w",        float(data.get("pv_power_w", 0)))
                .field("wind_power_w",       float(data.get("wind_power_w", 0)))
                .field("battery_power_w",    float(data.get("battery_power_w", 0)))
                .field("grid_power_w",       float(data.get("grid_power_w", 0)))
                .field("house_load_w",       float(data.get("house_load_w", 0)))
                .field("battery_soc_pct",    float(data.get("battery_soc_pct", 0)))
                .field("battery_soh_pct",    float(data.get("battery_soh_pct", 100)))
                .field("battery_temp_c",     float(data.get("battery_temp_c", 0)))
                .field("battery_voltage_v",  float(data.get("battery_voltage_v", 0)))
                .field("grid_voltage_v",     float(data.get("grid_voltage_v", 0)))
                .field("grid_frequency_hz",  float(data.get("grid_frequency_hz", 50)))
                .field("inverter_temp_c",    float(data.get("inverter_temp_c", 0)))
                .field("fault_code",         int(data.get("fault_code", 0)))
                .time(data.get("ts"), WritePrecision.NS)
            )
            await self._write.write(bucket=self._bucket, org=self._org, record=p)
        except Exception as exc:
            log.error("InfluxDB write_telemetry error", exc=str(exc))

    async def write_status(self, device_id: str, data: dict) -> None:
        if not self._ok:
            return
        try:
            p = (
                Point("device_status")
                .tag("device_id", device_id)
                .tag("health", data.get("health", "UNKNOWN"))
                .field("uptime_s",           int(data.get("uptime_s", 0)))
                .field("mqtt_buffer_count",   int(data.get("mqtt_buffer_count", 0)))
                .field("cloud_connected",     int(data.get("cloud_connected", False)))
                .time(data.get("ts"), WritePrecision.NS)
            )
            await self._write.write(bucket=self._bucket, org=self._org, record=p)
        except Exception as exc:
            log.error("InfluxDB write_status error", exc=str(exc))

    async def write_fault(self, device_id: str, data: dict) -> None:
        if not self._ok:
            return
        try:
            p = (
                Point("fault_event")
                .tag("device_id", device_id)
                .tag("severity", data.get("severity", "WARNING"))
                .tag("component", data.get("component", "unknown"))
                .field("fault_code", int(data.get("fault_code", 0)))
                .field("message",    str(data.get("message", "")))
                .time(data.get("ts"), WritePrecision.NS)
            )
            await self._write.write(bucket=self._bucket, org=self._org, record=p)
        except Exception as exc:
            log.error("InfluxDB write_fault error", exc=str(exc))

    async def query(self, flux: str) -> list[dict]:
        """Run a Flux query and return rows as dicts."""
        if not self._ok:
            return []
        try:
            query_api = self._client.query_api()
            tables = await query_api.query(flux, org=self._org)
            rows = []
            for table in tables:
                for record in table.records:
                    rows.append(record.values)
            return rows
        except Exception as exc:
            log.error("InfluxDB query error", exc=str(exc))
            return []
