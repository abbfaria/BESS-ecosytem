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

    def _telemetry_point(self, measurement: str, device_id: str, data: dict) -> "Point":
        return (
            Point(measurement)
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
            .field("revenue_uah",        float(data.get("revenue_uah", 0)))
            .field("fault_code",         int(data.get("fault_code", 0)))
            .time(data.get("ts"), WritePrecision.NS)
        )

    async def write_telemetry(self, device_id: str, data: dict) -> None:
        if not self._ok:
            return
        try:
            p = self._telemetry_point("telemetry", device_id, data)
            await self._write.write(bucket=self._bucket, org=self._org, record=p)
        except Exception as exc:
            log.error("InfluxDB write_telemetry error", exc=str(exc))

    async def write_telemetry_forecast(self, device_id: str, data: dict) -> None:
        """Same schema as write_telemetry, written to a separate
        `telemetry_forecast` measurement — a projection for a day that
        hasn't happened yet (see cloud/api/src/history.py), never to be
        confused with `telemetry`'s actual measurements even by accident."""
        if not self._ok:
            return
        try:
            p = self._telemetry_point("telemetry_forecast", device_id, data)
            await self._write.write(bucket=self._bucket, org=self._org, record=p)
        except Exception as exc:
            log.error("InfluxDB write_telemetry_forecast error", exc=str(exc))

    async def write_schedule(self, device_id: str, data: dict) -> None:
        """Store the edge's planned 24h dispatch schedule — one point per
        hour, timestamped at that hour's actual UTC start, so it can be
        queried by time range like any other series (e.g. "tomorrow's
        expected revenue")."""
        if not self._ok:
            return
        try:
            valid_date = data.get("valid_date")
            if not valid_date:
                return
            day = datetime.fromisoformat(valid_date).replace(tzinfo=timezone.utc)
            points = []
            for slot in data.get("slots", []):
                hour = int(slot.get("hour", 0))
                ts = day.replace(hour=hour, minute=0, second=0, microsecond=0)
                points.append(
                    Point("schedule")
                    .tag("device_id", device_id)
                    .tag("mode", slot.get("mode", "SOLAR_PRIORITY"))
                    .field("price_uah_mwh",       float(slot.get("price_uah_mwh", 0)))
                    .field("expected_revenue_uah", float(slot.get("expected_revenue_uah", 0)))
                    .time(ts, WritePrecision.NS)
                )
            if points:
                await self._write.write(bucket=self._bucket, org=self._org, record=points)
        except Exception as exc:
            log.error("InfluxDB write_schedule error", exc=str(exc))

    async def write_market_price(self, device_id: str, valid_date: str,
                                  prices: list[float], source: str) -> None:
        """Store a raw fetched DAM price curve (independent of the
        optimizer's schedule) — lets Grafana show the real market price
        curve even before/without an optimized dispatch plan."""
        if not self._ok:
            return
        try:
            day = datetime.fromisoformat(valid_date).replace(tzinfo=timezone.utc)
            points = [
                Point("market_price")
                .tag("device_id", device_id)
                .tag("source", source)
                .field("price_uah_mwh", float(p))
                .time(day.replace(hour=h, minute=0, second=0, microsecond=0), WritePrecision.NS)
                for h, p in enumerate(prices)
            ]
            await self._write.write(bucket=self._bucket, org=self._org, record=points)
        except Exception as exc:
            log.error("InfluxDB write_market_price error", exc=str(exc))

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
