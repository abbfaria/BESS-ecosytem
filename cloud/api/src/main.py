"""
Cloud API: receives edge telemetry over MQTT, stores it in InfluxDB, and
keeps yesterday/today/tomorrow populated for Grafana.

On startup the bucket is cleared and refilled from bess_model (the edge's
own model, copied into this image at build time — see Dockerfile):
  * `telemetry`          yesterday 00:00 UTC .. now, 5-min resolution
  * `telemetry_forecast` now .. end of tomorrow, 15-min resolution
  * `market_price`       hourly DAM prices for yesterday .. tomorrow
The fill is repeated every 5 minutes so the window moves with the clock. Live
telemetry from the edge is written to `telemetry` as it arrives; since both
sides evaluate the same model, the two match exactly.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, time, timedelta, timezone

import aiomqtt
from fastapi import FastAPI
from influxdb_client import Point, WritePrecision
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync

from .bess_model import prices, state_at

MQTT_HOST       = os.environ.get("MQTT_BROKER_HOST", "mosquitto")
MQTT_PORT       = int(os.environ.get("MQTT_BROKER_PORT", "1883"))
INFLUXDB_URL    = os.environ.get("INFLUXDB_URL", "http://influxdb:8086")
INFLUXDB_TOKEN  = os.environ.get("INFLUXDB_TOKEN", "")
INFLUXDB_ORG    = os.environ.get("INFLUXDB_ORG", "bess")
INFLUXDB_BUCKET = os.environ.get("INFLUXDB_BUCKET", "telemetry")
DEVICE_ID       = os.environ.get("DEVICE_ID", "bess-edge-01")
SEED_INTERVAL_S = 300

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"),
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("cloud-api")

_influx: InfluxDBClientAsync | None = None
_latest: dict[str, dict] = {}          # device_id -> last received telemetry
_tasks: list[asyncio.Task] = []

_FIELDS = ("battery_soc_pct", "battery_power_w", "pv_power_w", "house_load_w",
           "grid_power_w", "battery_temp_c", "grid_voltage_v", "price_uah_mwh",
           "revenue_today_uah")


def telemetry_point(measurement: str, data: dict) -> Point:
    p = Point(measurement).tag("device_id", data["device_id"]).field("mode", data["mode"])
    for f in _FIELDS:
        p = p.field(f, float(data[f]))
    return p.time(datetime.fromisoformat(data["ts"]), WritePrecision.S)


def seed_points(now: datetime, device_id: str) -> list[Point]:
    today = datetime.combine(now.date(), time(), tzinfo=timezone.utc)
    start, end = today - timedelta(days=1), today + timedelta(days=2)
    points = []
    t = start
    while t < now:
        points.append(telemetry_point("telemetry", state_at(t, device_id)))
        t += timedelta(minutes=5)
    points.append(telemetry_point("telemetry", state_at(now, device_id)))
    while t < end:
        points.append(telemetry_point("telemetry_forecast", state_at(t, device_id)))
        t += timedelta(minutes=15)
    for d in range(3):
        day = start + timedelta(days=d)
        for h, price in enumerate(prices(day.date())):
            points.append(Point("market_price").tag("device_id", device_id)
                          .field("price_uah_mwh", float(price))
                          .time(day + timedelta(hours=h), WritePrecision.S))
    return points


async def write(points) -> None:
    await _influx.write_api().write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=points)


async def seed_loop() -> None:
    while True:
        try:
            points = seed_points(datetime.now(timezone.utc), DEVICE_ID)
            await write(points)
            log.info("Seeded %d points (yesterday, today, tomorrow)", len(points))
        except Exception as exc:
            log.error("Seeding failed: %s", exc)
        await asyncio.sleep(SEED_INTERVAL_S)


async def mqtt_loop() -> None:
    while True:
        try:
            async with aiomqtt.Client(MQTT_HOST, MQTT_PORT, identifier="cloud-api") as client:
                await client.subscribe("bess/+/telemetry")
                log.info("Subscribed to bess/+/telemetry on %s:%s", MQTT_HOST, MQTT_PORT)
                async for msg in client.messages:
                    try:
                        data = json.loads(msg.payload)
                        await write(telemetry_point("telemetry", data))
                        _latest[data["device_id"]] = data
                    except Exception as exc:
                        log.warning("Bad telemetry on %s: %s", msg.topic, exc)
        except aiomqtt.MqttError as exc:
            log.warning("MQTT connection lost (%s), reconnecting in 5 s", exc)
            await asyncio.sleep(5)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _influx
    _influx = InfluxDBClientAsync(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
    # Start from a clean bucket: every point is reproducible from bess_model.
    await _influx.delete_api().delete(
        start=datetime(1970, 1, 1, tzinfo=timezone.utc),
        stop=datetime(2100, 1, 1, tzinfo=timezone.utc),
        predicate="", bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG)
    _tasks.append(asyncio.create_task(seed_loop()))
    _tasks.append(asyncio.create_task(mqtt_loop()))
    yield
    for task in _tasks:
        task.cancel()
    await _influx.close()


app = FastAPI(title="BESS Cloud API", lifespan=lifespan)


@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "ts": datetime.now(timezone.utc).isoformat()}


@app.get("/api/v1/latest")
async def latest() -> dict:
    return _latest
