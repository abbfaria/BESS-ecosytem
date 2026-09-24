"""
Cloud API — FastAPI application.

Responsibilities:
  1. Subscribe to all edge MQTT uplinks (via aiomqtt)
  2. Write telemetry to InfluxDB 2.x
  3. Expose REST API for Grafana and web clients
  4. Send schedule/command downlinks to edges
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from .influx_writer import InfluxWriter
from .market_fetcher import fetch_dam_prices
from .mqtt_subscriber import MQTTSubscriber
from .router import create_router
from .logging_config import get_logger
from .history import ensure_history

log = get_logger(__name__)

MQTT_BROKER_HOST  = os.environ.get("MQTT_BROKER_HOST",  "mosquitto")
MQTT_BROKER_PORT  = int(os.environ.get("MQTT_BROKER_PORT", "1883"))   # internal LAN port
INFLUXDB_URL      = os.environ.get("INFLUXDB_URL",      "http://influxdb:8086")
INFLUXDB_TOKEN    = os.environ.get("INFLUXDB_TOKEN",    "")
INFLUXDB_ORG      = os.environ.get("INFLUXDB_ORG",      "bess")
INFLUXDB_BUCKET   = os.environ.get("INFLUXDB_BUCKET",   "telemetry")
# Device(s) to push fetched market prices to. Single-device testbed today;
# comma-separated for a small fleet.
MARKET_PUSH_DEVICE_IDS = [
    d.strip() for d in os.environ.get("MARKET_PUSH_DEVICE_IDS", "bess-edge-01").split(",")
    if d.strip()
]
# Ukrainian DAM auction results publish ~13:00-14:00 UTC; fetch a bit ahead
# of edge's own 14:00 UTC attempt so the MQTT push already has fresh data
# waiting.
MARKET_FETCH_HOUR_UTC   = int(os.environ.get("MARKET_FETCH_HOUR_UTC", "13"))
MARKET_FETCH_MINUTE_UTC = int(os.environ.get("MARKET_FETCH_MINUTE_UTC", "30"))
MARKET_FETCH_RETRY_S    = 900  # retry cadence while today's prices aren't published yet


_writer:     InfluxWriter    = None   # type: ignore
_subscriber: MQTTSubscriber  = None   # type: ignore
# Long-lived background tasks. References are retained deliberately: the
# event loop keeps only weak references to tasks, so a bare
# asyncio.create_task(...) whose result is discarded can be garbage
# collected while still running.
_market_task:    "asyncio.Task | None" = None
_subscriber_task:"asyncio.Task | None" = None
_backfill_task:  "asyncio.Task | None" = None


async def _backfill_history_task() -> None:
    """One-shot at startup: fill any gaps in the last BACKFILL_DAYS of
    history and ensure today/tomorrow's forecast schedule exists. See
    history.py."""
    for device_id in MARKET_PUSH_DEVICE_IDS:
        try:
            await ensure_history(_writer, INFLUXDB_BUCKET, device_id)
        except Exception as exc:
            log.error("History backfill failed", device_id=device_id, exc=str(exc))


async def _market_fetch_loop() -> None:
    """Daily: fetch tomorrow's real DAM prices from oree.com.ua, store them,
    and push them down to edge devices over MQTT. Retries every 15 minutes
    within the day if tomorrow's prices aren't published yet."""
    while True:
        now = datetime.now(timezone.utc)
        next_run = now.replace(hour=MARKET_FETCH_HOUR_UTC, minute=MARKET_FETCH_MINUTE_UTC,
                                second=0, microsecond=0)
        if now >= next_run:
            next_run += timedelta(days=1)
        await asyncio.sleep((next_run - now).total_seconds())

        tomorrow = (datetime.now(timezone.utc) + timedelta(days=1)).date()
        for attempt in range(1, 9):   # give up after ~2h of retrying that day
            prices = await fetch_dam_prices(tomorrow)
            if prices is not None:
                log.info("Real DAM prices fetched from oree.com.ua",
                         date=tomorrow.isoformat(), attempt=attempt,
                         min=min(prices), max=max(prices))
                if _writer:
                    await _writer.write_market_price(
                        "fleet", tomorrow.isoformat(), prices, "oree"
                    )
                if _subscriber:
                    payload = json.dumps({
                        "valid_date": tomorrow.isoformat(),
                        "prices": [{"price_uah_mwh": p} for p in prices],
                    })
                    for device_id in MARKET_PUSH_DEVICE_IDS:
                        await _subscriber.publish(f"bess/{device_id}/market", payload, qos=1)
                break
            log.warning("oree.com.ua fetch returned no data, will retry",
                        date=tomorrow.isoformat(), attempt=attempt)
            await asyncio.sleep(MARKET_FETCH_RETRY_S)
        else:
            log.error("Gave up fetching real DAM prices for the day",
                      date=tomorrow.isoformat())


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    global _writer, _subscriber, _market_task, _subscriber_task, _backfill_task
    log.info("Cloud API starting")
    _writer = InfluxWriter(
        url=INFLUXDB_URL,
        token=INFLUXDB_TOKEN,
        org=INFLUXDB_ORG,
        bucket=INFLUXDB_BUCKET,
    )
    await _writer.connect()

    # Guarantee Grafana has a full picture soon after this container is up
    # — a continuous history for lookback panels (esp. the 30-day payback
    # panel) and a today/tomorrow forecast — instead of depending on
    # wall-clock time to pass with the stack continuously running. See
    # history.py for the full rationale. Backfilling ~35 days (real price
    # fetches included) can take minutes, so this runs in the background
    # rather than blocking startup/the health check, same as
    # _market_fetch_loop below.
    _backfill_task = asyncio.create_task(_backfill_history_task(), name="history-backfill")

    _subscriber = MQTTSubscriber(
        host=MQTT_BROKER_HOST,
        port=MQTT_BROKER_PORT,
        on_telemetry=_writer.write_telemetry,
        on_status=_writer.write_status,
        on_fault=_writer.write_fault,
        on_schedule=_writer.write_schedule,
    )
    _subscriber_task = asyncio.create_task(_subscriber.run(), name="mqtt-subscriber")
    _market_task = asyncio.create_task(_market_fetch_loop(), name="market-fetcher")

    log.info("Cloud API ready", influx=INFLUXDB_URL, mqtt=MQTT_BROKER_HOST)
    yield

    log.info("Cloud API shutting down")
    for task in (_market_task, _subscriber_task, _backfill_task):
        if task:
            task.cancel()
    if _subscriber:
        await _subscriber.stop()
    if _writer:
        await _writer.close()


def create_app() -> FastAPI:
    app = FastAPI(
        title="BESS Cloud API",
        description="Cloud management API for BESS fleet monitoring and control",
        version="1.0.0",
        lifespan=lifespan,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    router = create_router(
        get_writer=lambda: _writer,
        get_subscriber=lambda: _subscriber,
    )
    app.include_router(router, prefix="/api/v1")

    @app.get("/health", tags=["system"])
    async def health() -> dict:
        return {
            "status": "ok",
            "ts":     datetime.now(timezone.utc).isoformat(),
            "influx": _writer.is_connected if _writer else False,
            "mqtt":   _subscriber.is_connected if _subscriber else False,
        }

    return app


app = create_app()
