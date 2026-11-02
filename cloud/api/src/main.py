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
from datetime import date, datetime, timedelta, timezone
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
# How often to re-check history/forecast completeness after startup. Hourly
# bounds how long a gap opened by a host suspension can stay visible.
BACKFILL_INTERVAL_S     = int(os.environ.get("BACKFILL_INTERVAL_S", "3600"))
# Ceiling for one maintenance pass. A cold 35-day backfill with live price
# fetches takes minutes; anything approaching this is wedged, not slow.
BACKFILL_MAX_RUNTIME_S  = int(os.environ.get("BACKFILL_MAX_RUNTIME_S", "1800"))


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
    """Keep history and forecast continuously complete — at startup *and*
    periodically thereafter.

    This used to run exactly once, at startup. That was enough only if the
    stack then ran uninterrupted: whenever the host was suspended (the
    normal case for a laptop-hosted testbed) the hours that elapsed while
    it was down were never filled, because nothing re-ran the check. The
    dashboard then showed whichever handful of hours happened to be live —
    e.g. only 14:00-17:00 of a day, which in Ukraine's price profile is
    precisely the midday solar-glut window where prices sit near the
    technical minimum. The resulting "daily revenue" was arithmetically
    correct and economically meaningless.

    ensure_history() is idempotent and skips fully-covered days cheaply, so
    re-running it hourly costs little and keeps every panel complete no
    matter how the host's uptime is distributed.
    """
    first = True
    while True:
        started = datetime.now(timezone.utc)
        for device_id in MARKET_PUSH_DEVICE_IDS:
            try:
                # Bound the whole pass. Any single await inside it that never
                # returns — a wedged browser pipe after the host suspends is
                # the observed case — would otherwise stop this loop forever,
                # silently: the task simply never reaches its next iteration,
                # so there is no exception to log and no restart to trigger.
                # Losing one pass costs nothing; ensure_history is idempotent
                # and the next pass redoes the work.
                await asyncio.wait_for(
                    ensure_history(_writer, INFLUXDB_BUCKET, device_id),
                    timeout=BACKFILL_MAX_RUNTIME_S,
                )
            except asyncio.TimeoutError:
                log.error("History backfill exceeded its runtime budget — "
                          "abandoning this pass, will retry next interval",
                          device_id=device_id, budget_s=BACKFILL_MAX_RUNTIME_S)
            except Exception as exc:
                log.error("History backfill failed", device_id=device_id, exc=str(exc))

        if first:
            log.info("History maintenance now running periodically",
                     interval_min=BACKFILL_INTERVAL_S // 60)
            first = False
        # Heartbeat: makes a stalled loop visible in the log instead of
        # having to infer it from missing data days later.
        log.info("History maintenance pass finished",
                 took_s=round((datetime.now(timezone.utc) - started).total_seconds(), 1),
                 next_in_min=BACKFILL_INTERVAL_S // 60)
        await asyncio.sleep(BACKFILL_INTERVAL_S)


async def _publish_market(day_iso: str, prices: list[float]) -> bool:
    """Push a price curve to every device, *retained*. A device that is
    offline at push time — routine on a laptop testbed — receives the latest
    curve as soon as it reconnects, instead of losing it and running the
    next day on its 14-day average."""
    if not _subscriber:
        return False
    payload = json.dumps({
        "valid_date": day_iso,
        "prices": [{"price_uah_mwh": p} for p in prices],
    })
    ok = True
    for device_id in MARKET_PUSH_DEVICE_IDS:
        ok &= await _subscriber.publish(f"bess/{device_id}/market", payload,
                                        qos=1, retain=True)
    return ok


async def _market_fetch_loop() -> None:
    """Fetch tomorrow's real DAM prices once they are due, store them, and
    push them to the devices.

    Driven by the wall clock, polled every minute. The previous version
    slept on asyncio's monotonic clock until 13:30, which stands still while
    the host is suspended, so after a suspend it fired hours late — or, if
    the sleep spanned midnight, asked for the wrong day and gave up after
    two hours. Now it simply keeps trying (every MARKET_FETCH_RETRY_S) from
    13:30 UTC until the day's curve is in, and retries the MQTT push too.
    """
    fetched: dict[str, list[float]] = {}   # date -> prices
    pushed: set[str] = set()
    last_try: dict[str, float] = {}
    loop = asyncio.get_running_loop()
    while True:
        try:
            now = datetime.now(timezone.utc)
            due = (now.hour, now.minute) >= (MARKET_FETCH_HOUR_UTC, MARKET_FETCH_MINUTE_UTC)
            tomorrow = (now + timedelta(days=1)).date().isoformat()
            if due and tomorrow not in fetched and \
                    loop.time() - last_try.get(tomorrow, -1e9) >= MARKET_FETCH_RETRY_S:
                last_try[tomorrow] = loop.time()
                prices = await fetch_dam_prices(date.fromisoformat(tomorrow))
                if prices is None:
                    log.warning("oree.com.ua has no prices for tomorrow yet, will retry",
                                date=tomorrow, retry_in_min=MARKET_FETCH_RETRY_S // 60)
                else:
                    log.info("Real DAM prices fetched from oree.com.ua",
                             date=tomorrow, min=min(prices), max=max(prices))
                    fetched[tomorrow] = prices
                    if _writer:
                        for device_id in MARKET_PUSH_DEVICE_IDS:
                            await _writer.write_market_price(device_id, tomorrow, prices, "oree")
            for day_iso, prices in fetched.items():
                if day_iso not in pushed and await _publish_market(day_iso, prices):
                    pushed.add(day_iso)
                    log.info("Market prices pushed to devices", date=day_iso)
            # Forget days that are over.
            today = now.date().isoformat()
            for d in [d for d in fetched if d < today]:
                fetched.pop(d); pushed.discard(d); last_try.pop(d, None)
        except Exception as exc:
            log.error("Market fetch loop error", exc=str(exc))
        await asyncio.sleep(60)


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
        on_cmd_ack=_writer.write_cmd_ack,
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
