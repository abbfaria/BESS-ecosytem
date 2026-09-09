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
from datetime import datetime, timezone
from typing import AsyncIterator

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from .influx_writer import InfluxWriter
from .mqtt_subscriber import MQTTSubscriber
from .router import create_router
from .logging_config import get_logger

log = get_logger(__name__)

MQTT_BROKER_HOST  = os.environ.get("MQTT_BROKER_HOST",  "mosquitto")
MQTT_BROKER_PORT  = int(os.environ.get("MQTT_BROKER_PORT", "1883"))   # internal LAN port
INFLUXDB_URL      = os.environ.get("INFLUXDB_URL",      "http://influxdb:8086")
INFLUXDB_TOKEN    = os.environ.get("INFLUXDB_TOKEN",    "")
INFLUXDB_ORG      = os.environ.get("INFLUXDB_ORG",      "bess")
INFLUXDB_BUCKET   = os.environ.get("INFLUXDB_BUCKET",   "telemetry")


_writer:     InfluxWriter    = None   # type: ignore
_subscriber: MQTTSubscriber  = None   # type: ignore


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    global _writer, _subscriber
    log.info("Cloud API starting")
    _writer = InfluxWriter(
        url=INFLUXDB_URL,
        token=INFLUXDB_TOKEN,
        org=INFLUXDB_ORG,
        bucket=INFLUXDB_BUCKET,
    )
    await _writer.connect()

    _subscriber = MQTTSubscriber(
        host=MQTT_BROKER_HOST,
        port=MQTT_BROKER_PORT,
        on_telemetry=_writer.write_telemetry,
        on_status=_writer.write_status,
        on_fault=_writer.write_fault,
    )
    asyncio.create_task(_subscriber.run(), name="mqtt-subscriber")

    log.info("Cloud API ready", influx=INFLUXDB_URL, mqtt=MQTT_BROKER_HOST)
    yield

    log.info("Cloud API shutting down")
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
