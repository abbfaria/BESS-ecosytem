"""
Cloud-side MQTT subscriber.

Subscribes to all edge device uplinks and dispatches to the InfluxDB writer.
Uses aiomqtt (async wrapper around paho-mqtt) on the internal 1883 port.
"""

from __future__ import annotations

import asyncio
import json
import re
from typing import Callable, Awaitable, Optional

try:
    import aiomqtt  # type: ignore
    _AIOMQTT_OK = True
except ImportError:
    _AIOMQTT_OK = False

from .logging_config import get_logger

log = get_logger(__name__)

_TELEMETRY_RE = re.compile(r"^bess/([^/]+)/telemetry$")
_STATUS_RE    = re.compile(r"^bess/([^/]+)/status$")
_FAULT_RE     = re.compile(r"^bess/([^/]+)/fault$")
_LWT_RE       = re.compile(r"^bess/([^/]+)/lwt$")
_SCHEDULE_RE  = re.compile(r"^bess/([^/]+)/schedule$")

TelemetryCallback = Callable[[str, dict], Awaitable[None]]


class MQTTSubscriber:
    def __init__(
        self,
        host: str,
        port: int,
        on_telemetry: TelemetryCallback,
        on_status:    TelemetryCallback,
        on_fault:     TelemetryCallback,
        on_schedule:  Optional[TelemetryCallback] = None,
    ) -> None:
        self._host         = host
        self._port         = port
        self._on_telemetry = on_telemetry
        self._on_status    = on_status
        self._on_fault     = on_fault
        self._on_schedule  = on_schedule
        self._connected    = False
        self._running      = False
        self._client        = None
        self._stats        = {"received": 0, "errors": 0, "devices": set()}

    async def run(self) -> None:
        self._running = True
        backoff = 2.0
        while self._running:
            try:
                await self._connect_and_consume()
                backoff = 2.0
            except Exception as exc:
                log.warning("MQTT subscriber disconnected",
                            exc=str(exc), retry_in=backoff)
                self._connected = False
                await asyncio.sleep(backoff)
                backoff = min(60.0, backoff * 2)

    async def _connect_and_consume(self) -> None:
        if not _AIOMQTT_OK:
            log.error("aiomqtt not installed — cloud MQTT subscriber disabled")
            await asyncio.sleep(3600)
            return

        async with aiomqtt.Client(
            hostname=self._host,
            port=self._port,
            identifier="cloud-api",
            keepalive=30,
        ) as client:
            self._client    = client
            self._connected = True
            log.info("Cloud MQTT subscriber connected",
                     host=self._host, port=self._port)

            # Subscribe to all edge uplinks
            await client.subscribe("bess/+/telemetry", qos=0)
            await client.subscribe("bess/+/status",    qos=1)
            await client.subscribe("bess/+/fault",     qos=1)
            await client.subscribe("bess/+/lwt",       qos=1)
            await client.subscribe("bess/+/buffer_stats", qos=1)
            await client.subscribe("bess/+/schedule",  qos=1)

            try:
                async for message in client.messages:
                    if not self._running:
                        break
                    await self._dispatch(str(message.topic), bytes(message.payload))
            finally:
                self._client = None

    async def _dispatch(self, topic: str, payload: bytes) -> None:
        try:
            data = json.loads(payload.decode())
        except Exception:
            self._stats["errors"] += 1
            return

        self._stats["received"] += 1

        m = _TELEMETRY_RE.match(topic)
        if m:
            device_id = m.group(1)
            self._stats["devices"].add(device_id)
            await self._on_telemetry(device_id, data)
            return

        m = _STATUS_RE.match(topic)
        if m:
            await self._on_status(m.group(1), data)
            return

        m = _FAULT_RE.match(topic)
        if m:
            device_id = m.group(1)
            log.warning("Fault received from device",
                        device_id=device_id, code=data.get("fault_code"))
            await self._on_fault(device_id, data)
            return

        m = _LWT_RE.match(topic)
        if m:
            log.warning("LWT received — device offline", device_id=m.group(1))
            return

        m = _SCHEDULE_RE.match(topic)
        if m and self._on_schedule:
            await self._on_schedule(m.group(1), data)
            return

    async def publish(self, topic: str, payload: str, qos: int = 1) -> bool:
        """Publish a downlink (e.g. a fetched market price curve) to edge
        devices. Best-effort: returns False (and logs) if not currently
        connected, rather than raising — callers treat this as one signal
        among several, not a hard dependency."""
        if self._client is None:
            log.warning("Cannot publish — MQTT subscriber not connected", topic=topic)
            return False
        try:
            await self._client.publish(topic, payload=payload, qos=qos)
            return True
        except Exception as exc:
            log.error("MQTT publish failed", topic=topic, exc=str(exc))
            return False

    async def stop(self) -> None:
        self._running = False

    @property
    def is_connected(self) -> bool:
        return self._connected

    def stats(self) -> dict:
        return {
            **self._stats,
            "devices": list(self._stats["devices"]),
        }
