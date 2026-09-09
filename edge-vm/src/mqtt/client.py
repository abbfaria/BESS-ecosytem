"""
Resilient MQTT client for edge device.

Features:
  - mTLS (mutual TLS 1.3) via X.509 client certificates
  - paho-mqtt 2.x callback v2 API
  - Exponential backoff with jitter on disconnect
  - Last Will and Testament for offline detection
  - Seamless SQLite buffer replay on reconnect
  - Per-topic QoS configuration
"""

from __future__ import annotations

import asyncio
import json
import random
import ssl
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

try:
    import paho.mqtt.client as mqtt  # type: ignore
    _PAHO_OK = True
except ImportError:
    _PAHO_OK = False
    mqtt = None  # type: ignore

from .buffer import MQTTBuffer, BufferedMessage
from ..utils.logging_config import get_logger

log = get_logger(__name__)

# Reconnect backoff: 1 s → 2 → 4 → … → 60 s (with ±20% jitter)
_BACKOFF_MIN_S   = 1.0
_BACKOFF_MAX_S   = 60.0
_BACKOFF_FACTOR  = 2.0
_BACKOFF_JITTER  = 0.2

_KEEPALIVE_S     = 30
_CONNECT_TIMEOUT = 10


class MQTTClientConfig:
    host:         str  = "127.0.0.1"
    port:         int  = 8883
    device_id:    str  = "bess-edge-01"
    ca_cert:      str  = "/certs/ca.crt"
    client_cert:  str  = "/certs/edge.crt"
    client_key:   str  = "/certs/edge.key"
    lwt_topic:    str  = ""   # set by EdgeMQTTClient
    username:     str  = ""
    password:     str  = ""


class EdgeMQTTClient:
    """
    Async wrapper around paho-mqtt with buffer replay and TLS.

    Usage:
        async with EdgeMQTTClient(cfg, buffer) as client:
            await client.publish(topic, payload)
    """

    def __init__(
        self,
        cfg: MQTTClientConfig,
        buffer: MQTTBuffer,
        on_message: Optional[Callable[[str, bytes], None]] = None,
    ) -> None:
        self._cfg       = cfg
        self._buffer    = buffer
        self._on_message_cb = on_message
        self._connected = False
        self._backoff   = _BACKOFF_MIN_S
        self._client: Optional[object] = None  # paho client
        self._running   = False
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._subscriptions: list[tuple[str, int]] = []

    # ── Context manager ───────────────────────────────────────────────────────

    async def __aenter__(self) -> "EdgeMQTTClient":
        self._loop = asyncio.get_running_loop()
        await self._start()
        return self

    async def __aexit__(self, *_) -> None:
        await self.stop()

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def _start(self) -> None:
        if not _PAHO_OK:
            log.error("paho-mqtt not installed — MQTT disabled")
            return
        self._running = True
        self._build_client()
        asyncio.create_task(self._connect_loop(), name="mqtt-connect")

    async def stop(self) -> None:
        self._running = False
        if self._client and self._connected:
            self._client.disconnect()
        log.info("MQTT client stopped")

    def _build_client(self) -> None:
        cfg = self._cfg
        lwt_topic = cfg.lwt_topic or f"bess/{cfg.device_id}/lwt"
        lwt_payload = json.dumps({
            "ts":        datetime.now(timezone.utc).isoformat(),
            "device_id": cfg.device_id,
            "status":    "offline",
        })

        client = mqtt.Client(
            client_id=cfg.device_id,
            protocol=mqtt.MQTTv5,
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        )
        client.will_set(lwt_topic, lwt_payload, qos=1, retain=True)

        # mTLS setup
        if Path(cfg.ca_cert).exists():
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ctx.minimum_version = ssl.TLSVersion.TLSv1_3
            ctx.load_verify_locations(cfg.ca_cert)
            if Path(cfg.client_cert).exists() and Path(cfg.client_key).exists():
                ctx.load_cert_chain(cfg.client_cert, cfg.client_key)
            ctx.check_hostname = True
            ctx.verify_mode    = ssl.CERT_REQUIRED
            client.tls_set_context(ctx)
        else:
            log.warning("TLS certificates not found — connecting without TLS (DEV ONLY)")

        if cfg.username:
            client.username_pw_set(cfg.username, cfg.password)

        # Callbacks (paho v2 API: func(client, userdata, ...))
        client.on_connect    = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_message    = self._on_message
        client.on_publish    = self._on_publish

        self._client = client

    # ── Connection loop ───────────────────────────────────────────────────────

    async def _connect_loop(self) -> None:
        while self._running:
            try:
                log.info("Connecting to MQTT broker",
                         host=self._cfg.host, port=self._cfg.port)
                self._client.connect_async(
                    self._cfg.host,
                    self._cfg.port,
                    keepalive=_KEEPALIVE_S,
                )
                self._client.loop_start()
                # Wait for connection (paho runs network I/O in background thread)
                for _ in range(_CONNECT_TIMEOUT * 10):
                    if self._connected:
                        break
                    await asyncio.sleep(0.1)
                if not self._connected:
                    raise ConnectionError("Connection timeout")

                self._backoff = _BACKOFF_MIN_S   # reset on success
                # Re-subscribe to downlink topics
                for topic, qos in self._subscriptions:
                    self._client.subscribe(topic, qos)
                # Replay buffered messages
                await self._replay_buffer()

                # Stay until disconnect
                while self._running and self._connected:
                    await asyncio.sleep(1.0)

            except Exception as exc:
                log.warning("MQTT connection failed",
                            exc=str(exc), retry_in=round(self._backoff, 1))

            if not self._running:
                break
            await asyncio.sleep(self._backoff)
            jitter = self._backoff * _BACKOFF_JITTER
            self._backoff = min(
                _BACKOFF_MAX_S,
                self._backoff * _BACKOFF_FACTOR + random.uniform(-jitter, jitter),
            )

    # ── Publish API ───────────────────────────────────────────────────────────

    async def publish(
        self,
        topic: str,
        payload: bytes | str | dict,
        qos: int = 1,
        buffer_on_fail: bool = True,
    ) -> bool:
        """Publish a message. Buffers locally if broker unreachable."""
        if isinstance(payload, dict):
            payload = json.dumps(payload, default=str).encode()
        elif isinstance(payload, str):
            payload = payload.encode()

        if self._connected and self._client:
            try:
                info = self._client.publish(topic, payload, qos=qos)
                if info.rc == 0:
                    return True
            except Exception as exc:
                log.debug("Publish error", exc=str(exc))

        if buffer_on_fail and qos > 0:
            await self._buffer.enqueue(topic, payload, qos)
            log.debug("Message buffered", topic=topic)
        return False

    def subscribe(self, topic: str, qos: int = 1) -> None:
        """Subscribe to a downlink topic (re-applied on reconnect)."""
        self._subscriptions.append((topic, qos))
        if self._connected and self._client:
            self._client.subscribe(topic, qos)

    # ── Buffer replay ─────────────────────────────────────────────────────────

    async def _replay_buffer(self) -> None:
        count = await self._buffer.pending_count()
        if count == 0:
            return
        log.info("Replaying buffered messages", count=count)
        async for msg in self._buffer.iter_pending(batch_size=200):
            if not self._connected:
                break
            try:
                info = self._client.publish(msg.topic, msg.payload, qos=msg.qos)
                if info.rc == 0:
                    await self._buffer.mark_delivered(msg.id)
                else:
                    await self._buffer.increment_attempts(msg.id)
            except Exception:
                await self._buffer.increment_attempts(msg.id)
            await asyncio.sleep(0.01)   # yield between replays

    # ── Paho callbacks ────────────────────────────────────────────────────────

    def _on_connect(self, client, userdata, flags, reason_code, properties) -> None:
        if reason_code == 0:
            self._connected = True
            log.info("MQTT connected", broker=self._cfg.host)
        else:
            log.error("MQTT connect refused", reason=reason_code)

    def _on_disconnect(self, client, userdata, flags, reason_code, properties) -> None:
        self._connected = False
        log.warning("MQTT disconnected", reason=reason_code)

    def _on_message(self, client, userdata, message) -> None:
        if self._on_message_cb:
            try:
                self._on_message_cb(message.topic, message.payload)
            except Exception as exc:
                log.error("on_message callback error", exc=str(exc))

    def _on_publish(self, client, userdata, mid, reason_code, properties) -> None:
        pass   # QoS 1 PUBACK; buffer mark_delivered handled in replay

    @property
    def is_connected(self) -> bool:
        return self._connected
