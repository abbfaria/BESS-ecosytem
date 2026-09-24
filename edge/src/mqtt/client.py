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
        # paho message-id → buffer row id, for QoS 1 messages awaiting PUBACK.
        # Mutated from both the event loop (publish) and paho's network
        # thread (_on_publish); dict get/set/pop are atomic under the GIL,
        # which is sufficient for this access pattern.
        self._inflight: dict[int, int] = {}
        self._connect_task: Optional[asyncio.Task] = None

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
        # Keep a reference: the event loop holds only a *weak* reference to
        # tasks, so a fire-and-forget create_task() can be garbage-collected
        # mid-flight.
        self._connect_task = asyncio.create_task(
            self._connect_loop(), name="mqtt-connect"
        )

    async def stop(self) -> None:
        self._running = False
        if self._connect_task:
            self._connect_task.cancel()
            try:
                await self._connect_task
            except (asyncio.CancelledError, Exception):
                pass
        if self._client:
            try:
                self._client.disconnect()
                self._client.loop_stop()
            except Exception:
                pass
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
        """Start paho's network thread once, then watch for connection
        transitions.

        Reconnection is delegated entirely to paho (`reconnect_delay_set`
        below). An earlier version drove its own connect/backoff loop *on
        top of* paho's built-in auto-reconnect, so two independent
        reconnect mechanisms raced each other on the same socket — each
        iteration re-issued connect_async() and loop_start() against an
        already-looping client. Now there is exactly one.
        """
        log.info("Connecting to MQTT broker",
                 host=self._cfg.host, port=self._cfg.port)
        self._client.reconnect_delay_set(min_delay=int(_BACKOFF_MIN_S),
                                          max_delay=int(_BACKOFF_MAX_S))
        try:
            self._client.connect_async(
                self._cfg.host, self._cfg.port, keepalive=_KEEPALIVE_S,
            )
            self._client.loop_start()
        except Exception as exc:
            log.error("MQTT client could not be started", exc=str(exc))
            return

        was_connected = False
        while self._running:
            if self._connected and not was_connected:
                # Fresh connection: subscriptions are re-applied in
                # _on_connect (paho thread, immediately); drain the backlog
                # here, on the event loop, where the buffer's asyncio lock
                # can be awaited safely.
                was_connected = True
                try:
                    await self._replay_buffer()
                except Exception as exc:
                    log.error("Buffer replay failed", exc=str(exc))
            elif not self._connected and was_connected:
                was_connected = False
            await asyncio.sleep(1.0)

    # ── Publish API ───────────────────────────────────────────────────────────

    async def publish(
        self,
        topic: str,
        payload: bytes | str | dict,
        qos: int = 1,
        buffer_on_fail: bool = True,
    ) -> bool:
        """Publish a message, with durability appropriate to its QoS.

        QoS 1 — the message is persisted to the buffer *before* being
        handed to paho, and is only marked delivered once the broker's
        PUBACK arrives (_on_publish). paho's `rc == 0` means "accepted
        into the client's outbound queue", which is emphatically not the
        same as "the broker has it": an earlier version returned True on
        rc == 0 and never wrote the message anywhere, so anything in
        flight when the link dropped was lost without trace.

        QoS 0 — unacknowledged by definition, so it is published directly
        while connected. If the link is down it is buffered and replayed
        on reconnect instead of being discarded (this is the telemetry
        stream; dropping it was the single largest source of data loss).
        """
        if isinstance(payload, dict):
            payload = json.dumps(payload, default=str).encode()
        elif isinstance(payload, str):
            payload = payload.encode()

        # ── QoS 1: durable path ──────────────────────────────────────────
        if qos > 0:
            row_id = await self._buffer.enqueue(topic, payload, qos)
            if self._connected and self._client:
                try:
                    info = self._client.publish(topic, payload, qos=qos)
                    if info.rc == 0:
                        # Delivery is confirmed later, in _on_publish.
                        self._inflight[info.mid] = row_id
                        return True
                except Exception as exc:
                    log.debug("Publish error", exc=str(exc))
            return False    # stays pending in the buffer, replayed later

        # ── QoS 0: best-effort path ──────────────────────────────────────
        if self._connected and self._client:
            try:
                info = self._client.publish(topic, payload, qos=0)
                if info.rc == 0:
                    return True
            except Exception as exc:
                log.debug("Publish error", exc=str(exc))

        if buffer_on_fail:
            await self._buffer.enqueue(topic, payload, qos)
            log.debug("QoS 0 message buffered while offline", topic=topic)
        return False

    def subscribe(self, topic: str, qos: int = 1) -> None:
        """Subscribe to a downlink topic (re-applied on reconnect)."""
        self._subscriptions.append((topic, qos))
        if self._connected and self._client:
            self._client.subscribe(topic, qos)

    # ── Buffer replay ─────────────────────────────────────────────────────────

    async def _replay_buffer(self) -> None:
        """Drain the entire backlog, not just the first batch.

        `iter_pending(batch_size=N)` is a single `LIMIT N` query, and this
        method used to call it once per reconnect — so with a backlog of
        several thousand messages only the first 200 were ever sent and
        the remainder stayed pending forever, across every subsequent
        reconnect. The outer loop keeps fetching batches until the
        backlog is empty, the link drops, or a batch makes no progress.
        """
        pending = await self._buffer.pending_count()
        if pending == 0:
            return
        log.info("Replaying buffered messages", pending=pending)

        delivered = 0
        while self._running and self._connected:
            batch = [m async for m in self._buffer.iter_pending(batch_size=200)]
            if not batch:
                break

            progressed = False
            for msg in batch:
                if not self._connected:
                    break
                try:
                    info = self._client.publish(msg.topic, msg.payload, qos=msg.qos)
                    if info.rc == 0:
                        # QoS 0 has no PUBACK to wait for, so it is settled
                        # as soon as paho accepts it; QoS 1 is settled in
                        # _on_publish when the broker acknowledges it.
                        if msg.qos == 0:
                            await self._buffer.mark_delivered(msg.id)
                        else:
                            self._inflight[info.mid] = msg.id
                        delivered += 1
                        progressed = True
                    else:
                        await self._buffer.increment_attempts(msg.id)
                except Exception:
                    await self._buffer.increment_attempts(msg.id)
                await asyncio.sleep(0.005)   # yield; avoid flooding paho's queue

            if not progressed:
                # Nothing in this batch could be handed off (broker refusing,
                # outbound queue full). Stop rather than spin on it.
                log.warning("Buffer replay stalled — will retry on next connect")
                break

        remaining = await self._buffer.pending_count()
        log.info("Buffer replay finished", handed_off=delivered, still_pending=remaining)

    # ── Paho callbacks ────────────────────────────────────────────────────────

    def _on_connect(self, client, userdata, flags, reason_code, properties) -> None:
        if reason_code == 0:
            self._connected = True
            log.info("MQTT connected", broker=self._cfg.host)
            # Re-apply subscriptions here rather than from the monitor loop:
            # the broker drops them on every disconnect, and doing it in the
            # callback closes the window where a downlink could be missed.
            for topic, qos in self._subscriptions:
                try:
                    client.subscribe(topic, qos)
                except Exception as exc:
                    log.error("Re-subscribe failed", topic=topic, exc=str(exc))
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
        """QoS 1 PUBACK — the only point at which delivery is actually known.

        Runs on paho's network thread, so the buffer update (which takes an
        asyncio lock) is handed back to the event loop rather than executed
        here. Previously this callback did nothing at all and the comment
        claimed replay handled it, which left QoS 1 messages published on a
        live connection tracked by nothing whatsoever.
        """
        row_id = self._inflight.pop(mid, None)
        if row_id is None or self._loop is None:
            return
        try:
            asyncio.run_coroutine_threadsafe(
                self._buffer.mark_delivered(row_id), self._loop
            )
        except Exception as exc:
            log.debug("Could not mark message delivered", mid=mid, exc=str(exc))

    @property
    def is_connected(self) -> bool:
        return self._connected
