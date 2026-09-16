"""
Edge node main asyncio orchestrator.

Task graph:
  ┌─────────────────────────────────────────────┐
  │  sensor_loop   (every 10 s)                 │
  │    → emulate → store → MQTT publish         │
  │                                             │
  │  schedule_loop (every 1 h at :00)           │
  │    → apply current schedule slot            │
  │                                             │
  │  market_loop   (daily 14:00 UTC)            │
  │    → fetch DAM prices for tomorrow          │
  │    → run optimizer                          │
  │    → persist schedule                       │
  │                                             │
  │  status_loop   (every 60 s)                 │
  │    → publish device status heartbeat        │
  │                                             │
  │  api_server    (FastAPI, port 8000)         │
  └─────────────────────────────────────────────┘
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from .sensors.emulator import BESSEmulator
from .sensors.models import EmulatorConfig
from .market_data.dam_client import DAMClient
from .mqtt.client import EdgeMQTTClient, MQTTClientConfig
from .mqtt.buffer import MQTTBuffer
from .control.optimizer import BESSOptimizer, OptimizerConfig
from .control.fallback import FallbackController
from .db.sqlite_store import SQLiteStore
from .utils.logging_config import get_logger

log = get_logger(__name__)

TELEMETRY_INTERVAL_S = int(os.environ.get("TELEMETRY_INTERVAL_S", "10"))
STATUS_INTERVAL_S    = int(os.environ.get("STATUS_INTERVAL_S",    "60"))
DEVICE_ID            = os.environ.get("DEVICE_ID", "bess-edge-01")
MQTT_HOST            = os.environ.get("MQTT_HOST", "cloud.bess.local")
MQTT_PORT            = int(os.environ.get("MQTT_PORT", "8883"))
ENTSOE_TOKEN         = os.environ.get("ENTSOE_TOKEN", "")
DATA_DIR             = Path(os.environ.get("DATA_DIR", "/data"))

# MQTT topic helpers (inline to avoid circular import)
def _topic(kind: str) -> str:
    return f"bess/{DEVICE_ID}/{kind}"


class EdgeOrchestrator:
    def __init__(self) -> None:
        # Components
        self._emulator   = BESSEmulator(EmulatorConfig(device_id=DEVICE_ID))
        self._store      = SQLiteStore(DATA_DIR / "edge.db")
        self._dam        = DAMClient(entsoe_token=ENTSOE_TOKEN, store=self._store)
        self._optimizer  = BESSOptimizer(OptimizerConfig())
        self._fallback   = FallbackController(DATA_DIR / "last_schedule.json")
        self._buffer     = MQTTBuffer(DATA_DIR / "mqtt_buffer.db")

        mqtt_cfg         = MQTTClientConfig()
        mqtt_cfg.host    = MQTT_HOST
        mqtt_cfg.port    = MQTT_PORT
        mqtt_cfg.device_id = DEVICE_ID
        mqtt_cfg.ca_cert   = os.environ.get("CA_CERT",      "/certs/ca.crt")
        mqtt_cfg.client_cert = os.environ.get("CLIENT_CERT","/certs/edge.crt")
        mqtt_cfg.client_key  = os.environ.get("CLIENT_KEY", "/certs/edge.key")
        self._mqtt_cfg   = mqtt_cfg

        self._mqtt: Optional[EdgeMQTTClient] = None
        self._start_time = time.monotonic()
        self._running    = False
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._prices_today: Optional[list[float]] = None

    async def run(self) -> None:
        self._running = True
        # paho-mqtt's on_message callback (and therefore _apply_market_push)
        # runs on paho's own network thread (client.loop_start()), not this
        # event loop's thread — capture the loop here so that thread can
        # safely hand work back via run_coroutine_threadsafe.
        self._loop = asyncio.get_running_loop()
        self._store.open()
        self._buffer.open()

        # Load any persisted schedule
        self._fallback.load_schedule()

        async with EdgeMQTTClient(self._mqtt_cfg, self._buffer,
                                  on_message=self._handle_downlink) as mqtt:
            self._mqtt = mqtt

            # Subscribe to cloud downlinks
            mqtt.subscribe(_topic("schedule"), qos=1)
            mqtt.subscribe(_topic("cmd"),      qos=1)
            mqtt.subscribe(_topic("market"),   qos=1)

            await asyncio.gather(
                self._sensor_loop(),
                self._schedule_loop(),
                self._market_loop(),
                self._status_loop(),
                self._api_server(),
            )

        self._store.close()
        self._buffer.close()

    # ── Sensor loop ───────────────────────────────────────────────────────────

    async def _sensor_loop(self) -> None:
        log.info("Sensor loop started", interval_s=TELEMETRY_INTERVAL_S)
        while self._running:
            try:
                snapshot = await self._emulator.tick()

                # Realized revenue for this interval: the DAM price actually
                # scheduled for the current hour, applied to the energy
                # actually exchanged with the grid (not the battery — PV-fed
                # charging isn't a grid cost). + grid_power_w = import (cost),
                # − grid_power_w = export (revenue).
                price = self._fallback.get_current_action().price_uah_mwh
                energy_kwh = snapshot.grid_power_w * (TELEMETRY_INTERVAL_S / 3600.0) / 1000.0
                snapshot.revenue_uah = round(-energy_kwh * price / 1000.0, 4)

                snap_dict = snapshot.to_dict()

                # Persist locally
                self._store.insert_telemetry(snap_dict)

                # Publish (buffered if disconnected)
                payload = json.dumps(snap_dict, default=str)
                await self._mqtt.publish(_topic("telemetry"), payload, qos=0)

                # Fault alert
                if snapshot.fault_code != 0:
                    fault_payload = json.dumps({
                        "ts": snap_dict["ts"],
                        "device_id": DEVICE_ID,
                        "fault_code": snapshot.fault_code,
                        "severity": "WARNING",
                        "message": f"Fault bitmask: {snapshot.fault_code:#010b}",
                        "component": "bess",
                    })
                    await self._mqtt.publish(_topic("fault"), fault_payload, qos=1)

            except Exception as exc:
                log.error("Sensor loop error", exc=str(exc))

            await asyncio.sleep(TELEMETRY_INTERVAL_S)

    # ── Schedule application loop ─────────────────────────────────────────────

    async def _schedule_loop(self) -> None:
        log.info("Schedule loop started")
        while self._running:
            try:
                action = self._fallback.get_current_action()
                self._emulator.set_mode(
                    action.mode,
                    charge_pct=action.charge_pct,
                    discharge_pct=action.discharge_pct,
                    duration_min=61,   # hold until next check
                )
                log.debug("Schedule applied",
                          hour=datetime.now(timezone.utc).hour,
                          mode=action.mode)
            except Exception as exc:
                log.error("Schedule loop error", exc=str(exc))

            # Sleep until start of next hour
            now      = datetime.now(timezone.utc)
            next_h   = (now + timedelta(hours=1)).replace(
                minute=0, second=0, microsecond=0
            )
            sleep_s  = (next_h - now).total_seconds()
            await asyncio.sleep(max(60.0, sleep_s))

    # ── Market data loop ───────────────────────────────────────────────────────

    async def _market_loop(self) -> None:
        log.info("Market loop started (runs daily at 14:00 UTC)")
        while self._running:
            try:
                now = datetime.now(timezone.utc)
                # Ukrainian DAM publishes D+1 prices around 13:00–14:00 UTC
                target_hour = 14
                next_run = now.replace(
                    hour=target_hour, minute=5, second=0, microsecond=0
                )
                if now >= next_run:
                    next_run += timedelta(days=1)
                sleep_s = (next_run - now).total_seconds()
                log.info("Market loop sleeping", next_run=next_run.isoformat(),
                         sleep_h=round(sleep_s / 3600, 1))
                await asyncio.sleep(sleep_s)

                tomorrow = (datetime.now(timezone.utc) + timedelta(days=1)).date()
                prices   = await self._dam.get_prices(for_date=tomorrow)
                self._prices_today = prices
                await self._run_optimizer_and_publish(prices, tomorrow.isoformat())

            except Exception as exc:
                log.error("Market loop error", exc=str(exc))
                await asyncio.sleep(300)

    async def _run_optimizer_and_publish(
        self, prices: list[float], valid_date: str
    ) -> None:
        """Compute a schedule from `prices` and apply/publish it immediately.

        Shared by the daily timer (_market_loop) and by _apply_market_push,
        so that real prices act on the schedule as soon as they actually
        arrive — not only when the fixed 14:00 UTC window happens to land
        while everything is connected. A container restart (which resets
        _market_loop's timer) or a missed window no longer strands the
        device on price_uah_mwh=0.0 fallback behaviour for a whole day;
        any later real price push self-heals it immediately.
        """
        latest = self._store.get_latest_telemetry(1)
        soc = latest[0].get("battery_soc_pct", 50.0) if latest else 50.0

        actions = self._optimizer.optimize(prices, soc)
        self._fallback.save_schedule(actions, valid_date)

        schedule_payload = {
            "schedule_id": f"{DEVICE_ID}-{valid_date}",
            "generated_ts": datetime.now(timezone.utc).isoformat(),
            "valid_date": valid_date,
            "device_id": DEVICE_ID,
            "slots": [
                {
                    "hour":             a.hour,
                    "mode":             a.mode,
                    "charge_power_pct": a.charge_pct,
                    "discharge_power_pct": a.discharge_pct,
                    "price_uah_mwh":    a.price_uah_mwh,
                    "expected_revenue_uah": a.expected_revenue,
                }
                for a in actions
            ],
            "total_expected_revenue_uah": round(
                sum(a.expected_revenue for a in actions), 2
            ),
            "optimizer_version": "milp-v1" if True else "greedy-v1",
        }
        await self._mqtt.publish(
            _topic("schedule"),
            json.dumps(schedule_payload, default=str),
            qos=1,
        )
        self._store.log_event(
            "SCHEDULE_GENERATED",
            f"24h schedule for {valid_date} published",
            data={"revenue_uah": schedule_payload["total_expected_revenue_uah"]},
        )

    # ── Status loop ────────────────────────────────────────────────────────────

    async def _status_loop(self) -> None:
        log.info("Status loop started", interval_s=STATUS_INTERVAL_S)
        while self._running:
            try:
                uptime = int(time.monotonic() - self._start_time)
                pending = await self._buffer.pending_count()
                status = {
                    "ts":         datetime.now(timezone.utc).isoformat(),
                    "device_id":  DEVICE_ID,
                    "health":     "HEALTHY" if pending < 1000 else "WARNING",
                    "uptime_s":   uptime,
                    "fw_version": "1.0.0",
                    "mqtt_buffer_count": pending,
                    "cloud_connected":   self._mqtt.is_connected if self._mqtt else False,
                }
                await self._mqtt.publish(_topic("status"),
                                         json.dumps(status), qos=1)
            except Exception as exc:
                log.error("Status loop error", exc=str(exc))
            await asyncio.sleep(STATUS_INTERVAL_S)

    # ── Downlink handler ───────────────────────────────────────────────────────

    def _handle_downlink(self, topic: str, payload: bytes) -> None:
        try:
            data = json.loads(payload.decode())
        except Exception:
            log.warning("Invalid downlink payload", topic=topic)
            return

        if topic.endswith("/schedule"):
            self._apply_remote_schedule(data)
        elif topic.endswith("/cmd"):
            self._apply_command(data)
        elif topic.endswith("/market"):
            self._apply_market_push(data)

    def _apply_remote_schedule(self, data: dict) -> None:
        from .control.optimizer import HourlyAction
        slots = data.get("slots", [])
        actions = [
            HourlyAction(
                hour=s["hour"],
                mode=s.get("mode", "SOLAR_PRIORITY"),
                charge_pct=s.get("charge_power_pct", 0.0),
                discharge_pct=s.get("discharge_power_pct", 0.0),
                price_uah_mwh=s.get("price_uah_mwh", 0.0),
                expected_revenue=s.get("expected_revenue_uah", 0.0),
            )
            for s in slots
        ]
        valid_date = data.get("valid_date", "")
        if len(actions) == 24:
            self._fallback.save_schedule(actions, valid_date)
            log.info("Remote schedule applied", date=valid_date)

    def _apply_command(self, data: dict) -> None:
        mode     = data.get("mode", "SOLAR_PRIORITY")
        pwr_pct  = data.get("power_pct", 0.0)
        dur_min  = data.get("duration_min", 60)
        if mode in ("GRID_CHARGE",):
            self._emulator.set_mode(mode, charge_pct=pwr_pct, duration_min=dur_min)
        elif mode in ("DISCHARGE_SELL",):
            self._emulator.set_mode(mode, discharge_pct=pwr_pct, duration_min=dur_min)
        else:
            self._emulator.set_mode(mode, duration_min=dur_min)
        log.info("Command applied", mode=mode, pwr_pct=pwr_pct, dur_min=dur_min)

    def _apply_market_push(self, data: dict) -> None:
        prices = [p["price_uah_mwh"] for p in data.get("prices", [])]
        if len(prices) == 24:
            self._prices_today = prices
            valid_date = data.get("valid_date") or (
                datetime.now(timezone.utc) + timedelta(days=1)
            ).date().isoformat()
            # Real prices fetched by the cloud's headless-browser scraper
            # (see cloud/api/src/market_fetcher.py) — cache for the
            # DAMClient's history-based fallback.
            try:
                self._store.save_dam_prices(valid_date, prices, "cloud")
            except Exception as exc:
                log.warning("Failed to cache cloud-pushed prices", exc=str(exc))
            log.info("Market prices received from cloud", max=max(prices))
            # Act on real data the moment it arrives, rather than waiting
            # for _market_loop's own fixed daily window — that window
            # resets on every container restart and is otherwise skipped
            # for the rest of the day if missed, which is exactly what
            # repeated network outages have been causing in practice.
            #
            # This callback runs on paho-mqtt's own network thread
            # (client.loop_start()), not the asyncio event loop thread, so
            # scheduling the coroutine must go through
            # run_coroutine_threadsafe rather than create_task.
            if self._loop is not None:
                asyncio.run_coroutine_threadsafe(
                    self._run_optimizer_and_publish(prices, valid_date),
                    self._loop,
                )

    # ── FastAPI local API ─────────────────────────────────────────────────────

    async def _api_server(self) -> None:
        try:
            from .api.app import create_app
            import uvicorn  # type: ignore
            app = create_app(self)
            config = uvicorn.Config(
                app, host="0.0.0.0", port=8000, log_level="warning"
            )
            server = uvicorn.Server(config)
            await server.serve()
        except ImportError as exc:
            log.warning("FastAPI/uvicorn not available — local API disabled",
                        exc=str(exc))
            while self._running:
                await asyncio.sleep(60)

    def get_store(self) -> SQLiteStore:
        return self._store

    def get_buffer(self) -> MQTTBuffer:
        return self._buffer

    def get_emulator(self) -> BESSEmulator:
        return self._emulator


def main() -> None:
    orchestrator = EdgeOrchestrator()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def _shutdown(sig, frame):
        log.info("Shutdown signal received", signal=sig)
        orchestrator._running = False
        loop.stop()

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    try:
        loop.run_until_complete(orchestrator.run())
    finally:
        loop.close()
        log.info("Edge node stopped")


if __name__ == "__main__":
    main()
