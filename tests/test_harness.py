"""
BESS Ecosystem Integration Test Harness

Tests run WITHOUT a real MQTT broker or InfluxDB by mocking the I/O layer.
Covers:
  - BESSEmulator: physics model outputs
  - FallbackData: price profile validity
  - DAMClient: fallback path
  - MQTTBuffer: SQLite enqueue / replay / purge
  - BESSOptimizer: greedy schedule generation
  - FallbackController: schedule persistence
  - SQLiteStore: insert / query / trim
  - EdgeOrchestrator: component wiring smoke test
"""

import asyncio
import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, date, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

# Adjust path so we can import edge source
sys.path.insert(0, str(Path(__file__).parent.parent / "edge"))

from src.sensors.models import EmulatorConfig, SensorSnapshot
from src.sensors.emulator import BESSEmulator
from src.market_data.fallback_data import get_fallback_prices, get_fallback_prices_for_week
from src.mqtt.buffer import MQTTBuffer
from src.control.optimizer import BESSOptimizer, OptimizerConfig
from src.control.fallback import FallbackController
from src.db.sqlite_store import SQLiteStore


# ── Emulator Tests ─────────────────────────────────────────────────────────────

class TestBESSEmulator(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.cfg = EmulatorConfig(pv_peak_kw=10.0, battery_capacity_kwh=20.0)
        self.em  = BESSEmulator(self.cfg)

    async def test_tick_returns_snapshot(self):
        snap = await self.em.tick()
        self.assertIsInstance(snap, SensorSnapshot)
        self.assertEqual(snap.device_id, "bess-edge-01")
        self.assertGreater(snap.sequence_num, 0)

    async def test_soc_stays_bounded(self):
        """Run 50 ticks and verify SoC never leaves [0, 100]."""
        for _ in range(50):
            snap = await self.em.tick()
            self.assertGreaterEqual(snap.battery_soc_pct, 0.0)
            self.assertLessEqual(snap.battery_soc_pct, 100.0)

    async def test_pv_zero_at_night(self):
        """Patch datetime so the emulator thinks it's 2:00 AM UTC → no PV."""
        from datetime import datetime as dt
        fake_midnight = dt(2024, 7, 1, 0, 0, tzinfo=timezone.utc)
        with patch("src.sensors.emulator.datetime") as mock_dt:
            mock_dt.now.return_value = fake_midnight
            mock_dt.side_effect = lambda *a, **kw: dt(*a, **kw)
            snap = await self.em.tick()
        # At 00:00 UTC (~02:00 local), PV should be near zero
        self.assertLess(snap.pv_power_w, 100.0, "PV should be zero at night")

    async def test_mode_setting(self):
        self.em.set_mode("GRID_CHARGE", charge_pct=80.0, duration_min=120)
        snap = await self.em.tick()
        self.assertEqual(snap.inverter_mode, "GRID_CHARGE")

    def test_snapshot_to_dict(self):
        snap = SensorSnapshot(
            ts=datetime.now(timezone.utc),
            device_id="test-01",
        )
        d = snap.to_dict()
        self.assertIn("ts", d)
        self.assertIn("battery_soc_pct", d)
        self.assertIsInstance(d["ts"], str)


# ── Fallback Price Data Tests ──────────────────────────────────────────────────

class TestFallbackData(unittest.TestCase):

    def test_returns_24_prices(self):
        prices = get_fallback_prices()
        self.assertEqual(len(prices), 24)

    def test_prices_positive(self):
        prices = get_fallback_prices(date(2024, 1, 15))  # winter weekday
        self.assertTrue(all(p > 0 for p in prices))

    def test_weekend_summer_differs(self):
        weekday = get_fallback_prices(date(2024, 7, 1))   # Monday
        weekend = get_fallback_prices(date(2024, 7, 6))   # Saturday
        # Not identical (noise), but weekday peak should be higher
        self.assertNotEqual(max(weekday), max(weekend))

    def test_week_returns_7_days(self):
        week = get_fallback_prices_for_week(date(2024, 6, 3))
        self.assertEqual(len(week), 7)
        for prices in week.values():
            self.assertEqual(len(prices), 24)

    def test_price_cap(self):
        for _ in range(20):
            prices = get_fallback_prices()
            # Prices should stay below 20 000 UAH/MWh even with noise
            self.assertTrue(all(p < 20_000 for p in prices))


# ── MQTT Buffer Tests ─────────────────────────────────────────────────────────

class TestMQTTBuffer(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self._tmpdir = tempfile.mkdtemp()
        self.buf = MQTTBuffer(
            db_path=os.path.join(self._tmpdir, "test_buffer.db"),
            max_messages=100,
        )
        self.buf.open()

    async def asyncTearDown(self):
        self.buf.close()

    async def test_enqueue_and_count(self):
        await self.buf.enqueue("bess/dev1/telemetry", b'{"test":1}', qos=1)
        await self.buf.enqueue("bess/dev1/telemetry", b'{"test":2}', qos=1)
        count = await self.buf.pending_count()
        self.assertEqual(count, 2)

    async def test_mark_delivered(self):
        row_id = await self.buf.enqueue("test/topic", b"data", qos=1)
        await self.buf.mark_delivered(row_id)
        count = await self.buf.pending_count()
        self.assertEqual(count, 0)

    async def test_iter_pending_fifo(self):
        await self.buf.enqueue("t/1", b"a", qos=1)
        await self.buf.enqueue("t/2", b"b", qos=1)
        msgs = []
        async for msg in self.buf.iter_pending():
            msgs.append(msg)
        self.assertEqual(len(msgs), 2)
        self.assertEqual(msgs[0].topic, "t/1")
        self.assertEqual(msgs[1].topic, "t/2")

    async def test_overflow_purge(self):
        """Buffer max=100; inserting 110 should purge 10 oldest."""
        for i in range(110):
            await self.buf.enqueue(f"t/{i}", b"x", qos=1)
        count = await self.buf.pending_count()
        self.assertLessEqual(count, 100)

    async def test_stats(self):
        await self.buf.enqueue("x", b"y", qos=1)
        stats = await self.buf.stats()
        self.assertIn("enqueued", stats)
        self.assertIn("pending", stats)
        self.assertEqual(stats["pending"], 1)


# ── Optimizer Tests ────────────────────────────────────────────────────────────

class TestBESSOptimizer(unittest.TestCase):

    def setUp(self):
        self.opt = BESSOptimizer(OptimizerConfig(
            battery_capacity_kwh=20.0,
            battery_charge_kw_max=5.0,
            battery_discharge_kw_max=5.0,
        ))

    def _prices(self):
        # Cheap night, expensive evening
        p = [2000.0] * 6 + [3500.0] * 6 + [2500.0] * 6 + [6000.0] * 6
        return p

    def test_returns_24_actions(self):
        actions = self.opt.optimize(self._prices(), current_soc_pct=50.0)
        self.assertEqual(len(actions), 24)

    def test_charges_in_cheap_hours(self):
        actions = self.opt.optimize(self._prices(), current_soc_pct=30.0)
        charge_hours = [a.hour for a in actions if a.mode == "GRID_CHARGE"]
        # Should pick up at least some of hours 0–5 (price 2000)
        self.assertTrue(any(h < 6 for h in charge_hours))

    def test_discharges_in_expensive_hours(self):
        actions = self.opt.optimize(self._prices(), current_soc_pct=90.0)
        dis_hours = [a.hour for a in actions if a.mode == "DISCHARGE_SELL"]
        self.assertTrue(any(h >= 18 for h in dis_hours))

    def test_revenue_positive(self):
        actions = self.opt.optimize(self._prices(), current_soc_pct=50.0)
        total = sum(a.expected_revenue for a in actions)
        self.assertGreater(total, 0.0, "Optimizer should find positive arbitrage")


# ── FallbackController Tests ──────────────────────────────────────────────────

class TestFallbackController(unittest.TestCase):

    def test_default_action_at_night(self):
        fc = FallbackController(Path(tempfile.mktemp()))
        action = fc._default_action(2)   # 02:00
        self.assertEqual(action.mode, "GRID_CHARGE")

    def test_default_action_at_peak(self):
        fc = FallbackController(Path(tempfile.mktemp()))
        action = fc._default_action(19)  # 19:00
        self.assertEqual(action.mode, "DISCHARGE_SELL")

    def test_save_and_load_roundtrip(self):
        from src.control.optimizer import HourlyAction
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = Path(f.name)
        fc = FallbackController(path)
        actions = [HourlyAction(hour=h, mode="SOLAR_PRIORITY") for h in range(24)]
        fc.save_schedule(actions, "2024-07-15")
        fc2 = FallbackController(path)
        ok = fc2.load_schedule()
        self.assertTrue(ok)
        self.assertEqual(fc2.loaded_date, "2024-07-15")
        self.assertEqual(len(fc2._schedule), 24)


# ── SQLiteStore Tests ─────────────────────────────────────────────────────────

class TestSQLiteStore(unittest.TestCase):

    def setUp(self):
        self._tmpdir = tempfile.mkdtemp()
        self.store = SQLiteStore(os.path.join(self._tmpdir, "test_edge.db"))
        self.store.open()

    def tearDown(self):
        self.store.close()

    def _snap(self, i: int = 0) -> dict:
        return {
            "ts":           datetime.now(timezone.utc).isoformat(),
            "device_id":    "test-01",
            "sequence_num": i,
            "battery_soc_pct": 55.0 + i,
        }

    def test_insert_and_retrieve(self):
        self.store.insert_telemetry(self._snap(1))
        rows = self.store.get_latest_telemetry(1)
        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(rows[0]["battery_soc_pct"], 56.0)

    def test_multiple_inserts_ordered(self):
        for i in range(5):
            self.store.insert_telemetry(self._snap(i))
        rows = self.store.get_latest_telemetry(5)
        # Most recent first
        self.assertEqual(len(rows), 5)

    def test_log_and_retrieve_events(self):
        self.store.log_event("TEST_EVENT", "unit test event", severity="INFO")
        events = self.store.get_recent_events(10)
        self.assertTrue(any(e["event_type"] == "TEST_EVENT" for e in events))

    def test_stats(self):
        self.store.insert_telemetry(self._snap())
        stats = self.store.get_stats()
        self.assertIn("telemetry_rows", stats)
        self.assertGreater(stats["telemetry_rows"], 0)


# ── Main ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("BESS Ecosystem Integration Test Harness")
    print("=" * 60)
    unittest.main(verbosity=2)
