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
from datetime import datetime, date, timedelta, timezone
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
from src.control.economics import compute_revenue_uah
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

    async def test_force_mode_sets_mode_without_override_timer(self):
        """force_mode() is the historical-replay entry point — it must not
        depend on time.monotonic()-based override expiry like set_mode()."""
        self.em.force_mode("DISCHARGE_SELL", discharge_pct=60.0)
        snap = await self.em.tick(as_of=datetime(2026, 6, 1, 12, tzinfo=timezone.utc),
                                   dt_s=300.0)
        self.assertEqual(snap.inverter_mode, "DISCHARGE_SELL")

    async def test_tick_as_of_is_deterministic_regardless_of_wall_clock(self):
        """Two ticks for the *same* as_of/dt_s should describe the same
        simulated instant (noon, full PV) regardless of when the test
        actually runs — this is what makes historical backfill possible."""
        noon = datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc)
        self.em.force_mode("SOLAR_PRIORITY")
        snap = await self.em.tick(as_of=noon, dt_s=300.0)
        self.assertGreater(snap.pv_power_w, 0.0, "should be full daylight at noon")
        self.assertEqual(snap.ts, noon)

    async def test_tick_as_of_replays_historical_dates(self):
        """Stepping as_of backwards in time must not raise or misbehave —
        the backfill drives the emulator through many past days in a
        single long-lived instance."""
        past = datetime(2026, 1, 1, 6, tzinfo=timezone.utc)
        self.em.force_mode("SOLAR_PRIORITY")
        snap = await self.em.tick(as_of=past, dt_s=300.0)
        self.assertEqual(snap.ts, past)

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


# ── Economics Tests ────────────────────────────────────────────────────────────

class TestEconomics(unittest.TestCase):
    """The single revenue formula shared by the live sensor loop
    (EdgeOrchestrator._sensor_loop) and the cloud history backfill
    (cloud/api/src/history.py) — must stay identical between the two."""

    def test_import_costs_money(self):
        # +grid_power_w = import from the grid = a cost = negative revenue
        rev = compute_revenue_uah(grid_power_w=1000.0, interval_s=3600, price_uah_mwh=5000.0)
        self.assertLess(rev, 0.0)
        self.assertAlmostEqual(rev, -5.0, places=2)   # 1 kWh @ 5000 UAH/MWh

    def test_export_earns_money(self):
        # -grid_power_w = export to the grid = revenue
        rev = compute_revenue_uah(grid_power_w=-2000.0, interval_s=1800, price_uah_mwh=4000.0)
        self.assertGreater(rev, 0.0)
        self.assertAlmostEqual(rev, 4.0, places=2)   # 1 kWh @ 4000 UAH/MWh

    def test_no_grid_flow_is_zero_revenue(self):
        self.assertEqual(compute_revenue_uah(0.0, 300, 5000.0), 0.0)


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
        self.assertEqual(len(fc2._schedules["2024-07-15"]), 24)

    def test_load_schedule_accepts_legacy_single_schedule_format(self):
        """A device upgrading from a pre-fix image has last_schedule.json
        in the old `{"valid_date":..., "slots":[...]}` shape on disk —
        must still load, not be silently discarded."""
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = Path(f.name)
        path.write_text(json.dumps({
            "valid_date": "2024-07-15",
            "slots": [{"hour": h, "mode": "SOLAR_PRIORITY",
                       "charge_pct": 0.0, "discharge_pct": 0.0, "price_uah_mwh": 0.0}
                      for h in range(24)],
        }))
        fc = FallbackController(path)
        self.assertTrue(fc.load_schedule())
        self.assertEqual(fc.loaded_date, "2024-07-15")

    def test_tomorrows_schedule_does_not_evict_todays(self):
        """The actual bug this design fixes: the cloud routinely pushes
        tomorrow's real-price schedule during the afternoon, well before
        today is over (see _apply_market_push in main.py). Saving it must
        not make today's still-active schedule disappear."""
        from src.control.optimizer import HourlyAction
        from datetime import datetime, timezone, timedelta

        fc = FallbackController(Path(tempfile.mktemp()))
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        tomorrow = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%d")

        today_actions = [HourlyAction(hour=h, mode="DISCHARGE_SELL", price_uah_mwh=12000.0)
                          for h in range(24)]
        fc.save_schedule(today_actions, today)

        tomorrow_actions = [HourlyAction(hour=h, mode="SOLAR_PRIORITY", price_uah_mwh=1000.0)
                             for h in range(24)]
        fc.save_schedule(tomorrow_actions, tomorrow)

        action = fc.get_current_action()
        self.assertEqual(action.mode, "DISCHARGE_SELL",
                          "today's schedule must still govern the current hour "
                          "after tomorrow's schedule arrives early")
        self.assertEqual(action.price_uah_mwh, 12000.0)


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

    def test_save_and_read_dam_prices(self):
        prices = [1000.0 + h * 10 for h in range(24)]
        self.store.save_dam_prices("2026-09-16", prices, "oree")
        curves = self.store.get_real_price_history(max_days=14)
        self.assertEqual(len(curves), 1)
        self.assertEqual(curves[0], prices)

    def test_dam_price_upsert_overwrites(self):
        self.store.save_dam_prices("2026-09-16", [1.0] * 24, "oree")
        self.store.save_dam_prices("2026-09-16", [2.0] * 24, "entsoe")
        curves = self.store.get_real_price_history(max_days=14)
        self.assertEqual(len(curves), 1)
        self.assertEqual(curves[0], [2.0] * 24)

    def test_static_source_excluded_from_history(self):
        self.store.save_dam_prices("2026-09-16", [1.0] * 24, "static")
        curves = self.store.get_real_price_history(max_days=14)
        self.assertEqual(curves, [])


class TestDAMClientHistoryFallback(unittest.IsolatedAsyncioTestCase):
    """The fallback chain should prefer real cached history over the
    hardcoded static matrix once any real data has been observed."""

    def setUp(self):
        self._tmpdir = tempfile.mkdtemp()
        self.store = SQLiteStore(os.path.join(self._tmpdir, "test_edge.db"))
        self.store.open()

    def tearDown(self):
        self.store.close()

    async def test_falls_back_to_real_history_not_static(self):
        from src.market_data.dam_client import DAMClient

        self.store.save_dam_prices("2026-09-10", [2000.0] * 24, "oree")
        self.store.save_dam_prices("2026-09-11", [4000.0] * 24, "cloud")

        client = DAMClient(store=self.store)
        with patch.object(DAMClient, "_fetch_oree", new=AsyncMock(return_value=None)), \
             patch.object(DAMClient, "_fetch_entsoe", new=AsyncMock(return_value=None)):
            prices = await client.get_prices(date(2026, 9, 17))

        self.assertEqual(prices, [3000.0] * 24)

    async def test_cold_start_uses_static_fallback(self):
        from src.market_data.dam_client import DAMClient

        client = DAMClient(store=self.store)
        with patch.object(DAMClient, "_fetch_oree", new=AsyncMock(return_value=None)), \
             patch.object(DAMClient, "_fetch_entsoe", new=AsyncMock(return_value=None)):
            prices = await client.get_prices(date(2026, 9, 17))

        self.assertEqual(len(prices), 24)
        self.assertTrue(all(p > 0 for p in prices))


# ── Delivery-durability regression tests ──────────────────────────────────────
#
# Each test here pins down a defect that was found by auditing the delivery
# path against live buffer contents. They are written as regressions: every
# one of them fails against the previous implementation.

class _FakePublishInfo:
    def __init__(self, rc=0, mid=1):
        self.rc = rc
        self.mid = mid


class _FakePahoClient:
    """Minimal stand-in for paho's client: records publishes, never sends."""
    def __init__(self, rc=0):
        self.published = []
        self._rc = rc
        self._mid = 0

    def publish(self, topic, payload, qos=0):
        self._mid += 1
        self.published.append((topic, payload, qos))
        return _FakePublishInfo(rc=self._rc, mid=self._mid)

    def subscribe(self, *a, **kw):
        pass


class TestBufferDurability(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self._tmpdir = tempfile.mkdtemp()
        self.buf = MQTTBuffer(db_path=Path(self._tmpdir) / "b.db", max_messages=100)
        self.buf.open()

    async def test_qos0_is_buffered_when_offline(self):
        """Telemetry is QoS 0. The old client only buffered `qos > 0`, so an
        outage silently discarded the entire telemetry stream — the buffer
        on the live device contained zero telemetry rows."""
        from src.mqtt.client import EdgeMQTTClient, MQTTClientConfig
        client = EdgeMQTTClient(MQTTClientConfig(), self.buf)
        client._connected = False           # simulate outage

        ok = await client.publish("bess/x/telemetry", {"pv": 1}, qos=0)

        self.assertFalse(ok)
        self.assertEqual(await self.buf.pending_count(), 1,
                          "QoS 0 message must be retained while offline")

    async def test_qos1_is_persisted_before_send(self):
        """QoS 1 must be written to the buffer *before* being handed to the
        broker, so an in-flight message survives a crash. The old code
        returned True on paho rc==0 without storing anything."""
        from src.mqtt.client import EdgeMQTTClient, MQTTClientConfig
        client = EdgeMQTTClient(MQTTClientConfig(), self.buf)
        client._connected = True
        client._client = _FakePahoClient()

        ok = await client.publish("bess/x/status", {"a": 1}, qos=1)

        self.assertTrue(ok)
        # Still pending: only a PUBACK (_on_publish) may settle it.
        self.assertEqual(await self.buf.pending_count(), 1)
        self.assertEqual(len(client._inflight), 1)

    async def test_puback_settles_the_buffered_message(self):
        from src.mqtt.client import EdgeMQTTClient, MQTTClientConfig
        client = EdgeMQTTClient(MQTTClientConfig(), self.buf)
        client._connected = True
        client._client = _FakePahoClient()
        client._loop = asyncio.get_running_loop()

        await client.publish("bess/x/status", {"a": 1}, qos=1)
        mid = next(iter(client._inflight))
        client._on_publish(None, None, mid, 0, None)
        await asyncio.sleep(0.05)           # let the threadsafe call land

        self.assertEqual(await self.buf.pending_count(), 0)

    async def test_replay_drains_more_than_one_batch(self):
        """iter_pending() is a single LIMIT query; replaying it once left
        everything beyond the first batch pending forever. Observed live:
        exactly 200 delivered, 2923 stranded."""
        from src.mqtt.client import EdgeMQTTClient, MQTTClientConfig
        big = MQTTBuffer(db_path=Path(self._tmpdir) / "big.db", max_messages=10_000)
        big.open()
        for i in range(450):                # > 2 batches of 200
            await big.enqueue(f"bess/x/telemetry", f"m{i}".encode(), qos=0)

        client = EdgeMQTTClient(MQTTClientConfig(), big)
        client._connected = True
        client._running = True
        client._client = _FakePahoClient()

        await client._replay_buffer()

        self.assertEqual(await big.pending_count(), 0,
                          "replay must drain the whole backlog, not one batch")

    async def test_overflow_drops_qos0_before_qos1(self):
        """A long outage floods the buffer with telemetry. Eviction must not
        discard the acknowledged-delivery traffic it is meant to protect."""
        small = MQTTBuffer(db_path=Path(self._tmpdir) / "s.db", max_messages=10)
        small.open()
        for i in range(5):
            await small.enqueue("bess/x/fault", f"f{i}".encode(), qos=1)
        for i in range(40):
            await small.enqueue("bess/x/telemetry", f"t{i}".encode(), qos=0)

        rows = [m async for m in small.iter_pending(batch_size=1000)]
        self.assertEqual(sum(1 for m in rows if m.qos == 1), 5,
                          "QoS 1 messages must survive a QoS 0 flood")


class TestFaultBitmask(unittest.IsolatedAsyncioTestCase):

    async def test_thermal_fault_clears_when_cool(self):
        """Bits were set with |= but never cleared, so one transient event
        latched an alarm until the process restarted — which then published
        a QoS 1 fault every 10 s forever."""
        from src.sensors.emulator import FAULT_THERMAL
        em = BESSEmulator(EmulatorConfig())

        em._batt_temp_c = 50.0
        em._set_fault(FAULT_THERMAL, em._batt_temp_c > 45.0)
        self.assertTrue(em._fault_code & FAULT_THERMAL)

        em._batt_temp_c = 30.0
        em._set_fault(FAULT_THERMAL, False)
        self.assertFalse(em._fault_code & FAULT_THERMAL)

    async def test_setting_one_bit_preserves_others(self):
        """Low-SoC/blackout used plain `=`, wiping unrelated bits."""
        from src.sensors.emulator import FAULT_THERMAL, FAULT_GRID_OUTAGE
        em = BESSEmulator(EmulatorConfig())
        em._set_fault(FAULT_GRID_OUTAGE, True)
        em._set_fault(FAULT_THERMAL, True)
        self.assertTrue(em._fault_code & FAULT_GRID_OUTAGE)
        self.assertTrue(em._fault_code & FAULT_THERMAL)


class TestOptimizerTerminalSoC(unittest.TestCase):

    def test_schedule_leaves_a_terminal_reserve(self):
        """With no terminal constraint the optimum is to sell the pack down
        to soc_min every night, starving the next morning. A 15-day run
        ended every single day at ~10% SoC."""
        cfg = OptimizerConfig(terminal_soc_pct=50.0)
        opt = BESSOptimizer(cfg)
        prices = [1000.0] * 12 + [14000.0] * 12      # strong incentive to dump
        actions = opt.optimize(prices, current_soc_pct=80.0)

        cap = cfg.battery_capacity_kwh
        soc = 80.0 / 100.0 * cap
        for a in actions:
            soc += (a.charge_pct / 100.0) * cfg.battery_charge_kw_max * cfg.charge_efficiency
            soc -= (a.discharge_pct / 100.0) * cfg.battery_discharge_kw_max / cfg.discharge_efficiency
        end_pct = soc / cap * 100.0

        if opt.last_method == "milp-v1":
            self.assertGreater(end_pct, 35.0,
                                f"terminal reserve not honoured (ended {end_pct:.1f}%)")

    def test_last_method_reports_real_provenance(self):
        """optimizer_version was hardcoded `"milp-v1" if True else ...`, so
        greedy-produced schedules were labelled MILP — which is precisely
        what concealed the PuLP incompatibility."""
        opt = BESSOptimizer(OptimizerConfig())
        opt.optimize([5000.0] * 24, 50.0)
        self.assertIn(opt.last_method, ("milp-v1", "greedy-v1"))
        self.assertNotEqual(opt.last_method, "none")


# ── Main ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("BESS Ecosystem Integration Test Harness")
    print("=" * 60)
    unittest.main(verbosity=2)
