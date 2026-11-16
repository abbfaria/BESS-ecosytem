"""Business invariants of the shared BESS model (stdlib only)."""

import sys
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "edge"))

from src.bess_model import SOC_MAX, SOC_MIN, plan_for, state_at  # noqa: E402

DAYS = [date(2026, 1, 1) + timedelta(days=i) for i in range(730)]


class PlanInvariants(unittest.TestCase):
    def test_soc_stays_within_30_90(self):
        for d in DAYS:
            p = plan_for(d)
            self.assertGreaterEqual(min(p.soc), SOC_MIN - 1e-9, d)
            self.assertLessEqual(max(p.soc), SOC_MAX + 1e-9, d)

    def test_revenue_positive_at_every_hour(self):
        for d in DAYS:
            p = plan_for(d)
            self.assertGreater(min(p.cash[1:]), 0.0, d)

    def test_day_opens_with_a_sale(self):
        for d in DAYS:
            self.assertGreater(plan_for(d).soc[0], SOC_MIN, d)
            self.assertLess(plan_for(d).kw[0], 0.0, d)

    def test_soc_continuous_across_midnight(self):
        for d in DAYS[:-1]:
            self.assertAlmostEqual(plan_for(d).soc[-1], plan_for(d + timedelta(days=1)).soc[0])


class Telemetry(unittest.TestCase):
    def test_samples_over_two_days(self):
        t = datetime(2026, 11, 15, tzinfo=timezone.utc)
        end = t + timedelta(days=2)
        while t < end:
            s = state_at(t + timedelta(seconds=10))
            self.assertTrue(SOC_MIN <= s["battery_soc_pct"] <= SOC_MAX, s)
            self.assertGreater(s["revenue_today_uah"], 0.0, s)
            self.assertIn(s["mode"], ("IDLE", "GRID_CHARGE", "DISCHARGE"))
            t += timedelta(minutes=5)

    def test_end_of_day_revenue_matches_plan(self):
        d = date(2026, 11, 15)
        last = state_at(datetime(2026, 11, 15, 23, 59, 59, tzinfo=timezone.utc))
        self.assertAlmostEqual(last["revenue_today_uah"], plan_for(d).cash[-1], places=1)


if __name__ == "__main__":
    unittest.main()
