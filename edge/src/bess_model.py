"""
Deterministic BESS model: DAM price curve, daily dispatch plan, and the
system state at any moment.

This is the single source of truth for both services. The edge publishes
state_at(now) as live telemetry; cloud-api uses the same functions to
backfill yesterday/today and project tomorrow (the file is copied into the
cloud image at build time). Because everything is a pure function of time,
live and backfilled data are identical and join without jumps.

All times are UTC; a "day" is a UTC calendar day.

Business rules enforced by plan_day():
  * SoC always stays within [SOC_MIN, SOC_MAX] = [30 %, 90 %].
  * BESS revenue = energy sold to the grid at the DAM price minus the cost
    of scheduled GRID_CHARGE. House load and PV are not part of it.
  * Grid charging is only allowed while the day's cumulative revenue stays
    above CASH_MIN, and every day opens with a small sale from the energy
    carried overnight, so the day's revenue is > 0 at every moment.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, datetime, timezone
from functools import lru_cache

CAPACITY_KWH = 20.0
POWER_KW     = 5.0
EFF          = 0.95     # one-way (charge or discharge) efficiency
SOC_MIN      = 30.0
SOC_MAX      = 90.0
CARRY_SOC    = 60.0     # kept overnight for the next morning's sales
OPENING_KW   = 1.0      # hour-0 sale that makes each day's revenue positive
CASH_MIN     = 1.0      # UAH; grid charging never pushes the day below this
PV_PEAK_KW   = 10.0

# Typical Ukrainian DAM profile, UAH/MWh, by UTC hour (Kyiv = UTC+2):
# morning peak, midday solar trough, evening peak.
_BASE_PRICES = [
    3800, 3500, 3300, 3400, 4500, 6200, 7000, 6800, 5200, 3200, 2600, 2400,
    2500, 3100, 4800, 7500, 9000, 9500, 9200, 7800, 6000, 5000, 4500, 4100,
]
_N_CHARGE    = 6   # cheapest hours of the day
_N_DISCHARGE = 6   # most expensive hours of the day

IDLE, CHARGE, DISCHARGE = "IDLE", "GRID_CHARGE", "DISCHARGE"


def prices(day: date) -> list[float]:
    """DAM prices for `day`, UAH/MWh per UTC hour (±10 % day-to-day)."""
    k = 1.0 + 0.1 * math.sin(day.toordinal() * 0.7)
    return [round(p * k, 2) for p in _BASE_PRICES]


@dataclass(frozen=True)
class Plan:
    prices: list[float]   # UAH/MWh
    kw:     list[float]   # battery DC power per hour: + charge, - discharge
    soc:    list[float]   # SoC at the start of each hour, 25 values
    cash:   list[float]   # cumulative BESS revenue at each hour start, 25 values

    def mode(self, hour: int) -> str:
        kw = self.kw[hour]
        return CHARGE if kw > 0 else DISCHARGE if kw < 0 else IDLE


def grid_kw(battery_kw: float) -> float:
    """AC power the battery draws from (+) or delivers to (-) the grid."""
    return battery_kw / EFF if battery_kw > 0 else battery_kw * EFF


def revenue_rate(battery_kw: float, price: float) -> float:
    """BESS revenue rate in UAH/h: sales positive, grid charging negative."""
    return -grid_kw(battery_kw) * price / 1000.0


def plan_day(day_prices: list[float], soc_start: float) -> Plan:
    order = sorted(range(24), key=lambda h: day_prices[h])
    charge_hours = set(order[:_N_CHARGE])
    discharge_hours = set(order[-_N_DISCHARGE:]) | {0}

    soc, cash = soc_start, 0.0
    kws, socs, cashes = [], [soc], [cash]
    for h in range(24):
        price = day_prices[h] / 1000.0          # UAH/kWh
        later_charge = [k for k in charge_hours if k > h]
        kw = 0.0
        if h in discharge_hours:
            # Drain to 30 % only if the battery can be refilled later today,
            # otherwise keep CARRY_SOC for tomorrow morning.
            floor = SOC_MIN if later_charge else CARRY_SOC
            avail = max(0.0, (soc - floor) / 100.0 * CAPACITY_KWH)
            # Leave energy for pricier discharge hours before the next refill.
            stop = min(later_charge, default=24)
            pricier = sum(1 for k in discharge_hours
                          if h < k < stop and day_prices[k] > day_prices[h])
            limit = OPENING_KW if h == 0 else POWER_KW
            kw = 0.0 - min(limit, max(0.0, avail - pricier * POWER_KW))
        elif h in charge_hours:
            room = (SOC_MAX - soc) / 100.0 * CAPACITY_KWH
            affordable = max(0.0, cash - CASH_MIN) / price * EFF
            kw = min(POWER_KW, room, affordable)
        soc += kw / CAPACITY_KWH * 100.0
        cash += revenue_rate(kw, day_prices[h])
        kws.append(kw)
        socs.append(soc)
        cashes.append(cash)
    return Plan(list(day_prices), kws, socs, cashes)


@lru_cache(maxsize=64)
def plan_for(day: date) -> Plan:
    """The plan for `day`. Every day starts at CARRY_SOC, and the evening
    discharge stops at CARRY_SOC, so consecutive days join seamlessly
    (checked by tests/test_bess_model.py)."""
    return plan_day(prices(day), CARRY_SOC)


def pv_kw(t: datetime) -> float:
    """Clear-sky PV output: bell curve around 10:00 UTC, seasonal length/peak."""
    season = math.cos(2 * math.pi * (t.timetuple().tm_yday - 172) / 365)
    half_day = 6.0 + 2.5 * season                 # 3.5 h (winter) .. 8.5 h (summer)
    x = (t.hour + t.minute / 60 + t.second / 3600 - 10.0) / half_day
    if abs(x) >= 1:
        return 0.0
    return PV_PEAK_KW * (0.55 + 0.45 * season) * math.cos(x * math.pi / 2) ** 2


def load_kw(t: datetime) -> float:
    """House load: base plus morning and evening bumps."""
    h = t.hour + t.minute / 60 + t.second / 3600
    return 0.4 + 0.6 * math.exp(-((h - 5.5) / 1.5) ** 2) + 1.2 * math.exp(-((h - 17) / 2) ** 2)


def state_at(t: datetime, device_id: str = "bess-edge-01") -> dict:
    """Telemetry snapshot at `t` (timezone-aware)."""
    t = t.astimezone(timezone.utc)
    plan = plan_for(t.date())
    h = t.hour
    frac = (t.minute * 60 + t.second + t.microsecond / 1e6) / 3600.0
    kw = plan.kw[h]
    soc = plan.soc[h] + kw * frac / CAPACITY_KWH * 100.0
    soc = min(SOC_MAX, max(SOC_MIN, soc))     # hard guard; the plan already respects it
    pv, load = pv_kw(t), load_kw(t)
    grid = load - pv + grid_kw(kw)
    return {
        "ts":                t.isoformat(),
        "device_id":         device_id,
        "mode":              plan.mode(h),
        "battery_soc_pct":   round(soc, 3),
        "battery_power_w":   round(kw * 1000, 1),
        "pv_power_w":        round(pv * 1000, 1),
        "house_load_w":      round(load * 1000, 1),
        "grid_power_w":      round(grid * 1000, 1),
        "battery_temp_c":    round(22.0 + 4.0 * abs(kw) / POWER_KW, 2),
        "grid_voltage_v":    round(230.0 - 0.8 * grid, 2),
        "price_uah_mwh":     plan.prices[h],
        "revenue_today_uah": round(plan.cash[h] + revenue_rate(kw, plan.prices[h]) * frac, 4),
    }
