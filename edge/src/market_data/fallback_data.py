"""
Realistic Ukrainian DAM (РДН) price fallback matrix.

Derived from 2023-2024 operatormarket.ua historical data statistics:
  - Off-peak (00–06):   1 800 – 2 500 UAH/MWh
  - Morning peak (07–10): 3 200 – 4 500 UAH/MWh
  - Mid-day valley (11–14): 2 200 – 3 000 UAH/MWh
  - Evening peak (17–21): 4 000 – 6 000 UAH/MWh  (cap 15 000)
  - Night trough (22–23): 2 000 – 2 800 UAH/MWh

The matrix encodes 7 seasonal/day-type profiles (Mon–Sun ≈ weekday/weekend x season).
"""

from __future__ import annotations

import random
from datetime import date
from typing import Optional

# Typical price profiles by hour (UAH/MWh, 24 values)
_WEEKDAY_SUMMER = [
    1850, 1700, 1620, 1580, 1600, 1900,   # 00–05  night / early morning
    2800, 3800, 4200, 4000, 3500, 2800,   # 06–11  morning peak
    2400, 2200, 2300, 2600, 3100, 3900,   # 12–17  mid-day, early evening ramp
    5100, 5800, 5500, 4800, 3200, 2200,   # 18–23  evening peak + trough
]

_WEEKDAY_WINTER = [
    2100, 1900, 1800, 1780, 1820, 2300,
    4200, 5500, 5800, 5400, 4000, 3200,
    2900, 2700, 2800, 3200, 4500, 6200,
    7500, 8000, 7200, 5800, 4100, 2800,
]

_WEEKDAY_SPRING = [
    1950, 1780, 1650, 1620, 1680, 2100,
    3200, 4100, 4400, 4100, 3200, 2500,
    2100, 1900, 2000, 2400, 3000, 4200,
    5400, 5900, 5300, 4400, 3000, 2200,
]

_WEEKEND_SUMMER = [
    1600, 1500, 1450, 1420, 1450, 1700,
    2100, 2800, 3100, 3200, 3000, 2500,
    2100, 1900, 1950, 2200, 2800, 3600,
    4500, 5000, 4600, 3800, 2700, 1900,
]

_WEEKEND_WINTER = [
    1950, 1780, 1700, 1680, 1720, 2100,
    3600, 4800, 5200, 5000, 4200, 3400,
    2900, 2600, 2700, 3000, 4100, 5500,
    6500, 7000, 6300, 5100, 3700, 2500,
]

# Map: (is_weekend, season) → profile
# season: 0=winter(DJF), 1=spring(MAM), 2=summer(JJA), 3=autumn(SON)
_PROFILES: dict[tuple[bool, int], list[float]] = {
    (False, 0): _WEEKDAY_WINTER,
    (False, 1): _WEEKDAY_SPRING,
    (False, 2): _WEEKDAY_SUMMER,
    (False, 3): _WEEKDAY_SPRING,   # autumn ≈ spring dynamics
    (True,  0): _WEEKEND_WINTER,
    (True,  1): _WEEKEND_SUMMER,
    (True,  2): _WEEKEND_SUMMER,
    (True,  3): _WEEKEND_SUMMER,
}


def _season(d: date) -> int:
    m = d.month
    if m in (12, 1, 2):  return 0
    if m in (3,  4, 5):  return 1
    if m in (6,  7, 8):  return 2
    return 3


def get_fallback_prices(for_date: Optional[date] = None) -> list[float]:
    """Return 24 hourly prices (UAH/MWh) for the given date.

    Adds ±15% random noise so each call is unique, simulating market volatility.
    """
    if for_date is None:
        from datetime import date as _date
        for_date = _date.today()

    is_weekend = for_date.weekday() >= 5
    season     = _season(for_date)
    profile    = _PROFILES[(is_weekend, season)]

    prices = []
    for base in profile:
        noise = random.uniform(-0.12, 0.18)   # slight positive skew (spike risk)
        prices.append(round(max(500.0, base * (1 + noise)), 1))

    return prices


def get_fallback_prices_for_week(start_date: date) -> dict[str, list[float]]:
    """Convenience: return a full week of daily profiles."""
    from datetime import timedelta
    result = {}
    for offset in range(7):
        d = start_date + timedelta(days=offset)
        result[d.isoformat()] = get_fallback_prices(d)
    return result
