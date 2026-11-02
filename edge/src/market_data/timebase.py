"""
Single definition of how a DAM hour index maps onto wall-clock time.

The Ukrainian day-ahead market (РДН) trades 24 hourly delivery periods per
calendar day, and oree.com.ua publishes them — as every Ukrainian market
participant does — in *Kyiv local time*: column 1 is the 00:00–01:00
EET/EEST period. Everything downstream of price acquisition in this system,
by contrast, works in UTC: telemetry carries UTC timestamps, the optimizer
indexes its 24-hour horizon by UTC hour, and the edge picks the active
hourly action with `datetime.now(timezone.utc).hour`.

Those two conventions were silently treated as one. A locally-indexed price
curve was handed straight to the optimizer and written to InfluxDB as
`day.replace(hour=h, tzinfo=utc)`, which displaces the whole curve by
Kyiv's UTC offset — three hours while EEST is in force, two under EET. The
visible symptom was an arbitrage plan that discharged hours after the real
evening peak had passed, and that treated the midday solar-glut price floor
as though it fell in the late afternoon.

Converting at the acquisition boundary lets the rest of the system keep its
single UTC convention, which is what it already assumed it had.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

_KYIV = ZoneInfo("Europe/Kyiv")


def kyiv_utc_offset_hours(day: date) -> int:
    """Kyiv's UTC offset in whole hours on `day` (+3 under EEST, +2 EET)."""
    # Noon is deliberate: it never falls inside a DST transition, so the
    # offset it reports is the one applying to the bulk of that day.
    return int(
        datetime(day.year, day.month, day.day, 12, tzinfo=_KYIV)
        .utcoffset()
        .total_seconds() // 3600
    )


def local_hours_to_utc(prices: list[float], day: date) -> list[float]:
    """Re-index a 24-value hourly curve from Kyiv local hours to UTC hours.

    `prices[i]` arrives as the value for local hour `i`. The result is
    indexed so element `h` is the value for *UTC* hour `h`, since local
    hour `h + offset` is the same instant as UTC hour `h`.

    The final `offset` elements wrap back to the start of the same local
    day instead of reaching into the next one, which would need a second
    day's prices that are not necessarily published yet. Those wrapped
    slots are the hours from 21:00 UTC onward — the overnight valley, at
    both ends of the wrap — so the substitution stays inside the flattest
    part of the curve. That keeps the residual error bounded to `offset`
    hours out of 24, rather than skewing all 24.
    """
    if len(prices) != 24:
        raise ValueError(f"expected 24 hourly prices, got {len(prices)}")
    off = kyiv_utc_offset_hours(day)
    return [prices[(h + off) % 24] for h in range(24)]


def kyiv_local_hour(utc_now: datetime) -> float:
    """Fractional Kyiv local hour for a UTC instant, DST included.

    The emulator's irradiance and household-load curves are both written
    against local clock time — sunrise at 05:30, an evening consumption
    peak at 20:00 — so they need the real local hour, not a fixed offset.
    They previously used a hardcoded `+ 2.0`, which is right only while
    EET is in force and an hour short throughout EEST, i.e. for roughly
    seven months of the year.
    """
    if utc_now.tzinfo is None:
        # Replay and test paths hand in naive UTC; interpreting that as
        # system-local time would reintroduce exactly the class of skew
        # this module exists to remove.
        utc_now = utc_now.replace(tzinfo=timezone.utc)
    local = utc_now.astimezone(_KYIV)
    return local.hour + local.minute / 60.0 + local.second / 3600.0
