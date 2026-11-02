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

from datetime import date, datetime, timedelta, timezone
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

    On the two DST-change days the local day has 25 (October) or 23
    (March) delivery periods, and the curve must carry all of them: each
    UTC hour is then matched to the local period that actually covers it,
    counted from local midnight, so the repeated or skipped hour is
    accounted for instead of shifting every later hour by one.
    """
    n = local_day_hours(day)
    if len(prices) != n:
        raise ValueError(f"expected {n} hourly prices for {day}, got {len(prices)}")
    local_midnight = datetime(day.year, day.month, day.day, tzinfo=_KYIV).astimezone(timezone.utc)
    out = []
    for h in range(24):
        instant = datetime(day.year, day.month, day.day, h, tzinfo=timezone.utc)
        k = int((instant - local_midnight).total_seconds() // 3600)
        out.append(prices[k % n])
    return out


def local_day_hours(day: date) -> int:
    """Number of delivery hours in Kyiv's local day: 24, or 23/25 on DST days."""
    # Converted to UTC first: subtracting two datetimes that share a tzinfo
    # is wall-clock arithmetic in Python and would always give 24 h.
    start = datetime(day.year, day.month, day.day, tzinfo=_KYIV).astimezone(timezone.utc)
    nxt = day + timedelta(days=1)
    end = datetime(nxt.year, nxt.month, nxt.day, tzinfo=_KYIV).astimezone(timezone.utc)
    return int((end - start).total_seconds() // 3600)


def local_period_wall_hours(day: date) -> list[int]:
    """Local clock hour (0-23) at the start of each delivery period of `day`.

    [0, 1, ..., 23] on a normal day; on the October DST day hour 3 appears
    twice and on the March one it is missing. Lets a curve written per wall
    clock hour be laid out over the real 23/24/25 periods.
    """
    start = datetime(day.year, day.month, day.day, tzinfo=_KYIV).astimezone(timezone.utc)
    return [(start + timedelta(hours=k)).astimezone(_KYIV).hour
            for k in range(local_day_hours(day))]


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
