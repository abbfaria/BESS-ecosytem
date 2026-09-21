"""
Startup history backfill + near-term forecast.

Why this exists: the system previously only had visible data for whatever
stretch of real wall-clock time the edge/cloud VMs happened to both be
powered on and connected — which, on a laptop that sleeps, meant Grafana
panels (especially the 30-day rolling payback-period panel) were full of
holes depending purely on when you happened to look. That's backwards for
a demo: past behavior is a matter of history and a short lookahead is a
matter of forecasting, neither of which needs the VMs to have been running
in real time.

`ensure_history()` runs once at cloud-api startup (see main.py's
`lifespan`) and is idempotent/gap-filling, not a wipe-and-regenerate: for
each of the last `BACKFILL_DAYS` days, it checks whether InfluxDB already
has telemetry for that day and only fills what's missing. It reuses the
*real* edge dispatch code — `BESSOptimizer` (the real MILP/greedy
optimizer) and `BESSEmulator` (the real physics model, via its
`as_of`/`dt_s`/`force_mode` replay hooks) — copied into this image as
`edge_src` (see cloud/api/Dockerfile) so backfilled data is produced by
the exact same logic as live operation, not a second implementation that
can quietly drift from it.

Price data preference, per day: (1) whatever's already cached in InfluxDB,
(2) a real fetch from oree.com.ua via market_fetcher.fetch_dam_prices
(works for any date within the site's currently-displayed month), (3) the
same illustrative fallback curve (`fallback_data.get_fallback_prices`)
DAMClient already uses for genuine cold start on the edge — never a new,
undocumented fabrication. Every written point keeps a `source` tag so
real vs. illustrative stays traceable.

Actual vs. forecast stays clearly separated by measurement, exactly as the
dashboard already expects: backfilled *past* days get `telemetry` points
(actual, as if the device had really been running). Today/tomorrow get a
`schedule` entry only (`expected_revenue_uah` — a projection, the same
field the "Очікуваний дохід завтра" panel already reads) — never written
to `telemetry`, since nothing has actually happened yet for those hours.
"""

from __future__ import annotations

import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

# `/app` (parent of both this package, `src`, and the copied-in `edge_src`)
# is normally already on sys.path via uvicorn's CWD-based import of
# `src.main:app` — inserted explicitly too so this module also imports
# cleanly if ever run standalone (e.g. a test harness with a different CWD).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from edge_src.control.optimizer import BESSOptimizer, OptimizerConfig       # noqa: E402
from edge_src.control.economics import compute_revenue_uah                  # noqa: E402
from edge_src.sensors.emulator import BESSEmulator                          # noqa: E402
from edge_src.sensors.models import EmulatorConfig                          # noqa: E402
from edge_src.market_data.fallback_data import get_fallback_prices          # noqa: E402

from .influx_writer import InfluxWriter
from .market_fetcher import fetch_dam_prices
from .logging_config import get_logger

log = get_logger(__name__)

BACKFILL_DAYS   = 35   # margin over the payback panel's 30-day window
TICK_MINUTES    = 5    # telemetry resolution for backfilled days
TICK_SECONDS    = TICK_MINUTES * 60


async def ensure_history(writer: InfluxWriter, bucket: str, device_id: str,
                          today: Optional[date] = None) -> None:
    if not writer.is_connected:
        log.warning("Skipping history backfill — InfluxDB not connected")
        return

    today = today or datetime.now(timezone.utc).date()
    optimizer = BESSOptimizer(OptimizerConfig())
    emulator  = BESSEmulator(EmulatorConfig(device_id=device_id))
    soc       = emulator.cfg.initial_soc_pct

    start = today - timedelta(days=BACKFILL_DAYS)
    day   = start
    filled, skipped = 0, 0
    while day < today:
        if await _has_telemetry(writer, bucket, device_id, day):
            # Still need to know the day's ending SoC to keep the *next*
            # day's optimizer input realistic — cheap to read back.
            soc = await _last_known_soc(writer, bucket, device_id, day, soc)
            skipped += 1
            day += timedelta(days=1)
            continue

        prices, source = await _prices_for(writer, bucket, device_id, day)
        schedule = optimizer.optimize(prices, soc)
        soc = await _replay_day(writer, emulator, device_id, day, schedule)

        await writer.write_market_price(device_id, day.isoformat(), prices, source)
        await writer.write_schedule(device_id, {
            "valid_date": day.isoformat(),
            "slots": [
                {"hour": a.hour, "mode": a.mode,
                 "price_uah_mwh": a.price_uah_mwh,
                 "expected_revenue_uah": a.expected_revenue}
                for a in schedule
            ],
        })
        filled += 1
        day += timedelta(days=1)

    log.info("History backfill complete", days_filled=filled, days_already_present=skipped,
             window_start=start.isoformat(), window_end=(today - timedelta(days=1)).isoformat())

    await _ensure_forecast(writer, bucket, optimizer, device_id, today, soc)


async def _replay_day(writer: InfluxWriter, emulator: "BESSEmulator", device_id: str,
                       day: date, schedule: list) -> float:
    """Step the real BESSEmulator through one historical day at
    TICK_MINUTES resolution, writing telemetry as it goes. Returns the
    SoC at end of day, to carry into the next day's optimizer call."""
    day_start = datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)

    for minute_of_day in range(0, 24 * 60, TICK_MINUTES):
        hour = minute_of_day // 60
        ts = day_start + timedelta(minutes=minute_of_day)
        action = schedule[hour]

        emulator.force_mode(action.mode, action.charge_pct, action.discharge_pct)
        snapshot = await emulator.tick(as_of=ts, dt_s=float(TICK_SECONDS))
        snapshot.revenue_uah = compute_revenue_uah(
            snapshot.grid_power_w, TICK_SECONDS, action.price_uah_mwh
        )
        last_soc = snapshot.battery_soc_pct
        await writer.write_telemetry(device_id, snapshot.to_dict())

    return last_soc


async def _prices_for(writer: InfluxWriter, bucket: str, device_id: str,
                       day: date) -> tuple[list[float], str]:
    cached = await _cached_market_prices(writer, bucket, device_id, day)
    if cached:
        return cached, "oree"

    fetched = await fetch_dam_prices(day)
    if fetched:
        return fetched, "oree"

    return get_fallback_prices(day), "fallback"


async def _ensure_forecast(writer: InfluxWriter, bucket: str, optimizer: "BESSOptimizer",
                            device_id: str, today: date, soc: float) -> None:
    """Guarantee today's and tomorrow's `schedule` (forecast) exist, so
    "Дохід сьогодні" has a real price to price actual telemetry against as
    soon as it arrives, and "Очікуваний дохід завтра" is never empty on a
    fresh boot. Writes schedule only — never telemetry, these are
    projections, not measurements."""
    for offset, label in ((0, "today"), (1, "tomorrow")):
        d = today + timedelta(days=offset)
        if await _has_schedule(writer, bucket, device_id, d):
            continue
        prices, source = await _prices_for(writer, bucket, device_id, d)
        schedule = optimizer.optimize(prices, soc)
        await writer.write_market_price(device_id, d.isoformat(), prices, source)
        await writer.write_schedule(device_id, {
            "valid_date": d.isoformat(),
            "slots": [
                {"hour": a.hour, "mode": a.mode,
                 "price_uah_mwh": a.price_uah_mwh,
                 "expected_revenue_uah": a.expected_revenue}
                for a in schedule
            ],
        })
        log.info("Forecast schedule ensured", which=label, date=d.isoformat(), source=source)


# ── InfluxDB gap-detection helpers ──────────────────────────────────────────

async def _has_telemetry(writer: InfluxWriter, bucket: str, device_id: str, day: date) -> bool:
    flux = f'''
from(bucket:"{bucket}")
  |> range(start: {day.isoformat()}T00:00:00Z, stop: {(day + timedelta(days=1)).isoformat()}T00:00:00Z)
  |> filter(fn: (r) => r._measurement == "telemetry" and r._field == "revenue_uah" and r.device_id == "{device_id}")
  |> limit(n: 1)
'''
    rows = await writer.query(flux)
    return len(rows) > 0


async def _has_schedule(writer: InfluxWriter, bucket: str, device_id: str, day: date) -> bool:
    flux = f'''
from(bucket:"{bucket}")
  |> range(start: {day.isoformat()}T00:00:00Z, stop: {(day + timedelta(days=1)).isoformat()}T00:00:00Z)
  |> filter(fn: (r) => r._measurement == "schedule" and r._field == "expected_revenue_uah" and r.device_id == "{device_id}")
  |> limit(n: 1)
'''
    rows = await writer.query(flux)
    return len(rows) > 0


async def _cached_market_prices(writer: InfluxWriter, bucket: str, device_id: str,
                                 day: date) -> Optional[list[float]]:
    flux = f'''
from(bucket:"{bucket}")
  |> range(start: {day.isoformat()}T00:00:00Z, stop: {(day + timedelta(days=1)).isoformat()}T00:00:00Z)
  |> filter(fn: (r) => r._measurement == "market_price" and r._field == "price_uah_mwh"
            and r.device_id == "{device_id}" and r.source == "oree")
  |> sort(columns: ["_time"])
'''
    rows = await writer.query(flux)
    if len(rows) != 24:
        return None
    return [float(r["_value"]) for r in rows]


async def _last_known_soc(writer: InfluxWriter, bucket: str, device_id: str,
                           day: date, default: float) -> float:
    flux = f'''
from(bucket:"{bucket}")
  |> range(start: {day.isoformat()}T00:00:00Z, stop: {(day + timedelta(days=1)).isoformat()}T00:00:00Z)
  |> filter(fn: (r) => r._measurement == "telemetry" and r._field == "battery_soc_pct" and r.device_id == "{device_id}")
  |> sort(columns: ["_time"], desc: true)
  |> limit(n: 1)
'''
    rows = await writer.query(flux)
    if not rows:
        return default
    return float(rows[0]["_value"])
