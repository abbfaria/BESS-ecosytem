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
(actual, as if the device had really been running). The future — out to
`FORECAST_DAYS` (~4 months) — gets a `schedule` entry (`expected_revenue_uah`,
the same field the "Очікуваний дохід завтра" panel reads) plus, separately,
a `telemetry_forecast` measurement carrying the *same physical fields* as
`telemetry` (PV power, battery power, SoC, temperature, ...) so the
time-series panels (PV power, power balance, temperature, grid voltage)
also have something to draw when the operator scrolls the dashboard's time
range forward, instead of running into a wall at "now". This is still
never written to `telemetry` itself — the panels union the two
measurements at query time, splitting exactly at `now()`, so what's real
and what's projected stays structurally distinguishable in InfluxDB even
though the chart draws them as one continuous line. The forecast horizon
necessarily uses the same illustrative fallback price curve as a genuine
cold start (Section on price sourcing above) — no source claims to know
real DAM auction results months ahead, because none exist yet.
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

BACKFILL_DAYS   = 35    # margin over the payback panel's 30-day window
MIN_COVERAGE_POINTS = 288   # a day needs at least this many telemetry points
                             # (== a full synthetic backfill day at
                             # TICK_MINUTES resolution) to count as
                             # "covered" — fewer than that means a real but
                             # partial live day, which still leaves a
                             # visible gap on the dashboard and gets topped
                             # up rather than skipped (see _has_telemetry).
FORECAST_DAYS   = 120   # ~4 months — how far forward panels stay populated
TICK_MINUTES    = 5     # telemetry resolution for backfilled (past) days
TICK_SECONDS    = TICK_MINUTES * 60
FORECAST_TICK_MINUTES = 15   # coarser resolution for the forward projection
                              # — months of history don't need 5-min fidelity,
                              # and it cuts the forecast horizon's write volume
                              # by 3x on every idempotent startup check.
FORECAST_TICK_SECONDS = FORECAST_TICK_MINUTES * 60


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

    await _ensure_forward(writer, bucket, optimizer, emulator, device_id, today, soc)


async def _replay_day(writer: InfluxWriter, emulator: "BESSEmulator", device_id: str,
                       day: date, schedule: list, forecast: bool = False) -> float:
    """Step the real BESSEmulator through one day, writing telemetry as it
    goes. `forecast=False` (past days): TICK_MINUTES resolution, written to
    `telemetry` — this is the backfill's "as if the device had really been
    running" path. `forecast=True` (future days): coarser
    FORECAST_TICK_MINUTES resolution, written to `telemetry_forecast` — a
    clearly separate measurement, since nothing has actually happened yet
    for a day that hasn't occurred. Returns the SoC at end of day, to carry
    into the next day's optimizer call."""
    day_start = datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)
    step_minutes = FORECAST_TICK_MINUTES if forecast else TICK_MINUTES
    step_seconds = FORECAST_TICK_SECONDS if forecast else TICK_SECONDS
    write = writer.write_telemetry_forecast if forecast else writer.write_telemetry

    for minute_of_day in range(0, 24 * 60, step_minutes):
        hour = minute_of_day // 60
        ts = day_start + timedelta(minutes=minute_of_day)
        action = schedule[hour]

        emulator.force_mode(action.mode, action.charge_pct, action.discharge_pct)
        snapshot = await emulator.tick(as_of=ts, dt_s=float(step_seconds))
        snapshot.revenue_uah = compute_revenue_uah(
            snapshot.grid_power_w, step_seconds, action.price_uah_mwh
        )
        last_soc = snapshot.battery_soc_pct
        await write(device_id, snapshot.to_dict())

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


async def _ensure_forward(writer: InfluxWriter, bucket: str, optimizer: "BESSOptimizer",
                           emulator: "BESSEmulator", device_id: str, today: date,
                           soc: float) -> None:
    """Guarantee `schedule` + `telemetry_forecast` exist out to
    `FORECAST_DAYS` ahead, so every dashboard panel — not just the
    single-value economic ones — stays populated no matter how far forward
    the operator scrolls the time range. "Дохід сьогодні" needs today's
    price the moment real telemetry starts arriving; "Очікуваний дохід
    завтра" needs tomorrow's; the PV/balance/temperature/voltage panels
    need the whole horizon, because their Flux queries union `telemetry`
    (real, up to now) with `telemetry_forecast` (projected, from now on) —
    see the dashboard JSON and the module docstring.

    Continues the *same* long-lived emulator instance used for the
    historical backfill, so SoC carries through the today boundary
    exactly as it would through any other day."""
    filled, skipped = 0, 0
    for offset in range(FORECAST_DAYS):
        d = today + timedelta(days=offset)
        has_schedule = await _has_schedule(writer, bucket, device_id, d)
        has_telemetry_fc = await _has_telemetry_forecast(writer, bucket, device_id, d)

        if has_schedule and has_telemetry_fc:
            soc = await _last_known_soc_forecast(writer, bucket, device_id, d, soc)
            skipped += 1
            continue

        prices, source = await _prices_for(writer, bucket, device_id, d)
        schedule = optimizer.optimize(prices, soc)

        if not has_schedule:
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

        if not has_telemetry_fc:
            soc = await _replay_day(writer, emulator, device_id, d, schedule,
                                     forecast=True)
        filled += 1

    log.info("Forward projection ensured", days_filled=filled, days_already_present=skipped,
             horizon_days=FORECAST_DAYS, window_end=(today + timedelta(days=FORECAST_DAYS - 1)).isoformat())


# ── InfluxDB gap-detection helpers ──────────────────────────────────────────

async def _has_telemetry(writer: InfluxWriter, bucket: str, device_id: str, day: date) -> bool:
    """A day counts as "covered" only once it has at least
    MIN_COVERAGE_POINTS telemetry points — not merely "at least one".
    A day where the edge was only briefly connected (a handful of live
    points, then a multi-hour gap until the next reconnect) used to pass
    the old any-point check and get skipped, leaving the empty stretch
    visible on the dashboard's time-series panels even though the day
    "had data" by the coarse day-level check. Backfilling such a day is
    safe: the synthetic points land on a fixed 5-minute grid, while real
    live telemetry is written on its own ~10s cadence, so the two
    essentially never collide/overwrite — the result is the real points
    plus synthetic ones filling what would otherwise be gaps, not real
    data replaced by synthetic."""
    # drop() before count() is required, not cosmetic: telemetry carries
    # grid_status/inverter_mode tags that vary within a day (SOLAR_PRIORITY
    # vs DISCHARGE_SELL, CONNECTED vs UNSTABLE, ...). Without dropping them
    # first, count() groups per unique tag combination and returns one row
    # per combination instead of a single daily total — the exact
    # tag-splitting failure mode already documented for the Grafana panels
    # themselves (see the dashboard's own history of this bug). Reading
    # only rows[0] in that case undercounts, wrongly concludes a fully
    # covered day is sparse, and re-backfills on top of it every restart.
    flux = f'''
from(bucket:"{bucket}")
  |> range(start: {day.isoformat()}T00:00:00Z, stop: {(day + timedelta(days=1)).isoformat()}T00:00:00Z)
  |> filter(fn: (r) => r._measurement == "telemetry" and r._field == "revenue_uah" and r.device_id == "{device_id}")
  |> drop(columns: ["grid_status", "inverter_mode", "device_id"])
  |> count()
'''
    rows = await writer.query(flux)
    count = int(rows[0]["_value"]) if rows else 0
    return count >= MIN_COVERAGE_POINTS


async def _has_telemetry_forecast(writer: InfluxWriter, bucket: str, device_id: str,
                                   day: date) -> bool:
    flux = f'''
from(bucket:"{bucket}")
  |> range(start: {day.isoformat()}T00:00:00Z, stop: {(day + timedelta(days=1)).isoformat()}T00:00:00Z)
  |> filter(fn: (r) => r._measurement == "telemetry_forecast" and r._field == "revenue_uah" and r.device_id == "{device_id}")
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


async def _last_known_soc_forecast(writer: InfluxWriter, bucket: str, device_id: str,
                                    day: date, default: float) -> float:
    flux = f'''
from(bucket:"{bucket}")
  |> range(start: {day.isoformat()}T00:00:00Z, stop: {(day + timedelta(days=1)).isoformat()}T00:00:00Z)
  |> filter(fn: (r) => r._measurement == "telemetry_forecast" and r._field == "battery_soc_pct" and r.device_id == "{device_id}")
  |> sort(columns: ["_time"], desc: true)
  |> limit(n: 1)
'''
    rows = await writer.query(flux)
    if not rows:
        return default
    return float(rows[0]["_value"])
