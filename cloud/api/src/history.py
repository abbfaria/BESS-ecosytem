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

from edge_src.control.optimizer import BESSOptimizer, OptimizerConfig, HourlyAction  # noqa: E402
from edge_src.control.economics import compute_revenue_uah                  # noqa: E402
from edge_src.sensors.emulator import BESSEmulator                          # noqa: E402
from edge_src.sensors.models import EmulatorConfig                          # noqa: E402
from edge_src.market_data.fallback_data import get_fallback_prices          # noqa: E402

from .influx_writer import InfluxWriter
from .market_fetcher import fetch_dam_prices
from .logging_config import get_logger

log = get_logger(__name__)

BACKFILL_DAYS   = 35    # margin over the payback panel's 30-day window
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

    # Before anything reads `schedule`: the stale mode-tagged series make
    # _stored_edge_schedule see >24 rows and double every revenue sum.
    await writer.purge_legacy_schedule_points()

    today = today or datetime.now(timezone.utc).date()
    await _purge_synthetic_overlaps(writer, bucket, device_id,
                                    today - timedelta(days=BACKFILL_DAYS),
                                    today + timedelta(days=1))
    optimizer = BESSOptimizer(OptimizerConfig())
    emulator  = BESSEmulator(EmulatorConfig(device_id=device_id))
    soc       = emulator.cfg.initial_soc_pct

    start = today - timedelta(days=BACKFILL_DAYS)
    day   = start
    filled, topped_up, skipped = 0, 0, 0
    # Today is included, not excluded. The window used to stop at yesterday,
    # which left any hour of *today* that elapsed while the host was asleep
    # permanently empty — "Дохід сьогодні" then reported only the hours the
    # machine happened to be awake for. Only hours strictly before the
    # current one are eligible: the hour in progress is still accumulating
    # live telemetry, and filling it would interleave synthetic points with
    # real ones inside the same hour.
    current_hour = datetime.now(timezone.utc).hour
    while day <= today:
        missing = await _missing_hours(writer, bucket, device_id, day)
        if day == today:
            missing = {h for h in missing if h < current_hour}
        if not missing:
            # A day replayed while oree.com.ua was unreachable keeps its
            # fallback curve forever unless retried here: prices are
            # otherwise only fetched for days with missing telemetry. The
            # telemetry stays as replayed; revenue panels prefer "oree".
            if day < today and await _price_source(writer, bucket, device_id, day) == "fallback":
                fetched = await fetch_dam_prices(day)
                if fetched:
                    await writer.write_market_price(device_id, day.isoformat(), fetched, "oree")
                    log.info("Past day's prices upgraded from fallback to real",
                             date=day.isoformat())
            # Still need to know the day's ending SoC to keep the *next*
            # day's optimizer input realistic — cheap to read back.
            soc = await _last_known_soc(writer, bucket, device_id, day, soc)
            skipped += 1
            day += timedelta(days=1)
            continue

        prices, source = await _prices_for(writer, bucket, device_id, day)
        gen_kw, load_kw = emulator.forecast_day(day)
        schedule = optimizer.optimize(prices, soc,
                                      gen_forecast_kw=gen_kw,
                                      load_forecast_kw=load_kw)
        soc = await _replay_day(writer, emulator, device_id, day, schedule, hours=missing,
                                anchor_bucket=bucket)

        await writer.write_market_price(device_id, day.isoformat(), prices, source)
        # Only record this replay plan when the day has none: a partially
        # covered day normally already holds the schedule the device itself
        # ran, and the dashboard must keep showing that one.
        if not await _has_schedule(writer, bucket, device_id, day):
            await writer.write_schedule(device_id, _schedule_payload(day, schedule, source),
                                        producer="cloud")
        if len(missing) == 24:
            filled += 1
        else:
            topped_up += 1
            log.info("Day topped up (partial coverage)", date=day.isoformat(),
                      missing_hours=sorted(missing))
        day += timedelta(days=1)

    log.info("History backfill complete", days_filled=filled, days_topped_up=topped_up,
             days_already_present=skipped,
             window_start=start.isoformat(), window_end=today.isoformat())

    await _ensure_forward(writer, bucket, optimizer, emulator, device_id, today, soc)


def _schedule_payload(day: date, schedule: list, price_source: str) -> dict:
    return {
        "valid_date": day.isoformat(),
        "price_source": price_source,
        "slots": [
            {"hour": a.hour, "mode": a.mode,
             "charge_power_pct": a.charge_pct,
             "discharge_power_pct": a.discharge_pct,
             "price_uah_mwh": a.price_uah_mwh,
             "expected_revenue_uah": a.expected_revenue}
            for a in schedule
        ],
    }


async def _replay_day(writer: InfluxWriter, emulator: "BESSEmulator", device_id: str,
                       day: date, schedule: list, forecast: bool = False,
                       hours: Optional[set[int]] = None,
                       anchor_bucket: Optional[str] = None) -> float:
    """Step the real BESSEmulator through one day, writing telemetry as it
    goes. `forecast=False` (past days): TICK_MINUTES resolution, written to
    `telemetry` — this is the backfill's "as if the device had really been
    running" path. `forecast=True` (future days): coarser
    FORECAST_TICK_MINUTES resolution, written to `telemetry_forecast` — a
    clearly separate measurement, since nothing has actually happened yet
    for a day that hasn't occurred. `hours`, when given, restricts replay
    to only those hours-of-day — used to top up a day that already has
    real telemetry for *some* hours (a brief live connection window)
    without rewriting the ones it already has; `None` replays the whole
    day, for a genuinely uncovered one. Returns the SoC at end of day (or
    at the end of the last replayed hour), to carry into the next day's
    optimizer call — note that when `hours` skips real-data hours, this
    doesn't re-derive SoC through them, so the boundary between real and
    backfilled hours within the same day is a reasonable approximation,
    not a physically exact continuation; this only affects the internal
    SoC trajectory used to plan dispatch, not the revenue already recorded
    for the real hours themselves."""
    day_start = datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)
    step_minutes = FORECAST_TICK_MINUTES if forecast else TICK_MINUTES
    step_seconds = FORECAST_TICK_SECONDS if forecast else TICK_SECONDS
    write = writer.write_telemetry_forecast if forecast else writer.write_telemetry
    last_soc = emulator.cfg.initial_soc_pct

    if forecast:
        # A rebuilt forecast must replace the old one, not sit beside it:
        # grid_status/inverter_mode are tags, so the same instant planned
        # in a different mode is a separate series and the panels averaged
        # the stale projection into the new one.
        first_hour = min(hours) if hours else 0
        await writer.delete_range("telemetry_forecast", device_id,
                                  day_start + timedelta(hours=first_hour),
                                  day_start + timedelta(days=1))

    for minute_of_day in range(0, 24 * 60, step_minutes):
        hour = minute_of_day // 60
        if hours is not None and hour not in hours:
            continue
        ts = day_start + timedelta(minutes=minute_of_day)
        # A gap filled between real hours starts from the SoC actually
        # recorded before it, so the line joins its neighbours instead of
        # jumping to wherever the replay chain happened to be.
        if (anchor_bucket and hours is not None and minute_of_day % 60 == 0
                and hour - 1 not in hours):
            anchor = await _soc_before(writer, anchor_bucket, device_id, ts)
            if anchor is not None:
                emulator.set_soc(anchor)
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
    exactly as it would through any other day.

    DAM auction results for a given day typically aren't published until
    the afternoon of the *previous* day (see market_fetcher.py / the
    MARKET_FETCH_HOUR_UTC convention), so a schedule computed for
    today/tomorrow before that moment is necessarily built on the
    fallback curve. Once written, the old any-schedule-exists check
    treated that as permanently done — the cloud-side "Очікуваний дохід
    завтра" figure could stay pinned to a fallback-based estimate
    indefinitely, even hours after the real auction result was published,
    if nothing happened to trigger a recompute. For the near-term days
    only (where a same-day upgrade is actually possible — days further
    out have no real data to fetch regardless of how many times it's
    retried), a fallback-sourced schedule is treated as *not yet done*
    and retried on every run.
    """
    filled, skipped, upgraded = 0, 0, 0
    near_term_upgrade_days = 2   # today + tomorrow: the only days for which
                                  # a same-day real-price upgrade is realistic

    # Anchor the projection to the battery's *actual* current charge, at the
    # current hour. Handing that SoC only to the optimizer (as before) left
    # the replay itself starting at midnight from whatever state the
    # emulator happened to hold, so the projected SoC at "now" could sit
    # 20-40 points away from the measured one and the dashboard line jumped
    # where actual hands over to forecast.
    actual_soc = await _latest_actual_soc(writer, bucket, device_id)
    now_hour = datetime.now(timezone.utc).hour
    if actual_soc is not None:
        soc = actual_soc

    for offset in range(FORECAST_DAYS):
        d = today + timedelta(days=offset)
        near_term = offset < near_term_upgrade_days
        has_schedule = await _has_schedule(writer, bucket, device_id, d)
        has_telemetry_fc = await _has_telemetry_forecast(writer, bucket, device_id, d)
        producer, sched_price_source = (await _schedule_meta(writer, bucket, device_id, d)
                                        if has_schedule and near_term else (None, None))
        # A schedule the device published is authoritative; one this service
        # wrote is redone once real prices exist for its day.
        needs_upgrade = (near_term and has_schedule and producer != "edge"
                         and sched_price_source != "oree")
        # The near-term projection is always rebuilt, never reused: it is the
        # part that viewers actually read ("will it charge this evening?"),
        # and it is the part reality diverges from fastest.
        if near_term:
            has_telemetry_fc = False

        if has_schedule and has_telemetry_fc and not needs_upgrade:
            soc = await _last_known_soc_forecast(writer, bucket, device_id, d, soc)
            skipped += 1
            continue

        start_hour = 0
        if offset == 0 and actual_soc is not None:
            emulator.set_soc(actual_soc)
            start_hour = now_hour

        prices, source = await _prices_for(writer, bucket, device_id, d)
        # Project what the device will actually do: its own schedule when it
        # has published one, otherwise what the same optimizer would choose.
        schedule = (await _stored_edge_schedule(writer, bucket, device_id, d)
                    if near_term and producer == "edge" else None)
        if schedule is None:
            gen_kw, load_kw = emulator.forecast_day(d)
            schedule = optimizer.optimize(prices, soc,
                                          gen_forecast_kw=gen_kw,
                                          load_forecast_kw=load_kw,
                                          start_hour=start_hour)
        upgraded_now = needs_upgrade and source == "oree"
        if upgraded_now:
            upgraded += 1
            log.info("Forecast upgraded from fallback to real prices",
                      date=d.isoformat())

        if not has_schedule or upgraded_now:
            await writer.write_market_price(device_id, d.isoformat(), prices, source)
            await writer.write_schedule(device_id, _schedule_payload(d, schedule, source),
                                        producer="cloud")

        if not has_telemetry_fc or upgraded_now:
            soc = await _replay_day(writer, emulator, device_id, d, schedule,
                                     forecast=True,
                                     hours=set(range(start_hour, 24)) if start_hour else None)
        elif needs_upgrade:
            soc = await _last_known_soc_forecast(writer, bucket, device_id, d, soc)
        filled += 1

    log.info("Forward projection ensured", days_filled=filled, days_already_present=skipped,
             forecasts_upgraded_to_real=upgraded, horizon_days=FORECAST_DAYS,
             window_end=(today + timedelta(days=FORECAST_DAYS - 1)).isoformat())


# ── InfluxDB gap-detection helpers ──────────────────────────────────────────

# Backfilled points sit exactly on a TICK_MINUTES boundary with zero
# sub-seconds; live telemetry carries the edge's microsecond timestamps.
_SYNTHETIC_TS = (f'date.second(t: r._time) == 0 and date.nanosecond(t: r._time) == 0 '
                 f'and date.minute(t: r._time) % {TICK_MINUTES} == 0')


async def _purge_synthetic_overlaps(writer: InfluxWriter, bucket: str, device_id: str,
                                    start: date, stop: date) -> None:
    """Remove backfilled points from hours that also hold real telemetry.

    An hour is backfilled only while it has no telemetry at all, but the
    edge buffers telemetry while it is offline and replays it on reconnect,
    so the real points for that hour can arrive afterwards. Both then sit
    in `telemetry` and every panel averages two unrelated trajectories
    (seen: real SoC 77 % against backfilled 37 % in the same hour). Real
    data wins; the synthetic points of such an hour are deleted.
    """
    flux = f'''
import "date"
from(bucket:"{bucket}")
  |> range(start: {start.isoformat()}T00:00:00Z, stop: {stop.isoformat()}T00:00:00Z)
  |> filter(fn: (r) => r._measurement == "telemetry" and r._field == "battery_soc_pct" and r.device_id == "{device_id}")
  |> drop(columns: ["grid_status", "inverter_mode", "device_id"])
  |> group()
  |> map(fn: (r) => ({{_time: r._time, h: date.truncate(t: r._time, unit: 1h),
                       syn: if {_SYNTHETIC_TS} then 1 else 0}}))
  |> group(columns: ["h"])
  |> reduce(identity: {{s: 0, n: 0}}, fn: (r, accumulator) => ({{s: accumulator.s + r.syn, n: accumulator.n + 1}}))
  |> filter(fn: (r) => r.s > 0 and r.n > r.s)
  |> group()
  |> keep(columns: ["h"])
'''
    # Hours whose backfilled timestamps carry more than one point. Before
    # _missing_hours labelled windows by their start, an empty hour made the
    # hour after it look missing, and every pass replayed that hour again;
    # grid_status/inverter_mode are tags, so each replay landed as another
    # parallel series (up to four SoC values at one instant). All synthetic
    # points of such an hour go; if nothing real is left it is simply
    # refilled once by the backfill that follows.
    stacked_flux = f'''
import "date"
from(bucket:"{bucket}")
  |> range(start: {start.isoformat()}T00:00:00Z, stop: {stop.isoformat()}T00:00:00Z)
  |> filter(fn: (r) => r._measurement == "telemetry" and r._field == "battery_soc_pct" and r.device_id == "{device_id}")
  |> drop(columns: ["grid_status", "inverter_mode", "device_id"])
  |> group()
  |> filter(fn: (r) => {_SYNTHETIC_TS})
  |> group(columns: ["_time"])
  |> count()
  |> filter(fn: (r) => r._value > 1)
  |> group()
  |> map(fn: (r) => ({{h: date.truncate(t: r._time, unit: 1h)}}))
  |> unique(column: "h")
'''
    hours = sorted({r["h"] for r in await writer.query(flux)}
                   | {r["h"] for r in await writer.query(stacked_flux)})
    for h in hours:
        h_end = h + timedelta(hours=1)
        ts_flux = f'''
import "date"
from(bucket:"{bucket}")
  |> range(start: {h.strftime("%Y-%m-%dT%H:%M:%SZ")}, stop: {h_end.strftime("%Y-%m-%dT%H:%M:%SZ")})
  |> filter(fn: (r) => r._measurement == "telemetry" and r._field == "battery_soc_pct" and r.device_id == "{device_id}")
  |> filter(fn: (r) => {_SYNTHETIC_TS})
  |> keep(columns: ["_time"])
  |> group()
'''
        times = sorted({r["_time"] for r in await writer.query(ts_flux)})
        for t in times:
            await writer.delete_range("telemetry", device_id, t, t + timedelta(microseconds=2))
    if hours:
        log.info("Removed overlapping or duplicated backfilled points",
                 hours=len(hours))


async def _soc_before(writer: InfluxWriter, bucket: str, device_id: str,
                      ts: datetime) -> Optional[float]:
    """Last recorded SoC in the 6 h before `ts`, or None."""
    flux = f'''
from(bucket:"{bucket}")
  |> range(start: {(ts - timedelta(hours=6)).strftime("%Y-%m-%dT%H:%M:%SZ")}, stop: {ts.strftime("%Y-%m-%dT%H:%M:%SZ")})
  |> filter(fn: (r) => r._measurement == "telemetry" and r._field == "battery_soc_pct" and r.device_id == "{device_id}")
  |> drop(columns: ["grid_status", "inverter_mode", "device_id"])
  |> group()
  |> sort(columns: ["_time"])
  |> last()
'''
    rows = await writer.query(flux)
    return float(rows[0]["_value"]) if rows else None

async def _missing_hours(writer: InfluxWriter, bucket: str, device_id: str,
                          day: date) -> set[int]:
    """Which of the day's 24 hours have *no* telemetry at all.

    A flat point-count threshold isn't the right completeness test: a day
    where the edge was only live for one contiguous stretch can easily
    clear a count threshold (thousands of 10s-cadence points) while still
    missing several *specific* hours entirely — and because revenue
    concentrates in just the 2-4 hours the optimizer actually schedules a
    charge or discharge, exactly those hours are the ones most likely to
    fall in a connectivity gap and the most costly to miss: a day can look
    "fully covered" by a count check and still show almost no realized
    revenue because the hours that mattered financially are the ones
    missing. Checking hour-by-hour instead catches that; the caller then
    backfills only the missing hours (see _replay_day's `hours` param),
    never touching the ones with real data.
    """
    # drop() before aggregateWindow(count) is required, not cosmetic:
    # telemetry carries grid_status/inverter_mode tags that vary within a
    # day (SOLAR_PRIORITY vs DISCHARGE_SELL, CONNECTED vs UNSTABLE, ...).
    # Without dropping them first, aggregation groups per unique tag
    # combination and returns multiple rows per hour instead of one true
    # per-hour total — the exact tag-splitting failure mode already
    # documented for the Grafana panels themselves.
    # timeSrc "_start" labels each window by the hour it covers. With the
    # default (_stop) an empty hour h was reported as h+1: the backfill
    # wrote into the next hour, on top of its real data, and h stayed empty.
    flux = f'''
import "date"
from(bucket:"{bucket}")
  |> range(start: {day.isoformat()}T00:00:00Z, stop: {(day + timedelta(days=1)).isoformat()}T00:00:00Z)
  |> filter(fn: (r) => r._measurement == "telemetry" and r._field == "revenue_uah" and r.device_id == "{device_id}")
  |> drop(columns: ["grid_status", "inverter_mode", "device_id"])
  |> aggregateWindow(every: 1h, fn: count, createEmpty: true, timeSrc: "_start")
  |> map(fn: (r) => ({{hour: date.hour(t: r._time), covered: r._value}}))
  |> keep(columns: ["hour", "covered"])
'''
    rows = await writer.query(flux)
    covered = {int(r["hour"]) for r in rows if float(r.get("covered", 0)) > 0}
    return set(range(24)) - covered


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


async def _schedule_meta(writer: InfluxWriter, bucket: str, device_id: str,
                         day: date) -> tuple[Optional[str], Optional[str]]:
    """(producer, price_source) of the schedule stored for `day`, as last
    written. Points written before these fields existed give (None, None)."""
    out: dict[str, Optional[str]] = {"producer": None, "price_source": None}
    for field in out:
        flux = f'''
from(bucket:"{bucket}")
  |> range(start: {day.isoformat()}T00:00:00Z, stop: {(day + timedelta(days=1)).isoformat()}T00:00:00Z)
  |> filter(fn: (r) => r._measurement == "schedule" and r._field == "{field}" and r.device_id == "{device_id}")
  |> group()
  |> sort(columns: ["_time"])
  |> last()
'''
        rows = await writer.query(flux)
        if rows:
            out[field] = str(rows[0]["_value"])
    return out["producer"], out["price_source"]


async def _stored_edge_schedule(writer: InfluxWriter, bucket: str, device_id: str,
                                day: date) -> Optional[list]:
    """The 24 hourly actions of the device's own published schedule for
    `day`, or None if it is incomplete or lacks the power set-points."""
    flux = f'''
from(bucket:"{bucket}")
  |> range(start: {day.isoformat()}T00:00:00Z, stop: {(day + timedelta(days=1)).isoformat()}T00:00:00Z)
  |> filter(fn: (r) => r._measurement == "schedule" and r.device_id == "{device_id}")
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> sort(columns: ["_time"])
'''
    rows = await writer.query(flux)
    if len(rows) != 24 or any("charge_pct" not in r or r.get("charge_pct") is None for r in rows):
        return None
    return [
        HourlyAction(hour=r["_time"].hour, mode=str(r.get("mode") or "IDLE"),
                     charge_pct=float(r.get("charge_pct") or 0.0),
                     discharge_pct=float(r.get("discharge_pct") or 0.0),
                     price_uah_mwh=float(r.get("price_uah_mwh") or 0.0),
                     expected_revenue=float(r.get("expected_revenue_uah") or 0.0))
        for r in rows
    ]


async def _price_source(writer: InfluxWriter, bucket: str, device_id: str,
                         day: date) -> Optional[str]:
    """The `source` tag (`oree` or `fallback`) of the market_price entry
    currently stored for `day`, or None if none exists yet.

    `source` is a tag, so a real fetch lands as a second series beside the
    fallback one rather than replacing it. "oree" wins whenever it exists —
    the same rule the dashboard's revenue panels apply — otherwise a day
    already upgraded would still read as fallback and be refetched on
    every start."""
    flux = f'''
from(bucket:"{bucket}")
  |> range(start: {day.isoformat()}T00:00:00Z, stop: {(day + timedelta(days=1)).isoformat()}T00:00:00Z)
  |> filter(fn: (r) => r._measurement == "market_price" and r._field == "price_uah_mwh" and r.device_id == "{device_id}")
  |> keep(columns: ["source"])
  |> group()
  |> sort(columns: ["source"], desc: true)
  |> limit(n: 1)
'''
    rows = await writer.query(flux)
    return rows[0]["source"] if rows else None


async def _cached_market_prices(writer: InfluxWriter, bucket: str, device_id: str,
                                 day: date) -> Optional[list[float]]:
    """Previously-stored real prices for `day`, or None to go and fetch them.

    Returning None for a day whose points predate the UTC-hour convention
    is what makes the timezone correction reach existing history. Those
    points sit at the right timestamps but hold Kyiv-local-hour values, and
    this function reads them back positionally — so without the check it
    would hand a locally-indexed curve to an optimizer that assumes UTC and
    then write it straight back, preserving the three-hour displacement
    indefinitely. Refetching rewrites the same 24 timestamps in place, so
    the repair needs no deletion.
    """
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

    marker = f'''
from(bucket:"{bucket}")
  |> range(start: {day.isoformat()}T00:00:00Z, stop: {(day + timedelta(days=1)).isoformat()}T00:00:00Z)
  |> filter(fn: (r) => r._measurement == "market_price" and r._field == "hour_basis_utc"
            and r.device_id == "{device_id}" and r.source == "oree")
  |> count()
'''
    marked = await writer.query(marker)
    if not marked or int(marked[0]["_value"]) != 24:
        log.info("Stored prices predate the UTC-hour convention — refetching",
                  date=day.isoformat())
        return None

    return [float(r["_value"]) for r in rows]


async def _last_known_soc(writer: InfluxWriter, bucket: str, device_id: str,
                           day: date, default: float) -> float:
    flux = f'''
from(bucket:"{bucket}")
  |> range(start: {day.isoformat()}T00:00:00Z, stop: {(day + timedelta(days=1)).isoformat()}T00:00:00Z)
  |> filter(fn: (r) => r._measurement == "telemetry" and r._field == "battery_soc_pct" and r.device_id == "{device_id}")
  |> drop(columns: ["grid_status", "inverter_mode", "device_id"])
  |> group()
  |> sort(columns: ["_time"], desc: true)
  |> limit(n: 1)
'''
    rows = await writer.query(flux)
    if not rows:
        return default
    return float(rows[0]["_value"])


async def _latest_actual_soc(writer: InfluxWriter, bucket: str, device_id: str
                              ) -> Optional[float]:
    """Most recent *measured* state of charge, or None if the device has
    not reported recently. Used to anchor the forward projection to
    reality rather than letting it continue from a stale assumption."""
    flux = f'''
from(bucket:"{bucket}")
  |> range(start: -6h)
  |> filter(fn: (r) => r._measurement == "telemetry" and r._field == "battery_soc_pct" and r.device_id == "{device_id}")
  |> drop(columns: ["grid_status", "inverter_mode", "device_id"])
  |> group()
  |> sort(columns: ["_time"])
  |> last()
'''
    rows = await writer.query(flux)
    if not rows:
        return None
    return float(rows[0]["_value"])


async def _last_known_soc_forecast(writer: InfluxWriter, bucket: str, device_id: str,
                                    day: date, default: float) -> float:
    flux = f'''
from(bucket:"{bucket}")
  |> range(start: {day.isoformat()}T00:00:00Z, stop: {(day + timedelta(days=1)).isoformat()}T00:00:00Z)
  |> filter(fn: (r) => r._measurement == "telemetry_forecast" and r._field == "battery_soc_pct" and r.device_id == "{device_id}")
  |> drop(columns: ["grid_status", "inverter_mode", "device_id"])
  |> group()
  |> sort(columns: ["_time"], desc: true)
  |> limit(n: 1)
'''
    rows = await writer.query(flux)
    if not rows:
        return default
    return float(rows[0]["_value"])
