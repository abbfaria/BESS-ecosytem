"""
High-fidelity BESS (Battery Energy Storage System) sensor emulator.

Simulates:
  - PV solar generation (physics-based irradiance model)
  - Wind generation (Ornstein-Uhlenbeck stochastic process)
  - Battery SoC / voltage / current / temperature
  - House load (diurnal profile with noise)
  - Grid connection / voltage / frequency
  - Inverter temperature and mode
  - Energy meter accumulators
"""

from __future__ import annotations

import asyncio
import math
import random
import time
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from ..sensors.models import EmulatorConfig, SensorSnapshot
from ..market_data.timebase import kyiv_local_hour
from ..utils.logging_config import get_logger

log = get_logger(__name__)

# Fault bitmask. Each bit is an independent, *recomputed* condition —
# every one of these must be cleared as well as set, or a single
# transient event latches an alarm for the lifetime of the process.
FAULT_LOW_SOC        = 1
FAULT_BLACKOUT_RISK  = 2
FAULT_THERMAL        = 4
FAULT_GRID_OUTAGE    = 8


class BESSEmulator:
    """Physics-informed BESS emulator with realistic stochastic dynamics."""

    # ── LiFePO4 cell constants ──────────────────────────────────────────
    _CELL_COUNT_S = 16          # 16S configuration
    _CELL_NOMINAL_V = 3.2       # V per cell
    _CELL_FULL_V    = 3.65      # V per cell (100% SoC)
    _CELL_EMPTY_V   = 2.80      # V per cell (0% SoC)

    def __init__(self, config: EmulatorConfig):
        self.cfg = config

        # ── Battery state ──────────────────────────────────────────────
        self._soc_pct        = config.initial_soc_pct
        self._soh_pct        = 100.0
        self._batt_temp_c    = 22.0
        self._total_charge_kwh    = 0.0
        self._total_discharge_kwh = 0.0

        # ── Inverter / control state ──────────────────────────────────
        self._mode           = "SOLAR_PRIORITY"
        self._charge_pct     = 0.0     # 0–100 commanded
        self._discharge_pct  = 0.0
        self._inverter_temp  = 30.0
        self._override_until: Optional[float] = None
        self._commanded_batt_w = 0.0   # +charge / -discharge

        # ── Grid state ────────────────────────────────────────────────
        self._grid_connected = True
        self._grid_v         = 220.0
        self._grid_hz        = 50.0
        self._grid_fault_timer = 0.0   # seconds until fault clears

        # ── Weather / environment ─────────────────────────────────────
        self._cloud_cover    = 0.3     # 0=clear, 1=overcast
        self._cloud_tau_s    = 3600.0  # cloud-cover correlation time
        self._cloud_sd       = 0.2     # stationary std. dev. of cloud cover
        self._cloud_mu       = 0.3     # long-run mean cloud cover
        self._weather_ts: Optional[datetime] = None   # sim time of the cover above
        self._wind_speed_ms  = 5.0     # m/s
        # Wind speed is an Ornstein-Uhlenbeck process, parameterised by its
        # correlation time and stationary spread rather than raw theta/sigma.
        self._wind_ou_tau_s  = 1800.0  # gusts decorrelate over ~30 min
        self._wind_ou_sd     = 1.8     # m/s, stationary std. dev.
        self._wind_ou_mu     = 5.0     # OU long-term mean

        # ── Accumulators ────────────────────────────────────────────
        self._total_pv_energy_kwh   = 0.0
        self._total_grid_import_kwh = 0.0
        self._total_grid_export_kwh = 0.0

        # ── Internal ──────────────────────────────────────────────────
        self._sequence_num   = 0
        self._last_tick_time = time.monotonic()
        self._running        = False
        self._fault_code     = 0

        log.info("BESSEmulator initialised",
                 pv_kw=config.pv_peak_kw,
                 batt_kwh=config.battery_capacity_kwh,
                 soc=self._soc_pct)

    # ── Public control API ───────────────────────────────────────────────

    def set_mode(self, mode: str, charge_pct: float = 0.0,
                 discharge_pct: float = 0.0, duration_min: int = 60) -> None:
        self._mode          = mode
        self._charge_pct    = max(0.0, min(100.0, charge_pct))
        self._discharge_pct = max(0.0, min(100.0, discharge_pct))
        if duration_min > 0:
            self._override_until = time.monotonic() + duration_min * 60
        log.info("Mode set", mode=mode, charge=charge_pct, discharge=discharge_pct)

    def set_soc(self, soc_pct: float) -> None:
        """Place the pack at a given state of charge (forecast anchoring)."""
        self._soc_pct = max(0.0, min(100.0, float(soc_pct)))

    def restore(self, snapshot: dict) -> None:
        """Resume from the last recorded snapshot after a process restart.

        Without this every container restart (deploy, watchdog, VM reboot)
        put the pack back at initial_soc_pct and zeroed the energy meters,
        so the SoC line jumped to 50 % and the counters went backwards.
        """
        def num(key: str, default: float) -> float:
            try:
                return float(snapshot.get(key, default))
            except (TypeError, ValueError):
                return default

        self.set_soc(num("battery_soc_pct", self._soc_pct))
        self._batt_temp_c   = num("battery_temp_c", self._batt_temp_c)
        self._inverter_temp = num("inverter_temp_c", self._inverter_temp)
        self._total_pv_energy_kwh   = num("total_pv_energy_kwh", 0.0)
        self._total_grid_import_kwh = num("total_grid_import_kwh", 0.0)
        self._total_grid_export_kwh = num("total_grid_export_kwh", 0.0)
        self._sequence_num = int(num("sequence_num", 0))
        log.info("Emulator state restored", soc=round(self._soc_pct, 2))

    def force_mode(self, mode: str, charge_pct: float = 0.0,
                    discharge_pct: float = 0.0) -> None:
        """Set dispatch mode directly, without the time.monotonic()-based
        override-expiry timer `set_mode()` uses — that timer is a live-only
        command-timeout safety feature and is meaningless during
        deterministic historical replay (see `tick(as_of=..., dt_s=...)`)."""
        self._mode           = mode
        self._charge_pct     = max(0.0, min(100.0, charge_pct))
        self._discharge_pct  = max(0.0, min(100.0, discharge_pct))
        self._override_until = None

    # ── Snapshot generation ──────────────────────────────────────────────

    async def tick(
        self,
        as_of: Optional[datetime] = None,
        dt_s: Optional[float] = None,
    ) -> SensorSnapshot:
        """Advance simulation by one time step and return a snapshot.

        `as_of`/`dt_s` let a caller deterministically replay the model for
        an arbitrary historical instant (used by the cloud's history
        backfill) instead of always stepping from the real wall clock —
        the live sensor loop never passes them, so its behavior is
        unchanged.
        """
        if dt_s is None:
            now  = time.monotonic()
            dt_s = max(0.1, min(now - self._last_tick_time, 60.0))   # clamp
            self._last_tick_time = now

        utc_now = as_of or datetime.now(timezone.utc)

        # 1. Evolve environment
        self._evolve_weather(dt_s)
        self._weather_ts = utc_now
        self._evolve_grid(dt_s)

        # 2. Compute power flows
        pv_w    = self._compute_pv_power(utc_now)
        wind_w  = self._compute_wind_power()
        load_w  = self._compute_house_load(utc_now)
        batt_w, grid_w = self._compute_dispatch(pv_w, wind_w, load_w, utc_now, dt_s)

        # 3. Update battery SoC
        self._update_battery(batt_w, dt_s)

        # 4. Update accumulators
        dt_h = dt_s / 3600.0
        if pv_w > 0:
            self._total_pv_energy_kwh += pv_w * dt_h / 1000.0
        if grid_w > 0:
            self._total_grid_import_kwh += grid_w * dt_h / 1000.0
        elif grid_w < 0:
            self._total_grid_export_kwh += abs(grid_w) * dt_h / 1000.0

        # 5. Thermal model (inverter warms under load)
        target_inv_t = 30.0 + 0.004 * abs(batt_w) + 0.002 * abs(grid_w)
        self._inverter_temp += (target_inv_t - self._inverter_temp) * min(1, dt_s / 300)
        self._inverter_temp += random.gauss(0, 0.1)

        self._sequence_num += 1

        return SensorSnapshot(
            ts=utc_now,
            device_id=self.cfg.device_id,
            sequence_num=self._sequence_num,
            pv_power_w=round(pv_w, 1),
            wind_power_w=round(wind_w, 1),
            battery_power_w=round(batt_w, 1),
            grid_power_w=round(grid_w, 1),
            house_load_w=round(load_w, 1),
            battery_soc_pct=round(self._soc_pct, 2),
            battery_soh_pct=round(self._soh_pct, 1),
            battery_temp_c=round(self._batt_temp_c, 1),
            battery_voltage_v=round(self._soc_to_voltage(), 2),
            battery_current_a=round(batt_w / max(self._soc_to_voltage(), 1), 1),
            grid_status=self._grid_status_str(),
            grid_voltage_v=round(self._grid_v, 1),
            grid_frequency_hz=round(self._grid_hz, 3),
            inverter_temp_c=round(self._inverter_temp, 1),
            inverter_mode=self._mode,
            total_pv_energy_kwh=round(self._total_pv_energy_kwh, 3),
            total_grid_import_kwh=round(self._total_grid_import_kwh, 3),
            total_grid_export_kwh=round(self._total_grid_export_kwh, 3),
            fault_code=self._fault_code,
        )

    # ── Physics models ───────────────────────────────────────────────────

    def _compute_pv_power(self, utc_now: datetime) -> float:
        """Measured PV power: the physical model plus sensor/weather noise."""
        power = self._pv_power_expected(utc_now)
        if power <= 0.0:
            return 0.0
        return max(0.0, power + random.gauss(0, max(30, power * 0.02)))

    def _pv_power_expected(self, utc_now: datetime,
                           cloud_cover: Optional[float] = None) -> float:
        """Noise-free PV output for a UTC instant.

        Separated from the measured value so the optimizer can be handed a
        forecast built from exactly the physics the plant will follow,
        rather than a second, independently-drifting model of it.

        Clear-sky output follows the real sun over the site. The previous
        fixed curve (sunrise 05:30, sunset 20:30 local, full peak at noon)
        was a June day all year round: in November it produced ~9 kW of
        midday export and a 15-hour solar day, and the planned "expected
        revenue" came out several times what the season allows.
        """
        if utc_now.tzinfo is None:
            utc_now = utc_now.replace(tzinfo=timezone.utc)
        utc_now = utc_now.astimezone(timezone.utc)
        sin_elev, cos_inc = self._sun_geometry(utc_now)
        if sin_elev <= 0.0:
            return 0.0

        # Meinel clear-sky beam (kW/m²) through a Kasten-Young air mass, on
        # the tilted array, plus ~10 % isotropic diffuse; 1 kW/m² = peak.
        elev_deg = math.degrees(math.asin(sin_elev))
        air_mass = 1.0 / (sin_elev + 0.50572 * (elev_deg + 6.07995) ** -1.6364)
        beam = 1.353 * 0.7 ** (air_mass ** 0.678)
        tilt = math.radians(self.cfg.pv_tilt_deg)
        poa = beam * max(0.0, cos_inc) + 0.1 * beam * (1.0 + math.cos(tilt)) / 2.0
        clearsky_w = self.cfg.pv_peak_kw * 1000.0 * min(1.0, poa)

        # Cloud cover attenuation
        cover = self._cloud_cover if cloud_cover is None else cloud_cover
        attenuation = 1.0 - 0.85 * cover
        # Temperature derating (panels lose ~0.4%/°C above 25°C NOCT)
        panel_temp   = max(25.0, 20.0 + 0.02 * clearsky_w / 100)
        temp_derating = max(0.8, 1.0 - 0.004 * (panel_temp - 25.0))

        return clearsky_w * attenuation * temp_derating * self.cfg.inverter_efficiency

    def _sun_geometry(self, utc_now: datetime) -> tuple[float, float]:
        """(sin of solar elevation, cos of incidence on the south-facing
        array) at the site, from the NOAA declination / equation-of-time
        series."""
        hour = utc_now.hour + utc_now.minute / 60.0 + utc_now.second / 3600.0
        g = 2.0 * math.pi / 365.0 * (utc_now.timetuple().tm_yday - 1 + (hour - 12.0) / 24.0)
        eot_min = 229.18 * (0.000075 + 0.001868 * math.cos(g) - 0.032077 * math.sin(g)
                            - 0.014615 * math.cos(2 * g) - 0.040849 * math.sin(2 * g))
        decl = (0.006918 - 0.399912 * math.cos(g) + 0.070257 * math.sin(g)
                - 0.006758 * math.cos(2 * g) + 0.000907 * math.sin(2 * g)
                - 0.002697 * math.cos(3 * g) + 0.00148 * math.sin(3 * g))
        solar_min = hour * 60.0 + eot_min + 4.0 * self.cfg.site_lon_deg
        ha = math.radians(solar_min / 4.0 - 180.0)
        lat = math.radians(self.cfg.site_lat_deg)
        eff = lat - math.radians(self.cfg.pv_tilt_deg)   # tilted plane's "latitude"
        sin_elev = (math.sin(lat) * math.sin(decl)
                    + math.cos(lat) * math.cos(decl) * math.cos(ha))
        cos_inc = (math.sin(eff) * math.sin(decl)
                   + math.cos(eff) * math.cos(decl) * math.cos(ha))
        return sin_elev, cos_inc

    def _compute_wind_power(self) -> float:
        """Measured wind power: the turbine curve plus sensor noise."""
        expected = self._wind_power_expected()
        p_rated  = self.cfg.wind_peak_kw * 1000.0
        return max(0.0, expected + random.gauss(0, p_rated * 0.03))

    def _wind_power_expected(self) -> float:
        """Noise-free turbine output (cubic law with cut-in/cut-out).

        Kept separate from the measured value so forecast_day() stays a
        pure function of state: drawing the noise term there made two
        successive forecasts for the same day disagree, which is both
        wrong as a forecast and a quiet source of schedule churn.
        """
        ws = self._wind_speed_ms
        p_rated = self.cfg.wind_peak_kw * 1000.0
        v_cutin, v_rated, v_cutout = 3.0, 12.0, 25.0
        if ws < v_cutin or ws > v_cutout:
            return 0.0
        frac = (1.0 if ws >= v_rated
                else ((ws - v_cutin) / (v_rated - v_cutin)) ** 3)
        return p_rated * frac

    def _compute_house_load(self, utc_now: datetime) -> float:
        """Measured household load: the profile plus metering noise."""
        return max(100.0, self._house_load_expected(utc_now) + random.gauss(0, 80.0))

    def _house_load_expected(self, utc_now: datetime) -> float:
        """Noise-free household load for a UTC instant (see _pv_power_expected)."""
        hour = kyiv_local_hour(utc_now)

        base       = self.cfg.base_load_w
        # Morning peak 7–9 local
        morning    = 1500.0 * math.exp(-0.5 * ((hour - 8.0) / 1.0) ** 2)
        # Lunch 12–13
        lunch      = 300.0  * math.exp(-0.5 * ((hour - 12.5) / 0.5) ** 2)
        # Evening peak 18–22
        evening    = 2000.0 * math.exp(-0.5 * ((hour - 20.0) / 2.0) ** 2)
        # Night lull
        night_dip  = -200.0 * math.exp(-0.5 * ((hour - 3.0) / 2.0) ** 2)

        return max(100.0, base + morning + lunch + evening + night_dip)

    def forecast_day(self, day: date) -> tuple[list[float], list[float]]:
        """24-hour (UTC-indexed) generation and load forecast, in kW.

        Generation includes the wind contribution at the currently observed
        wind speed, held flat across the horizon: the turbine model has no
        diurnal structure to project, so its present output is the best
        available estimate and leaving it out entirely would understate
        self-supply. Cloud cover is *not* held flat: it is the expected
        value of its own OU process given the current observation, which
        relaxes to the long-run mean within a few correlation times (~1 h).
        Holding the cover seen at planning time (14:05 UTC) for the whole
        next day made tomorrow's plan a coin flip between a cloudless and
        an overcast day.

        Indexed by UTC hour so it lines up with the price curve the
        optimizer receives (see market_data.timebase).

        Each value is the *mean* over its hour (5-minute sub-samples), the
        quantity a 1-hour MILP slot actually trades. Sampling the instant at
        hh:00 instead biased every slot by half an hour of curve slope and
        overstated planned revenue by ~2.5 % against a noise-free replay.
        """
        wind_kw = self._wind_power_expected() / 1000.0
        gen, load = [], []
        for h in range(24):
            at = datetime(day.year, day.month, day.day, h, tzinfo=timezone.utc)
            subs = [at + timedelta(minutes=m) for m in range(2, 60, 5)]
            gen.append(sum(self._pv_power_expected(t, self._expected_cloud_cover(t))
                           for t in subs) / len(subs) / 1000.0
                       + wind_kw)
            load.append(sum(self._house_load_expected(t) for t in subs) / len(subs) / 1000.0)
        return gen, load

    def _expected_cloud_cover(self, at: datetime) -> float:
        """E[cloud cover at `at`] given the last observed value."""
        if self._weather_ts is None:
            return self._cloud_mu
        lead_s = max(0.0, (at - self._weather_ts).total_seconds())
        return (self._cloud_mu + (self._cloud_cover - self._cloud_mu)
                * math.exp(-lead_s / self._cloud_tau_s))

    def _compute_dispatch(self, pv_w: float, wind_w: float,
                          load_w: float, utc_now: datetime,
                          dt_s: Optional[float] = None,
                          ) -> tuple[float, float]:
        """
        Dispatch logic: returns (battery_power_w, grid_power_w).
        battery_power_w: + = charging, - = discharging
        grid_power_w:    + = import, - = export
        """
        # Check override expiry
        if self._override_until and time.monotonic() > self._override_until:
            self._mode          = "SOLAR_PRIORITY"
            self._charge_pct    = 0.0
            self._discharge_pct = 0.0
            self._override_until = None

        total_gen = pv_w + wind_w
        surplus   = total_gen - load_w          # positive = excess generation

        batt_max_chg_w  = self.cfg.battery_charge_kw_max    * 1000.0
        batt_max_dis_w  = self.cfg.battery_discharge_kw_max * 1000.0

        if not self._grid_connected:
            # Island mode: the battery alone balances the node. This used to
            # return early, which skipped the SoC clamp below entirely and
            # let an islanded node discharge straight through soc_min.
            batt_w = (min(surplus, batt_max_chg_w) if surplus >= 0
                      else max(surplus, -batt_max_dis_w))

        # Each mode decides only the *battery* set-point. Grid flow is then
        # derived from it below, after the SoC limits have had their say.
        elif self._mode == "SOLAR_PRIORITY":
            batt_w = (min(surplus, batt_max_chg_w) if surplus > 0
                      else max(surplus, -batt_max_dis_w))

        elif self._mode == "GRID_CHARGE":
            batt_w = min(batt_max_chg_w * self._charge_pct / 100.0, batt_max_chg_w)

        elif self._mode == "DISCHARGE_SELL":
            batt_w = -min(batt_max_dis_w * self._discharge_pct / 100.0, batt_max_dis_w)

        elif self._mode == "BACKUP_MODE":
            batt_w = (min(surplus, batt_max_chg_w) if surplus >= 0
                      else max(surplus, -batt_max_dis_w))

        else:   # IDLE
            batt_w = 0.0

        # ── SoC limits, applied before grid flow is derived ───────────────
        if batt_w > 0 and self._soc_pct >= self.cfg.soc_max_pct:
            batt_w = 0.0

        # Within one step the pack may only travel as far as its limits.
        # Checking the limit alone at the start of the step let a 5-minute
        # backfill tick at full power overshoot it by ~2 pp (8 % / 97 %).
        if dt_s and dt_s > 0:
            dt_h = dt_s / 3600.0
            cap_wh = self.cfg.battery_capacity_kwh * 1000.0
            if batt_w > 0:
                room_wh = max(0.0, self.cfg.soc_max_pct - self._soc_pct) / 100.0 * cap_wh
                batt_w = min(batt_w, room_wh / (self.cfg.charge_efficiency * dt_h))
            elif batt_w < 0 and self._soc_pct > self.cfg.soc_min_pct:
                avail_wh = (self._soc_pct - self.cfg.soc_min_pct) / 100.0 * cap_wh
                batt_w = max(batt_w, -avail_wh * self.cfg.discharge_efficiency / dt_h)

        # Low-SoC / blackout-risk conditions are *transient*: they describe
        # the state right now, so they are recomputed (set AND cleared)
        # every tick. Earlier revisions assigned these with `=`, which both
        # latched them until the process restarted and clobbered the
        # unrelated thermal/grid bits stored in the same bitmask.
        low_soc = (batt_w < 0 and self._soc_pct <= self.cfg.soc_min_pct)
        if low_soc:
            batt_w = 0.0

        self._set_fault(FAULT_LOW_SOC,
                        low_soc and self._mode == "DISCHARGE_SELL")
        self._set_fault(FAULT_BLACKOUT_RISK,
                        low_soc and not self._grid_connected)

        # ── Grid flow = whatever the node cannot balance itself ───────────
        # Single energy-balance identity for every grid-connected mode:
        #   import(+) = load + battery_charge - generation
        # Deriving it here, from the *final* batt_w, is what keeps the model
        # physically consistent. Previously each mode computed grid_w from
        # its own un-clamped battery set-point, and the SoC limits were
        # applied afterwards without recomputing it — so a DISCHARGE_SELL
        # hour with an empty battery reported the full planned export even
        # though the battery contributed nothing. Observed live: PV 0 W,
        # load 1.6 kW, battery 0 W, grid -1.9 kW — 1.9 kW exported out of
        # thin air, and then billed as revenue.
        if not self._grid_connected:
            grid_w = 0.0
        elif self._mode == "BACKUP_MODE":
            # Backup never exports; it only draws what the battery cannot cover.
            grid_w = max(0.0, load_w + batt_w - total_gen)
        else:
            grid_w = load_w + batt_w - total_gen

        self._commanded_batt_w = batt_w
        return batt_w, grid_w

    def _set_fault(self, bit: int, active: bool) -> None:
        """Set or clear one bit of the fault bitmask."""
        if active:
            self._fault_code |= bit
        else:
            self._fault_code &= ~bit

    # ── State evolution ───────────────────────────────────────────────────

    def _update_battery(self, batt_w: float, dt_s: float) -> None:
        dt_h = dt_s / 3600.0
        if batt_w > 0:   # charging
            energy_delta_kwh = batt_w * self.cfg.charge_efficiency * dt_h / 1000.0
        else:             # discharging
            energy_delta_kwh = batt_w / self.cfg.discharge_efficiency * dt_h / 1000.0

        soc_delta = energy_delta_kwh / self.cfg.battery_capacity_kwh * 100.0
        # Self-discharge: ~3% per month = 0.004%/hour
        self._soc_pct -= 0.004 * dt_h
        self._soc_pct += soc_delta
        self._soc_pct  = max(0.0, min(100.0, self._soc_pct))

        # Thermal model (simple exponential)
        ambient = 22.0
        target_temp = ambient + 5.0 * abs(batt_w) / (self.cfg.battery_charge_kw_max * 1000.0)
        self._batt_temp_c += (target_temp - self._batt_temp_c) * min(1, dt_s / 600)
        self._batt_temp_c += random.gauss(0, 0.05)

        # Thermal fault, with hysteresis so a temperature hovering at the
        # threshold doesn't chatter the alarm on and off each tick.
        if self._batt_temp_c > 45.0:
            self._set_fault(FAULT_THERMAL, True)
        elif self._batt_temp_c < 43.0:
            self._set_fault(FAULT_THERMAL, False)

    def _evolve_weather(self, dt_s: float) -> None:
        """Mean-reverting (Ornstein-Uhlenbeck) cloud cover and wind speed.

        Both use the exact OU transition rather than an Euler step. The Euler
        form x += theta*(mu-x)*dt is only stable for theta*dt < 2; with the
        old wind theta of 0.1/s that already failed beyond 20 s, so the
        history backfill (300 s ticks) and forecast (900 s) produced wind
        speeds with a median of ~59 m/s alternating in sign, i.e. almost no
        wind generation in any synthetic day. The exact step is stable and
        statistically identical for every dt.
        """
        self._cloud_cover = self._ou_step(self._cloud_cover, self._cloud_mu,
                                          self._cloud_tau_s, self._cloud_sd, dt_s)
        self._cloud_cover = max(0.0, min(1.0, self._cloud_cover))

        self._wind_speed_ms = self._ou_step(self._wind_speed_ms, self._wind_ou_mu,
                                            self._wind_ou_tau_s, self._wind_ou_sd, dt_s)
        self._wind_speed_ms = max(0.0, self._wind_speed_ms)

    @staticmethod
    def _ou_step(x: float, mu: float, tau_s: float, sd: float, dt_s: float) -> float:
        """Exact OU transition over dt_s for correlation time tau_s and
        stationary standard deviation sd."""
        a = math.exp(-dt_s / tau_s)
        return mu + (x - mu) * a + sd * math.sqrt(1.0 - a * a) * random.gauss(0, 1)

    def _evolve_grid(self, dt_s: float) -> None:
        """Simulate grid voltage/frequency fluctuations and rare outages."""
        if self._grid_fault_timer > 0:
            self._grid_fault_timer -= dt_s
            if self._grid_fault_timer <= 0:
                self._grid_connected = True
                self._set_fault(FAULT_GRID_OUTAGE, False)
                log.info("Grid reconnected after simulated outage")
            return

        # Small mean-reverting fluctuations around nominal (exact OU step —
        # the Euler form with a 60 s frequency time constant diverged for
        # dt > 120 s and pinned backfilled frequency at 47.5/52.5 Hz, which
        # tagged ~99 % of synthetic history as UNSTABLE).
        self._grid_v  = self._ou_step(self._grid_v,  220.0, 600.0, 2.2,  dt_s)
        self._grid_hz = self._ou_step(self._grid_hz, 50.0,  60.0,  0.012, dt_s)
        self._grid_v   = max(195.0, min(253.0, self._grid_v))
        self._grid_hz  = max(47.5,  min(52.5,  self._grid_hz))

        # Rare outage simulation (mean rate: 2 per day = ~1/43200 per second)
        if self._grid_connected and random.random() < dt_s / 86400:
            duration_s = random.expovariate(1/1800)   # avg 30 min outage
            self._grid_connected  = False
            self._grid_fault_timer = duration_s
            self._set_fault(FAULT_GRID_OUTAGE, True)
            log.warning("Simulated grid outage", duration_min=round(duration_s/60, 1))

    # ── Helpers ────────────────────────────────────────────────────────────

    def _soc_to_voltage(self) -> float:
        """Convert SoC% to pack voltage (LFP approximation)."""
        soc = self._soc_pct / 100.0
        cell_v = self._CELL_EMPTY_V + (self._CELL_FULL_V - self._CELL_EMPTY_V) * soc
        return cell_v * self._CELL_COUNT_S

    def _grid_status_str(self) -> str:
        if not self._grid_connected:
            return "DISCONNECTED"
        if abs(self._grid_v - 220.0) > 15 or abs(self._grid_hz - 50.0) > 0.5:
            return "UNSTABLE"
        return "CONNECTED"

    @property
    def is_running(self) -> bool:
        return self._running
