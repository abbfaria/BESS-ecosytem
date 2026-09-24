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
from datetime import datetime, timezone
from typing import Optional

from ..sensors.models import EmulatorConfig, SensorSnapshot
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
        self._cloud_cover    = 0.2     # 0=clear, 1=overcast
        self._wind_speed_ms  = 5.0     # m/s
        self._wind_ou_theta  = 0.1     # OU mean-reversion speed
        self._wind_ou_sigma  = 0.8     # OU diffusion
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
        self._evolve_grid(dt_s)

        # 2. Compute power flows
        pv_w    = self._compute_pv_power(utc_now)
        wind_w  = self._compute_wind_power()
        load_w  = self._compute_house_load(utc_now)
        batt_w, grid_w = self._compute_dispatch(pv_w, wind_w, load_w, utc_now)

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
        """Physics-based PV power model (UAH timezone ~ UTC+2/3)."""
        hour = utc_now.hour + utc_now.minute / 60.0 + 2.0   # approximate local time
        if hour < 5.5 or hour > 20.5:
            return 0.0

        # Clearsky irradiance: simple sinusoidal model
        day_fraction = (hour - 5.5) / 15.0         # 0→1 sunrise to sunset
        sin_val      = math.sin(math.pi * day_fraction) ** 1.15
        clearsky_w   = self.cfg.pv_peak_kw * 1000.0 * sin_val

        # Cloud cover attenuation
        attenuation = 1.0 - 0.85 * self._cloud_cover
        # Temperature derating (panels lose ~0.4%/°C above 25°C NOCT)
        panel_temp   = max(25.0, 20.0 + 0.02 * clearsky_w / 100)
        temp_derating = max(0.8, 1.0 - 0.004 * (panel_temp - 25.0))

        power = clearsky_w * attenuation * temp_derating * self.cfg.inverter_efficiency
        noise = random.gauss(0, max(30, power * 0.02))
        return max(0.0, power + noise)

    def _compute_wind_power(self) -> float:
        """Wind turbine power (cubic law with cut-in/cut-out)."""
        ws = self._wind_speed_ms
        p_rated = self.cfg.wind_peak_kw * 1000.0
        v_cutin, v_rated, v_cutout = 3.0, 12.0, 25.0
        if ws < v_cutin or ws > v_cutout:
            return 0.0
        if ws >= v_rated:
            frac = 1.0
        else:
            frac = ((ws - v_cutin) / (v_rated - v_cutin)) ** 3
        noise = random.gauss(0, p_rated * 0.03)
        return max(0.0, p_rated * frac + noise)

    def _compute_house_load(self, utc_now: datetime) -> float:
        """Residential load profile for Ukrainian household."""
        hour = utc_now.hour + utc_now.minute / 60.0 + 2.0   # local time approx

        base       = self.cfg.base_load_w
        # Morning peak 7–9 local
        morning    = 1500.0 * math.exp(-0.5 * ((hour - 8.0) / 1.0) ** 2)
        # Lunch 12–13
        lunch      = 300.0  * math.exp(-0.5 * ((hour - 12.5) / 0.5) ** 2)
        # Evening peak 18–22
        evening    = 2000.0 * math.exp(-0.5 * ((hour - 20.0) / 2.0) ** 2)
        # Night lull
        night_dip  = -200.0 * math.exp(-0.5 * ((hour - 3.0) / 2.0) ** 2)

        total = base + morning + lunch + evening + night_dip
        noise = random.gauss(0, 80.0)
        return max(100.0, total + noise)

    def _compute_dispatch(self, pv_w: float, wind_w: float,
                          load_w: float, utc_now: datetime
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
            # Island mode: battery must cover deficit
            if surplus >= 0:
                # Charge battery with surplus
                batt_w = min(surplus, batt_max_chg_w)
            else:
                # Discharge to cover load
                batt_w = max(surplus, -batt_max_dis_w)
            return batt_w, 0.0

        if self._mode == "SOLAR_PRIORITY":
            if surplus > 0:
                batt_w  = min(surplus, batt_max_chg_w)
                grid_w  = surplus - batt_w   # export remainder
                grid_w  = -grid_w            # export is negative
            else:
                batt_w  = max(surplus, -batt_max_dis_w)
                grid_w  = -(surplus - batt_w)  # import remainder

        elif self._mode == "GRID_CHARGE":
            chg_w   = batt_max_chg_w * self._charge_pct / 100.0
            batt_w  = min(chg_w, batt_max_chg_w)
            grid_w  = load_w + batt_w - total_gen  # import = load + charge - gen

        elif self._mode == "DISCHARGE_SELL":
            dis_w   = batt_max_dis_w * self._discharge_pct / 100.0
            batt_w  = -min(dis_w, batt_max_dis_w)
            # Export: gen + discharge - load
            export  = total_gen + abs(batt_w) - load_w
            grid_w  = -max(0.0, export)  # negative = export

        elif self._mode == "BACKUP_MODE":
            if surplus >= 0:
                batt_w = min(surplus, batt_max_chg_w)
                grid_w = 0.0
            else:
                batt_w = max(surplus, -batt_max_dis_w)
                grid_w = max(0.0, -(surplus - batt_w))  # import only if batt can't cover

        else:   # IDLE
            batt_w = 0.0
            grid_w = load_w - total_gen  # import/export to balance

        # SoC limits hard enforcement
        if batt_w > 0 and self._soc_pct >= self.cfg.soc_max_pct:
            batt_w = 0.0

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
        """Slow random walk for cloud cover; OU process for wind speed."""
        # Cloud cover: slow mean-reverting walk
        mu_cloud = 0.3   # 30% typical overcast
        self._cloud_cover += (mu_cloud - self._cloud_cover) * dt_s / 3600.0
        self._cloud_cover += random.gauss(0, 0.01) * math.sqrt(dt_s)
        self._cloud_cover  = max(0.0, min(1.0, self._cloud_cover))

        # Wind speed: Ornstein-Uhlenbeck
        dW = random.gauss(0, 1) * math.sqrt(dt_s)
        self._wind_speed_ms += (
            self._wind_ou_theta * (self._wind_ou_mu - self._wind_speed_ms) * dt_s
            + self._wind_ou_sigma * dW
        )
        self._wind_speed_ms = max(0.0, self._wind_speed_ms)

    def _evolve_grid(self, dt_s: float) -> None:
        """Simulate grid voltage/frequency fluctuations and rare outages."""
        if self._grid_fault_timer > 0:
            self._grid_fault_timer -= dt_s
            if self._grid_fault_timer <= 0:
                self._grid_connected = True
                self._set_fault(FAULT_GRID_OUTAGE, False)
                log.info("Grid reconnected after simulated outage")
            return

        # Small fluctuations
        self._grid_v  += random.gauss(0, 0.3) * math.sqrt(dt_s / 10)
        self._grid_hz += random.gauss(0, 0.005) * math.sqrt(dt_s / 10)
        # Mean-revert to nominal
        self._grid_v  += (220.0 - self._grid_v)  * dt_s / 600
        self._grid_hz += (50.0  - self._grid_hz) * dt_s / 60
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
