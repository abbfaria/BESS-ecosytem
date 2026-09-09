"""
Configuration and snapshot models for the BESS emulator.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class EmulatorConfig:
    device_id:               str   = "bess-edge-01"

    # PV array
    pv_peak_kw:              float = 10.0     # kW installed capacity
    inverter_efficiency:     float = 0.97

    # Wind turbine (optional, 0 to disable)
    wind_peak_kw:            float = 2.0

    # Battery pack (LiFePO4 16S, 48 V nominal)
    battery_capacity_kwh:    float = 20.0
    battery_charge_kw_max:   float = 5.0
    battery_discharge_kw_max:float = 5.0
    charge_efficiency:       float = 0.96
    discharge_efficiency:    float = 0.96
    soc_min_pct:             float = 10.0
    soc_max_pct:             float = 95.0
    initial_soc_pct:         float = 50.0

    # House load baseline
    base_load_w:             float = 400.0    # always-on loads (W)


@dataclass
class SensorSnapshot:
    """Immutable snapshot of all sensor readings at one instant."""
    ts:               datetime
    device_id:        str
    sequence_num:     int = 0

    # Power flows (W)
    pv_power_w:       float = 0.0
    wind_power_w:     float = 0.0
    battery_power_w:  float = 0.0   # + = charging, - = discharging
    grid_power_w:     float = 0.0   # + = import, - = export
    house_load_w:     float = 0.0

    # Battery
    battery_soc_pct:  float = 50.0
    battery_soh_pct:  float = 100.0
    battery_temp_c:   float = 22.0
    battery_voltage_v:float = 51.2
    battery_current_a:float = 0.0

    # Grid
    grid_status:      str   = "CONNECTED"
    grid_voltage_v:   float = 220.0
    grid_frequency_hz:float = 50.0

    # Inverter
    inverter_temp_c:  float = 30.0
    inverter_mode:    str   = "SOLAR_PRIORITY"

    # Meter accumulators
    total_pv_energy_kwh:   float = 0.0
    total_grid_import_kwh: float = 0.0
    total_grid_export_kwh: float = 0.0

    # Fault bitmask (0 = healthy)
    fault_code:       int   = 0

    def to_dict(self) -> dict:
        return {
            "ts":                self.ts.isoformat(),
            "device_id":         self.device_id,
            "sequence_num":      self.sequence_num,
            "pv_power_w":        self.pv_power_w,
            "wind_power_w":      self.wind_power_w,
            "battery_power_w":   self.battery_power_w,
            "grid_power_w":      self.grid_power_w,
            "house_load_w":      self.house_load_w,
            "battery_soc_pct":   self.battery_soc_pct,
            "battery_soh_pct":   self.battery_soh_pct,
            "battery_temp_c":    self.battery_temp_c,
            "battery_voltage_v": self.battery_voltage_v,
            "battery_current_a": self.battery_current_a,
            "grid_status":       self.grid_status,
            "grid_voltage_v":    self.grid_voltage_v,
            "grid_frequency_hz": self.grid_frequency_hz,
            "inverter_temp_c":   self.inverter_temp_c,
            "inverter_mode":     self.inverter_mode,
            "total_pv_energy_kwh":   self.total_pv_energy_kwh,
            "total_grid_import_kwh": self.total_grid_import_kwh,
            "total_grid_export_kwh": self.total_grid_export_kwh,
            "fault_code":        self.fault_code,
        }
