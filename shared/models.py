"""
Shared Pydantic models for Edge ↔ Cloud message contracts.
Both sides must agree on these schemas.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field, field_validator
import uuid


# ── Enumerations ──────────────────────────────────────────────────────────

class OperatingMode(str, Enum):
    SOLAR_PRIORITY   = "SOLAR_PRIORITY"    # Use solar first, grid as backup
    GRID_CHARGE      = "GRID_CHARGE"       # Charge battery from grid (cheap hours)
    DISCHARGE_SELL   = "DISCHARGE_SELL"    # Discharge to grid (expensive hours)
    BACKUP_MODE      = "BACKUP_MODE"       # Island / off-grid mode
    IDLE             = "IDLE"              # No active charging/discharging

class GridStatus(str, Enum):
    CONNECTED    = "CONNECTED"
    DISCONNECTED = "DISCONNECTED"
    UNSTABLE     = "UNSTABLE"

class DeviceHealthStatus(str, Enum):
    HEALTHY  = "HEALTHY"
    WARNING  = "WARNING"
    CRITICAL = "CRITICAL"
    OFFLINE  = "OFFLINE"


# ── Telemetry (Edge → Cloud, ~every 10 s) ────────────────────────────────

class TelemetryPayload(BaseModel):
    """Uplink telemetry published by the edge device."""
    ts:               datetime = Field(...,  description="UTC timestamp (ISO-8601)")
    device_id:        str      = Field(...,  description="Unique device identifier")
    sequence_num:     int      = Field(0,    description="Monotonically increasing counter")

    # Power flows (W, positive = production/charging, negative = consumption/discharging)
    pv_power_w:       float    = Field(0.0,  ge=0,     description="PV generation (W)")
    wind_power_w:     float    = Field(0.0,  ge=0,     description="Wind generation (W)")
    battery_power_w:  float    = Field(0.0,             description="+charge / -discharge (W)")
    grid_power_w:     float    = Field(0.0,             description="+import / -export (W)")
    house_load_w:     float    = Field(0.0,  ge=0,     description="Local consumption (W)")

    # Battery state
    battery_soc_pct:  float    = Field(50.0, ge=0, le=100, description="State of Charge (%)")
    battery_soh_pct:  float    = Field(100.0,ge=0, le=100, description="State of Health (%)")
    battery_temp_c:   float    = Field(25.0,             description="Battery temperature (°C)")
    battery_voltage_v:float    = Field(51.2,             description="Battery pack voltage (V)")
    battery_current_a:float    = Field(0.0,              description="Battery current (A)")

    # Grid measurements
    grid_status:      GridStatus = Field(GridStatus.CONNECTED)
    grid_voltage_v:   float      = Field(220.0, description="Grid voltage RMS (V)")
    grid_frequency_hz:float      = Field(50.0,  description="Grid frequency (Hz)")

    # Inverter
    inverter_temp_c:  float    = Field(35.0,             description="Inverter temperature (°C)")
    inverter_mode:    OperatingMode = Field(OperatingMode.SOLAR_PRIORITY)

    # Meter totals
    total_pv_energy_kwh:      float = Field(0.0, ge=0)
    total_grid_import_kwh:    float = Field(0.0, ge=0)
    total_grid_export_kwh:    float = Field(0.0, ge=0)

    fault_code:       int      = Field(0,    description="0 = no fault")

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}


# ── Device Status (Edge → Cloud, on change + heartbeat) ──────────────────

class DeviceStatusPayload(BaseModel):
    ts:          datetime
    device_id:   str
    health:      DeviceHealthStatus
    uptime_s:    int = Field(0, ge=0)
    fw_version:  str = "1.0.0"
    ip_address:  Optional[str] = None
    mqtt_buffer_count: int = Field(0, ge=0, description="Buffered unsent messages")
    active_mode: OperatingMode = OperatingMode.SOLAR_PRIORITY
    last_schedule_ts: Optional[datetime] = None
    cloud_connected: bool = True

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}


# ── Schedule Slot (Cloud → Edge) ─────────────────────────────────────────

class HourSlot(BaseModel):
    hour:          int          = Field(..., ge=0, le=23)
    mode:          OperatingMode
    charge_power_pct:   float  = Field(0.0,  ge=0, le=100)
    discharge_power_pct:float  = Field(0.0,  ge=0, le=100)
    price_uah_mwh: float       = Field(0.0,  ge=0)
    expected_revenue_uah: float = Field(0.0)


class SchedulePayload(BaseModel):
    schedule_id:    str      = Field(default_factory=lambda: str(uuid.uuid4()))
    generated_ts:   datetime
    valid_date:     str      = Field(..., description="YYYY-MM-DD")
    device_id:      str
    slots:          list[HourSlot] = Field(default_factory=list)
    total_expected_revenue_uah: float = Field(0.0)
    optimizer_version: str = "greedy-v1"

    @field_validator("slots")
    @classmethod
    def validate_slots_count(cls, v):
        if v and len(v) != 24:
            raise ValueError(f"Schedule must have exactly 24 slots, got {len(v)}")
        return v

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}


# ── Command (Cloud → Edge, ad-hoc override) ──────────────────────────────

class CommandPayload(BaseModel):
    cmd_id:       str      = Field(default_factory=lambda: str(uuid.uuid4()))
    ts:           datetime
    device_id:    str
    mode:         OperatingMode
    power_pct:    float    = Field(0.0, ge=0, le=100)
    duration_min: int      = Field(60,  ge=1, le=1440)
    issued_by:    str      = "cloud-api"

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}


class CommandAckPayload(BaseModel):
    cmd_id:         str
    ts:             datetime
    device_id:      str
    accepted:       bool
    achieved_mode:  Optional[OperatingMode] = None
    actual_soc_pct: Optional[float] = None
    error_msg:      Optional[str] = None

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}


# ── Market Data ───────────────────────────────────────────────────────────

class HourlyPrice(BaseModel):
    hour:        int   = Field(..., ge=0, le=23)
    price_uah_mwh: float = Field(..., ge=0)
    source:      str  = "dam"


class MarketDataPayload(BaseModel):
    ts:          datetime
    valid_date:  str      = Field(..., description="YYYY-MM-DD")
    prices:      list[HourlyPrice]
    currency:    str      = "UAH"
    unit:        str      = "MWh"
    source:      str      = "operatormarket.ua"
    is_fallback: bool     = False

    @field_validator("prices")
    @classmethod
    def validate_prices(cls, v):
        if len(v) != 24:
            raise ValueError(f"Need 24 hourly prices, got {len(v)}")
        return v

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}


# ── Fault / Alert ────────────────────────────────────────────────────────

class FaultPayload(BaseModel):
    ts:        datetime
    device_id: str
    fault_code: int
    severity:  str   = "WARNING"
    message:   str
    component: str   = "unknown"

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}


# ── LWT (Last Will and Testament) ────────────────────────────────────────

class LWTPayload(BaseModel):
    ts:        datetime
    device_id: str
    status:    str = "offline"

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}
