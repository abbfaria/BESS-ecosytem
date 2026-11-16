"""
Shared economics formulas — used by both the live sensor loop
(EdgeOrchestrator._sensor_loop in main.py) and the cloud's history backfill
(cloud/api/src/history.py), so realized revenue is computed identically
whether the number came from a live tick or a replayed historical one.
"""

from __future__ import annotations


def compute_revenue_uah(grid_power_w: float, interval_s: float,
                         price_uah_mwh: float,
                         battery_power_w: float = 0.0,
                         inverter_mode: str = "") -> float:
    """Realized BESS revenue/cost for one interval:
    - Grid export (-grid_power_w) earns revenue at the DAM price.
    - Grid import for battery charging (GRID_CHARGE) incurs charging cost.
    - Baseline household load import is excluded from BESS asset P&L."""
    dt_h = interval_s / 3600.0
    if grid_power_w < 0:
        export_kwh = abs(grid_power_w) * dt_h / 1000.0
        return round(export_kwh * price_uah_mwh / 1000.0, 4)
    if inverter_mode == "GRID_CHARGE" and battery_power_w > 0:
        grid_chg_kwh = min(grid_power_w, battery_power_w) * dt_h / 1000.0
        return round(-grid_chg_kwh * price_uah_mwh / 1000.0, 4)
    return 0.0
