"""
Shared economics formulas — used by both the live sensor loop
(EdgeOrchestrator._sensor_loop in main.py) and the cloud's history backfill
(cloud/api/src/history.py), so realized revenue is computed identically
whether the number came from a live tick or a replayed historical one.
"""

from __future__ import annotations


def compute_revenue_uah(grid_power_w: float, interval_s: float,
                         price_uah_mwh: float) -> float:
    """Realized revenue/cost for one interval: the DAM price in effect for
    the current hour, applied to the energy actually exchanged with the
    grid (not the battery — PV-fed charging isn't a grid cost).
    + grid_power_w = import (cost), − grid_power_w = export (revenue)."""
    energy_kwh = grid_power_w * (interval_s / 3600.0) / 1000.0
    return round(-energy_kwh * price_uah_mwh / 1000.0, 4)
