"""
Fallback controller for cloud-disconnected operation.

When the MQTT link to the cloud is severed, the edge continues
autonomous operation using the last-received schedule. If no
schedule is stored, rule-based defaults maintain safe BESS operation.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from .optimizer import HourlyAction
from ..utils.logging_config import get_logger

log = get_logger(__name__)

_FALLBACK_SCHEDULE_PATH = Path("/data/last_schedule.json")


class FallbackController:
    """Manages schedule persistence and autonomous fallback logic."""

    def __init__(self, schedule_path: Path = _FALLBACK_SCHEDULE_PATH) -> None:
        self._path        = schedule_path
        self._schedule: Optional[list[HourlyAction]] = None
        self._loaded_date: Optional[str] = None

    def save_schedule(self, actions: list[HourlyAction], valid_date: str) -> None:
        """Persist schedule to disk for use after cloud disconnect."""
        self._schedule    = actions
        self._loaded_date = valid_date
        self._path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "valid_date": valid_date,
            "slots": [
                {
                    "hour":          a.hour,
                    "mode":          a.mode,
                    "charge_pct":    a.charge_pct,
                    "discharge_pct": a.discharge_pct,
                    "price_uah_mwh": a.price_uah_mwh,
                }
                for a in actions
            ],
        }
        self._path.write_text(json.dumps(payload, indent=2))
        log.info("Schedule persisted", date=valid_date, path=str(self._path))

    def load_schedule(self) -> bool:
        """Load last persisted schedule from disk. Returns True if successful."""
        if not self._path.exists():
            return False
        try:
            data = json.loads(self._path.read_text())
            self._loaded_date = data["valid_date"]
            self._schedule = [
                HourlyAction(
                    hour=s["hour"],
                    mode=s["mode"],
                    charge_pct=s.get("charge_pct", 0.0),
                    discharge_pct=s.get("discharge_pct", 0.0),
                    price_uah_mwh=s.get("price_uah_mwh", 0.0),
                )
                for s in data["slots"]
            ]
            log.info("Schedule loaded from disk",
                     date=self._loaded_date, slots=len(self._schedule))
            return True
        except Exception as exc:
            log.error("Failed to load schedule", exc=str(exc))
            return False

    def get_current_action(self) -> HourlyAction:
        """Return the appropriate action for the current hour."""
        now  = datetime.now(timezone.utc)
        hour = now.hour

        if self._schedule and len(self._schedule) == 24:
            today = now.strftime("%Y-%m-%d")
            if self._loaded_date == today:
                return self._schedule[hour]
            # Yesterday's schedule: fall through to default
            log.warning("Stored schedule is for a different date",
                        schedule_date=self._loaded_date, today=today)

        return self._default_action(hour)

    def _default_action(self, hour: int) -> HourlyAction:
        """Conservative safe defaults when no schedule is available."""
        # Night valley (00–06): gentle grid charge if SoC low is handled by caller
        # Morning/evening peaks: discharge
        # Day: solar priority
        if 0 <= hour < 6:
            return HourlyAction(hour=hour, mode="GRID_CHARGE",
                                charge_pct=50.0, price_uah_mwh=0.0)
        if hour in (7, 8, 9, 18, 19, 20, 21):
            return HourlyAction(hour=hour, mode="DISCHARGE_SELL",
                                discharge_pct=70.0, price_uah_mwh=0.0)
        return HourlyAction(hour=hour, mode="SOLAR_PRIORITY")

    @property
    def has_schedule(self) -> bool:
        return self._schedule is not None

    @property
    def loaded_date(self) -> Optional[str]:
        return self._loaded_date
