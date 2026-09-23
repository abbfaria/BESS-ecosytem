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

# How many distinct dates' schedules to retain at once. The edge only ever
# needs "today" (already active) and "tomorrow" (computed once real prices
# for it arrive, which routinely happens hours before midnight — see
# _apply_market_push in main.py) simultaneously; a small margin above that
# bounds memory/disk without ever evicting a date still in use.
_MAX_STORED_SCHEDULES = 3


class FallbackController:
    """Manages schedule persistence and autonomous fallback logic.

    Schedules are stored keyed by their `valid_date`, not as a single
    overwritable slot. That distinction is load-bearing: the cloud
    routinely pushes tomorrow's real-price schedule during the afternoon
    (as soon as DAM auction results publish), well before today is over.
    An earlier single-slot design treated that arrival as replacing
    today's still-active schedule outright — today's real, already-correct
    schedule would vanish from memory, and the newly-arrived one would
    then itself be rejected by the date check for the rest of today,
    leaving the device on the crude zero-price `_default_action()` for
    however many hours remained, including whatever evening peak-price
    window happened to fall in that gap. Keying by date means receiving
    tomorrow's schedule only adds an entry — today's stays exactly as
    usable as it was the instant before.
    """

    def __init__(self, schedule_path: Path = _FALLBACK_SCHEDULE_PATH) -> None:
        self._path        = schedule_path
        self._schedules: dict[str, list[HourlyAction]] = {}
        self._loaded_date: Optional[str] = None   # most recently saved/loaded date (diagnostic)

    def save_schedule(self, actions: list[HourlyAction], valid_date: str) -> None:
        """Persist a schedule for `valid_date`, alongside (not instead of)
        any other currently-stored dates."""
        self._schedules[valid_date] = actions
        self._loaded_date = valid_date
        self._prune()
        self._persist()

    def _prune(self) -> None:
        while len(self._schedules) > _MAX_STORED_SCHEDULES:
            oldest = min(self._schedules)
            del self._schedules[oldest]

    def _persist(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "schedules": {
                date: [
                    {
                        "hour":          a.hour,
                        "mode":          a.mode,
                        "charge_pct":    a.charge_pct,
                        "discharge_pct": a.discharge_pct,
                        "price_uah_mwh": a.price_uah_mwh,
                    }
                    for a in actions
                ]
                for date, actions in self._schedules.items()
            }
        }
        self._path.write_text(json.dumps(payload, indent=2))
        log.info("Schedule persisted", dates=sorted(self._schedules), path=str(self._path))

    def load_schedule(self) -> bool:
        """Load persisted schedule(s) from disk. Returns True if successful.

        Accepts both the current multi-date format and the older
        single-schedule format (`{"valid_date": ..., "slots": [...]}`) so
        that a device upgrading from a pre-fix image doesn't lose whatever
        it had on disk at the moment of deploy."""
        if not self._path.exists():
            return False
        try:
            data = json.loads(self._path.read_text())
            raw: dict = data["schedules"] if "schedules" in data else {data["valid_date"]: data["slots"]}
            self._schedules = {
                date: [
                    HourlyAction(
                        hour=s["hour"],
                        mode=s["mode"],
                        charge_pct=s.get("charge_pct", 0.0),
                        discharge_pct=s.get("discharge_pct", 0.0),
                        price_uah_mwh=s.get("price_uah_mwh", 0.0),
                    )
                    for s in slots
                ]
                for date, slots in raw.items()
            }
            self._prune()
            self._loaded_date = max(self._schedules) if self._schedules else None
            log.info("Schedule loaded from disk", dates=sorted(self._schedules))
            return True
        except Exception as exc:
            log.error("Failed to load schedule", exc=str(exc))
            return False

    def get_current_action(self) -> HourlyAction:
        """Return the appropriate action for the current hour, using
        *today's* stored schedule specifically — never a schedule for any
        other date, but critically also never displaced by one."""
        now   = datetime.now(timezone.utc)
        hour  = now.hour
        today = now.strftime("%Y-%m-%d")

        schedule = self._schedules.get(today)
        if schedule and len(schedule) == 24:
            return schedule[hour]

        if self._schedules:
            log.warning("No stored schedule for today", today=today,
                        stored_dates=sorted(self._schedules))
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
        return len(self._schedules) > 0

    @property
    def loaded_date(self) -> Optional[str]:
        return self._loaded_date
