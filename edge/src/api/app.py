"""
Local FastAPI management API — accessible on the edge LAN (port 8000).

Endpoints:
  GET  /health            — liveness probe
  GET  /api/v1/telemetry  — latest sensor readings
  GET  /api/v1/history    — historical telemetry (time range)
  GET  /api/v1/status     — device status and buffer stats
  GET  /api/v1/schedule   — current 24-hour schedule
  POST /api/v1/command    — ad-hoc mode override
  GET  /api/v1/events     — recent event log
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

if TYPE_CHECKING:
    from ..main import EdgeOrchestrator


class CommandRequest(BaseModel):
    mode:          str
    power_pct:     float = 0.0
    duration_min:  int   = 60


def create_app(orchestrator: "EdgeOrchestrator") -> FastAPI:
    app = FastAPI(
        title="BESS Edge API",
        description="Local management API for the BESS edge node",
        version="1.0.0",
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ── Health ────────────────────────────────────────────────────────────────

    @app.get("/health", tags=["system"])
    async def health() -> dict:
        return {"status": "ok", "ts": datetime.now(timezone.utc).isoformat()}

    # ── Telemetry ─────────────────────────────────────────────────────────────

    @app.get("/api/v1/telemetry", tags=["monitoring"])
    async def get_latest_telemetry(limit: int = Query(1, ge=1, le=100)) -> list[dict]:
        return orchestrator.get_store().get_latest_telemetry(limit)

    @app.get("/api/v1/history", tags=["monitoring"])
    async def get_history(
        from_ts: Optional[str] = Query(None, description="ISO-8601 start"),
        to_ts:   Optional[str] = Query(None, description="ISO-8601 end"),
        hours:   Optional[int] = Query(None, ge=1, le=168,
                                       description="Shorthand: last N hours"),
    ) -> list[dict]:
        if hours is not None:
            now = datetime.now(timezone.utc)
            from_ts = (now - timedelta(hours=hours)).isoformat()
            to_ts   = now.isoformat()
        if not from_ts or not to_ts:
            raise HTTPException(400, "Provide 'hours' or both 'from_ts' and 'to_ts'")
        return orchestrator.get_store().get_telemetry_range(from_ts, to_ts)

    # ── Status ────────────────────────────────────────────────────────────────

    @app.get("/api/v1/status", tags=["monitoring"])
    async def get_status() -> dict:
        buf_stats = await orchestrator.get_buffer().stats()
        db_stats  = orchestrator.get_store().get_stats()
        latest    = orchestrator.get_store().get_latest_telemetry(1)
        return {
            "ts":             datetime.now(timezone.utc).isoformat(),
            "mqtt_connected": orchestrator._mqtt.is_connected if orchestrator._mqtt else False,
            "buffer":         buf_stats,
            "db":             db_stats,
            "last_telemetry": latest[0] if latest else None,
        }

    # ── Schedule ──────────────────────────────────────────────────────────────

    @app.get("/api/v1/schedule", tags=["control"])
    async def get_schedule() -> dict:
        fc = orchestrator._fallback
        action = fc.get_current_action()
        return {
            "loaded_date":     fc.loaded_date,
            "current_hour":    datetime.now(timezone.utc).hour,
            "current_action": {
                "hour":          action.hour,
                "mode":          action.mode,
                "charge_pct":    action.charge_pct,
                "discharge_pct": action.discharge_pct,
            },
        }

    # ── Command ───────────────────────────────────────────────────────────────

    @app.post("/api/v1/command", tags=["control"], status_code=202)
    async def post_command(cmd: CommandRequest) -> dict:
        valid_modes = {"SOLAR_PRIORITY", "GRID_CHARGE", "DISCHARGE_SELL",
                       "BACKUP_MODE", "IDLE"}
        if cmd.mode not in valid_modes:
            raise HTTPException(400, f"Invalid mode. Must be one of: {valid_modes}")
        orchestrator._apply_command({
            "mode":        cmd.mode,
            "power_pct":   cmd.power_pct,
            "duration_min": cmd.duration_min,
        })
        return {"accepted": True, "mode": cmd.mode, "duration_min": cmd.duration_min}

    # ── Events ────────────────────────────────────────────────────────────────

    @app.get("/api/v1/events", tags=["monitoring"])
    async def get_events(
        limit:    int           = Query(50, ge=1, le=500),
        severity: Optional[str] = Query(None),
    ) -> list[dict]:
        return orchestrator.get_store().get_recent_events(limit, severity)

    return app
