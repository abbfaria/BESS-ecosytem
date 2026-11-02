"""
BESS dispatch optimizer.

Two-tier strategy:
  1. MILP (via PuLP/CBC)  — used when prices are available and PuLP is installed
  2. Greedy rule-based   — fallback when PuLP absent or price data missing

MILP formulation (24-hour look-ahead, 1-hour resolution):
  Variables:
    p_chg[h]  ≥ 0   charging power (kW)
    p_dis[h]  ≥ 0   discharging power (kW)
    soc[h]    ∈ [soc_min, soc_max]
    b_chg[h]  ∈ {0,1}  (charge/discharge mutual exclusion)

  Objective: maximise  Σ  price[h] * p_dis[h] * η_dis / 1000
                      − Σ  price[h] * p_chg[h] / (η_chg * 1000)

  Constraints:
    SoC dynamics, power limits, mutual exclusion,
    daily throughput limit (battery calendar ageing)
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from typing import Optional

from ..utils.logging_config import get_logger

log = get_logger(__name__)

try:
    import pulp  # type: ignore
    _PULP_OK = True
except ImportError:
    _PULP_OK = False


@dataclass
class OptimizerConfig:
    battery_capacity_kwh:     float = 20.0
    battery_charge_kw_max:    float = 5.0
    battery_discharge_kw_max: float = 5.0
    charge_efficiency:        float = 0.96
    discharge_efficiency:     float = 0.96
    soc_min_pct:              float = 10.0
    soc_max_pct:              float = 95.0
    # Daily throughput limit to reduce cycle ageing (e.g. 1 full cycle/day)
    max_daily_throughput_kwh: float = 20.0
    # Revenue threshold: only dispatch if price > this (avoid tiny arbitrage)
    min_spread_uah_mwh:       float = 500.0
    # State of charge the schedule must leave in the battery at hour 23.
    # Without this the horizon ends at midnight with no value attached to
    # stored energy, so the optimum is always to sell the pack down to
    # soc_min before the day ends — which then starves the *next* morning's
    # peak, day after day. Requiring a terminal reserve makes consecutive
    # single-day optimisations behave like a rolling schedule.
    terminal_soc_pct:         float = 50.0


@dataclass
class HourlyAction:
    hour:             int
    mode:             str          # SOLAR_PRIORITY / GRID_CHARGE / DISCHARGE_SELL / IDLE
    charge_pct:       float = 0.0  # 0–100% of max charge power
    discharge_pct:    float = 0.0  # 0–100% of max discharge power
    price_uah_mwh:    float = 0.0
    expected_revenue: float = 0.0  # UAH


class BESSOptimizer:
    """24-hour dispatch scheduler."""

    def __init__(self, cfg: OptimizerConfig) -> None:
        self._cfg = cfg
        # Which method actually produced the most recent schedule. Callers
        # record this as the schedule's provenance; it must never be
        # hardcoded, because the whole point of the greedy path is that it
        # engages silently when MILP cannot run (a hardcoded "milp-v1" label
        # is exactly what hid the PuLP incompatibility for so long).
        self.last_method: str = "none"

    def optimize(
        self,
        prices_uah_mwh: list[float],
        current_soc_pct: float,
        gen_forecast_kw:  Optional[list[float]] = None,
        load_forecast_kw: Optional[list[float]] = None,
    ) -> list[HourlyAction]:
        """Return 24 HourlyAction objects for the next day.

        Supplying both forecasts (24 UTC-indexed hourly values, in kW)
        switches the objective from pure battery arbitrage to the site's
        **net grid cash flow** — the same quantity the dashboard measures as
        revenue, and the one the operator actually banks.

        That distinction turned out to matter, not just to be tidier. With
        the house load outside the model, the optimizer could not see that
        an empty pack forces the dwelling onto the grid: it would happily
        sell the battery down to soc_min during the evening peak and leave
        the late-evening hours — still priced at 5 000–9 000 UAH/MWh — to be
        bought back at retail volumes. Measured over 2026-10-11 those hours
        ran at a loss of roughly 17 UAH against a 87 UAH day, while the
        schedule that produced them was reported as a ~160 UAH success,
        because planner and dashboard were scoring different things.

        Both forecasts are expected to be noise-free expectations of the
        plant's own physics (BESSEmulator.forecast_day), not a separate
        model of it. Omitting them preserves the original arbitrage-only
        objective, which the greedy fallback below still uses.
        """
        assert len(prices_uah_mwh) == 24
        co_opt = gen_forecast_kw is not None and load_forecast_kw is not None
        if co_opt:
            assert len(gen_forecast_kw) == 24 and len(load_forecast_kw) == 24

        if _PULP_OK:
            try:
                actions = self._milp_optimize(
                    prices_uah_mwh, current_soc_pct,
                    gen_forecast_kw if co_opt else None,
                    load_forecast_kw if co_opt else None,
                )
                # _milp_optimize sets last_method itself — either to the
                # variant it solved ("milp-v1" / "milp-netflow-v2") or to
                # "greedy-v1" when it fell back internally on an infeasible
                # model. Re-stamping it here would erase that distinction.
                return actions
            except Exception as exc:
                log.warning("MILP failed, falling back to greedy", exc=str(exc))

        self.last_method = "greedy-v1"
        return self._greedy_optimize(prices_uah_mwh, current_soc_pct)

    # ── MILP optimizer ────────────────────────────────────────────────────────

    def _milp_optimize(
        self,
        prices: list[float],
        soc0: float,
        gen:  Optional[list[float]] = None,
        load: Optional[list[float]] = None,
    ) -> list[HourlyAction]:
        cfg = self._cfg
        cap  = cfg.battery_capacity_kwh
        soc_lo = cfg.soc_min_pct / 100.0 * cap
        soc_hi = cfg.soc_max_pct / 100.0 * cap
        p_chg_max = cfg.battery_charge_kw_max
        p_dis_max = cfg.battery_discharge_kw_max
        eta_c = cfg.charge_efficiency
        eta_d = cfg.discharge_efficiency
        # PuLP's LpVariable doesn't support LpVariable / float (only
        # float / int * LpVariable style scaling), so divisions by a
        # round-trip efficiency are expressed as multiplication by the
        # reciprocal instead.
        inv_eta_c = 1.0 / eta_c
        inv_eta_d = 1.0 / eta_d

        prob = pulp.LpProblem("BESS_DAM", pulp.LpMaximize)

        p_chg = [pulp.LpVariable(f"chg_{h}", 0, p_chg_max) for h in range(24)]
        p_dis = [pulp.LpVariable(f"dis_{h}", 0, p_dis_max) for h in range(24)]
        soc   = [pulp.LpVariable(f"soc_{h}", soc_lo, soc_hi) for h in range(24)]
        b_chg = [pulp.LpVariable(f"bc_{h}", cat="Binary") for h in range(24)]

        co_opt = gen is not None and load is not None
        if co_opt:
            # Net grid cash flow. grid[h] is the site's exchange with the
            # network, free in sign: positive imports, negative exports.
            # Earning is therefore the negative of what crosses the meter,
            # priced hour by hour — identical to the dashboard's
            # -(grid_kW) x (price/1000), so the plan is denominated in the
            # same units it will later be judged in.
            grid = [pulp.LpVariable(f"grid_{h}") for h in range(24)]
            for h in range(24):
                prob += grid[h] == load[h] + p_chg[h] - p_dis[h] - gen[h]
            revenue = pulp.lpSum(
                -prices[h] / 1000.0 * grid[h] for h in range(24)
            )
        else:
            # Battery-only arbitrage: sell revenue - buy cost (UAH/h).
            grid = None
            revenue = pulp.lpSum(
                prices[h] / 1000.0 * (p_dis[h] * eta_d - p_chg[h] * inv_eta_c)
                for h in range(24)
            )
        prob += revenue

        # SoC dynamics (initial SoC)
        soc_init = soc0 / 100.0 * cap
        prob += soc[0] == soc_init + eta_c * p_chg[0] - p_dis[0] * inv_eta_d

        for h in range(1, 24):
            prob += soc[h] == soc[h-1] + eta_c * p_chg[h] - p_dis[h] * inv_eta_d

        # Mutual exclusion and power limits
        M = max(p_chg_max, p_dis_max) + 1
        for h in range(24):
            prob += p_chg[h] <= M * b_chg[h]
            prob += p_dis[h] <= M * (1 - b_chg[h])

        # Daily throughput cap
        prob += pulp.lpSum(p_chg[h] for h in range(24)) <= cfg.max_daily_throughput_kwh
        prob += pulp.lpSum(p_dis[h] for h in range(24)) <= cfg.max_daily_throughput_kwh

        # Terminal reserve — see OptimizerConfig.terminal_soc_pct. Clamped to
        # what is actually reachable from soc0 within the throughput cap so
        # the model can never be made infeasible by this constraint alone.
        reachable = soc_init + cfg.max_daily_throughput_kwh * eta_c
        terminal_kwh = min(cfg.terminal_soc_pct / 100.0 * cap, soc_hi, reachable)
        prob += soc[23] >= terminal_kwh

        solver = pulp.PULP_CBC_CMD(msg=0, timeLimit=10)
        prob.solve(solver)

        if prob.status != 1:
            log.warning("MILP infeasible/no solution — falling back to greedy",
                        status=prob.status)
            self.last_method = "greedy-v1"
            return self._greedy_optimize(prices, soc0)

        actions = []
        for h in range(24):
            chg = pulp.value(p_chg[h]) or 0.0
            dis = pulp.value(p_dis[h]) or 0.0
            if co_opt:
                rev = -prices[h] / 1000.0 * (pulp.value(grid[h]) or 0.0)
            else:
                rev = prices[h] / 1000.0 * (dis * eta_d - chg / eta_c)

            if dis > 0.1:
                mode = "DISCHARGE_SELL"
                dis_pct = dis / p_dis_max * 100.0
                chg_pct = 0.0
            elif chg > 0.1:
                mode = "GRID_CHARGE"
                dis_pct = 0.0
                chg_pct = chg / p_chg_max * 100.0
            else:
                mode = "SOLAR_PRIORITY"
                chg_pct = dis_pct = 0.0

            actions.append(HourlyAction(
                hour=h, mode=mode,
                charge_pct=round(chg_pct, 1),
                discharge_pct=round(dis_pct, 1),
                price_uah_mwh=prices[h],
                expected_revenue=round(rev, 2),
            ))

        self.last_method = "milp-netflow-v2" if co_opt else "milp-v1"

        total_rev = sum(a.expected_revenue for a in actions)
        log.info("MILP schedule computed",
                 objective="net-grid-cashflow" if co_opt else "battery-arbitrage",
                 total_revenue_uah=round(total_rev, 2),
                 discharge_hours=sum(1 for a in actions if a.mode == "DISCHARGE_SELL"),
                 charge_hours=sum(1 for a in actions if a.mode == "GRID_CHARGE"))
        return actions

    # ── Greedy optimizer ──────────────────────────────────────────────────────

    def _greedy_optimize(
        self,
        prices: list[float],
        soc0: float,
    ) -> list[HourlyAction]:
        """
        Rank hours by price. Cheapest N hours → charge, most expensive M → discharge.
        Constraint: charge hours precede corresponding discharge hours.
        """
        cfg = self._cfg
        avg   = sum(prices) / 24
        spread_ok = lambda p: abs(p - avg) > cfg.min_spread_uah_mwh / 2

        ranked = sorted(range(24), key=lambda h: prices[h])
        low_hours  = [h for h in ranked[:8]  if prices[h] < avg and spread_ok(prices[h])]
        high_hours = [h for h in ranked[-8:] if prices[h] > avg and spread_ok(prices[h])]

        # Simulate SoC through the day
        soc = soc0
        cap = cfg.battery_capacity_kwh
        soc_lo = cfg.soc_min_pct
        soc_hi = cfg.soc_max_pct
        eta_c  = cfg.charge_efficiency
        eta_d  = cfg.discharge_efficiency
        p_max  = min(cfg.battery_charge_kw_max, cfg.battery_discharge_kw_max)

        actions = []
        for h in range(24):
            if h in low_hours and soc < soc_hi:
                energy_room = (soc_hi - soc) / 100.0 * cap
                chg_kwh = min(p_max * eta_c, energy_room)
                soc += chg_kwh / cap * 100.0
                chg_pct = chg_kwh / (p_max * eta_c) * 100.0
                rev = -prices[h] / 1000.0 * chg_kwh / eta_c
                actions.append(HourlyAction(h, "GRID_CHARGE",
                                            charge_pct=round(chg_pct, 1),
                                            price_uah_mwh=prices[h],
                                            expected_revenue=round(rev, 2)))

            elif h in high_hours and soc > soc_lo:
                energy_avail = (soc - soc_lo) / 100.0 * cap
                dis_kwh = min(p_max / eta_d, energy_avail)
                soc -= dis_kwh / cap * 100.0
                dis_pct = dis_kwh / (p_max / eta_d) * 100.0
                rev = prices[h] / 1000.0 * dis_kwh * eta_d
                actions.append(HourlyAction(h, "DISCHARGE_SELL",
                                            discharge_pct=round(dis_pct, 1),
                                            price_uah_mwh=prices[h],
                                            expected_revenue=round(rev, 2)))

            else:
                actions.append(HourlyAction(h, "SOLAR_PRIORITY",
                                            price_uah_mwh=prices[h]))

        log.info("Greedy schedule computed",
                 total_revenue_uah=round(sum(a.expected_revenue for a in actions), 2))
        return actions
