"""
Ukrainian DAM (РДН — Ринок на Добу Наперед) price fetcher.

Source priority:
  1. oree.com.ua  — the real market operator site (ОРЕ). Its price table is
     loaded client-side and protected against plain scripted requests, so a
     direct fetch from here typically fails without a real browser — the
     reliable path for real oree.com.ua data is the cloud's headless-browser
     fetcher pushing prices down over MQTT (see main.py's `_apply_market_push`,
     which calls `SQLiteStore.save_dam_prices(..., source="cloud")`). This
     direct attempt is kept as a second, independent chance at the same
     real source when the edge has direct internet access.
  2. ENTSO-E Transparency Platform  — real EU-published day-ahead prices for
     Ukraine's price area (needs a free API token)
  3. Historical real-data average — the mean of this device's own recently
     *fetched* (non-fabricated) curves, cached locally in SQLite. Falls back
     to a static illustrative matrix only if no real data has ever been
     observed yet (true cold start).
"""

from __future__ import annotations

import asyncio
import json
import re
from datetime import date, datetime, timedelta, timezone
from typing import Optional

try:
    import aiohttp  # type: ignore
    _AIOHTTP = True
except ImportError:
    _AIOHTTP = False

from .fallback_data import get_fallback_prices
from ..utils.logging_config import get_logger

log = get_logger(__name__)

# Real Ukrainian market operator (ОРЕ) DAM price table endpoint.
_OREE_URL = "https://www.oree.com.ua/index.php/pricectr/data_view"

# ENTSO-E Transparency Platform (Day-Ahead Prices, area 10YUA-WEPS-----W)
_ENTSOE_URL = (
    "https://web-api.tp.entsoe.eu/api"
    "?documentType=A44"
    "&in_Domain=10YUA-WEPS-----W"
    "&out_Domain=10YUA-WEPS-----W"
    "&periodStart={start}&periodEnd={end}"
    "&securityToken={token}"
)

# Ukrainian price cap (NKREKP regulation, as of 2024)
_DAM_PRICE_CAP_UAH_MWH = 15_000.0
_HTTP_TIMEOUT_S = 10


class DAMClient:
    """Async DAM price fetcher with retry, caching, and fallback chain."""

    def __init__(
        self,
        entsoe_token: str = "",
        cache_ttl_min: int = 30,
        store: "Optional[object]" = None,
    ) -> None:
        self._entsoe_token  = entsoe_token
        self._cache_ttl_s   = cache_ttl_min * 60
        self._cache: dict[str, tuple[list[float], float]] = {}   # date -> (prices, expiry_ts)
        # SQLiteStore, for real-data history caching/fallback. Optional so
        # the client stays testable/usable standalone.
        self._store = store

    async def get_prices(self, for_date: Optional[date] = None) -> list[float]:
        """Fetch 24 hourly prices [UAH/MWh] for the given date (default: tomorrow)."""
        if for_date is None:
            for_date = date.today() + timedelta(days=1)

        key = for_date.isoformat()
        import time
        if key in self._cache and time.time() < self._cache[key][1]:
            log.debug("DAM price cache hit", date=key)
            return self._cache[key][0]

        source = "oree"
        prices = await self._fetch_oree(for_date)
        if prices is None:
            log.warning("oree.com.ua unavailable directly, trying ENTSO-E", date=key)
            source = "entsoe"
            prices = await self._fetch_entsoe(for_date)
        if prices is None:
            log.warning("ENTSO-E unavailable, trying real-data history", date=key)
            source = "history"
            prices = self._fetch_history_fallback()
        if prices is None:
            log.warning("No real price history yet, using static fallback", date=key)
            prices = get_fallback_prices(for_date)
            self._cache[key] = (prices, time.time() + 3600)   # shorter TTL for fallback
            return prices

        prices = [min(p, _DAM_PRICE_CAP_UAH_MWH) for p in prices]
        self._cache[key] = (prices, time.time() + self._cache_ttl_s)
        if self._store is not None and source in ("oree", "entsoe"):
            try:
                self._store.save_dam_prices(key, prices, source)
            except Exception as exc:
                log.debug("Failed to cache DAM prices", exc=str(exc))
        log.info("DAM prices fetched", date=key, source=source,
                 min=min(prices), max=max(prices))
        return prices

    def _fetch_history_fallback(self) -> Optional[list[float]]:
        """Average of this device's own recently *fetched* real curves —
        a data-driven fallback, not an invented one. None if no real data
        has ever been observed (true cold start)."""
        if self._store is None:
            return None
        try:
            curves = self._store.get_real_price_history(max_days=14)
        except Exception as exc:
            log.debug("Failed to read DAM price history", exc=str(exc))
            return None
        if not curves:
            return None
        return [
            round(sum(curve[h] for curve in curves) / len(curves), 2)
            for h in range(24)
        ]

    # ── Source 1: oree.com.ua (real market operator) ─────────────────────────

    async def _fetch_oree(self, for_date: date) -> Optional[list[float]]:
        """Best-effort direct fetch. oree.com.ua's price table is loaded
        client-side and guarded against plain scripted requests, so this
        commonly returns None even with correct parameters — that's expected
        and not a bug here. The reliable path for real oree.com.ua data is
        the cloud's headless-browser fetcher, which pushes prices down over
        MQTT (see EdgeOrchestrator._apply_market_push in main.py); this is
        kept as an independent second chance when the edge has direct
        internet access and the site permits it."""
        if not _AIOHTTP:
            return None
        try:
            date_str = for_date.strftime("%Y-%m-%d")
            async with aiohttp.ClientSession() as sess:
                async with sess.post(
                    _OREE_URL,
                    data={"date": date_str, "market": "1"},
                    timeout=aiohttp.ClientTimeout(total=_HTTP_TIMEOUT_S),
                    headers={
                        "X-Requested-With": "XMLHttpRequest",
                        "Referer": "https://www.oree.com.ua/index.php/pricectr",
                    },
                ) as resp:
                    if resp.status != 200:
                        log.debug("oree.com.ua HTTP error", status=resp.status)
                        return None
                    raw = await resp.json(content_type=None)
                    return self._parse_oree_table(raw.get("content", ""), for_date)
        except Exception as exc:
            log.debug("oree.com.ua fetch failed", exc=str(exc))
            return None

    def _parse_oree_table(self, html: str, for_date: date) -> Optional[list[float]]:
        """Parse the row for `for_date` out of oree.com.ua's price table HTML."""
        try:
            date_str = for_date.strftime("%d.%m.%Y")
            row_match = re.search(
                re.escape(f">{date_str}<") + r"(.*?)</tr>", html, re.DOTALL
            )
            if not row_match:
                return None
            cells = re.findall(r">([\d]+\.[\d]+)<", row_match.group(1))
            if len(cells) != 24:
                return None
            return [float(c) for c in cells]
        except Exception as exc:
            log.debug("oree.com.ua parse error", exc=str(exc))
            return None

    # ── Source 2: ENTSO-E Transparency Platform ──────────────────────────────

    async def _fetch_entsoe(self, for_date: date) -> Optional[list[float]]:
        if not _AIOHTTP or not self._entsoe_token:
            return None
        try:
            start = for_date.strftime("%Y%m%d0000")
            end   = (for_date + timedelta(days=1)).strftime("%Y%m%d0000")
            url   = _ENTSOE_URL.format(
                start=start, end=end, token=self._entsoe_token
            )
            async with aiohttp.ClientSession() as sess:
                async with sess.get(
                    url,
                    timeout=aiohttp.ClientTimeout(total=_HTTP_TIMEOUT_S * 2),
                ) as resp:
                    if resp.status != 200:
                        return None
                    xml_text = await resp.text()
                    return self._parse_entsoe_xml(xml_text)
        except Exception as exc:
            log.debug("ENTSO-E fetch failed", exc=str(exc))
            return None

    def _parse_entsoe_xml(self, xml_text: str) -> Optional[list[float]]:
        """Extract hourly prices from ENTSO-E Publication_MarketDocument XML."""
        try:
            # Minimal regex-based parse (avoids lxml dependency on edge)
            points = re.findall(
                r"<Point>.*?<position>(\d+)</position>.*?<price\.amount>([0-9.]+)</price\.amount>.*?</Point>",
                xml_text,
                re.DOTALL,
            )
            if len(points) < 24:
                return None
            prices_by_pos: dict[int, float] = {}
            for pos_str, price_str in points:
                prices_by_pos[int(pos_str)] = float(price_str)
            if len(prices_by_pos) < 24:
                return None
            # ENTSO-E prices are EUR/MWh; convert to UAH at ~40 UAH/EUR
            eur_to_uah = 40.0
            return [
                round(prices_by_pos[i + 1] * eur_to_uah, 1)
                for i in range(24)
            ]
        except Exception as exc:
            log.debug("ENTSO-E parse error", exc=str(exc))
            return None

    # ── Utility ───────────────────────────────────────────────────────────────

    def get_peak_valley_hours(self, prices: list[float]) -> dict[str, list[int]]:
        """Identify cheap and expensive hours for MILP optimizer input."""
        avg      = sum(prices) / 24
        high_thr = avg * 1.3
        low_thr  = avg * 0.7
        return {
            "peak":    [h for h, p in enumerate(prices) if p > high_thr],
            "valley":  [h for h, p in enumerate(prices) if p < low_thr],
            "average_uah_mwh": round(avg, 1),
        }
