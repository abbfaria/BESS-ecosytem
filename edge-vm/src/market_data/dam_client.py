"""
Ukrainian DAM (РДН — Ринок на Добу Наперед) price fetcher.

Source priority:
  1. operatormarket.ua  — official NEC Ukrenergo market portal
  2. ENTSO-E Transparency Platform  — EU energy data (fallback)
  3. Static fallback matrix  (offline operation)
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

# Ukrainian Transmission System Operator market portal
_OPERATOR_MARKET_URL = (
    "https://www.operatormarket.ua/pricechart/ajax/"
    "?action=getMarketData&market=dam"
)

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
    ) -> None:
        self._entsoe_token  = entsoe_token
        self._cache_ttl_s   = cache_ttl_min * 60
        self._cache: dict[str, tuple[list[float], float]] = {}   # date -> (prices, expiry_ts)

    async def get_prices(self, for_date: Optional[date] = None) -> list[float]:
        """Fetch 24 hourly prices [UAH/MWh] for the given date (default: tomorrow)."""
        if for_date is None:
            for_date = date.today() + timedelta(days=1)

        key = for_date.isoformat()
        import time
        if key in self._cache and time.time() < self._cache[key][1]:
            log.debug("DAM price cache hit", date=key)
            return self._cache[key][0]

        prices = await self._fetch_operator_market(for_date)
        if prices is None:
            log.warning("operatormarket.ua unavailable, trying ENTSO-E", date=key)
            prices = await self._fetch_entsoe(for_date)
        if prices is None:
            log.warning("ENTSO-E unavailable, using static fallback", date=key)
            prices = get_fallback_prices(for_date)
            self._cache[key] = (prices, time.time() + 3600)   # shorter TTL for fallback
            return prices

        prices = [min(p, _DAM_PRICE_CAP_UAH_MWH) for p in prices]
        self._cache[key] = (prices, time.time() + self._cache_ttl_s)
        log.info("DAM prices fetched", date=key, min=min(prices), max=max(prices))
        return prices

    # ── Source 1: operatormarket.ua ──────────────────────────────────────────

    async def _fetch_operator_market(self, for_date: date) -> Optional[list[float]]:
        if not _AIOHTTP:
            return None
        try:
            params = {
                "date": for_date.strftime("%Y-%m-%d"),
                "priceType": "mwh",
            }
            async with aiohttp.ClientSession() as sess:
                async with sess.get(
                    _OPERATOR_MARKET_URL,
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=_HTTP_TIMEOUT_S),
                    headers={"Accept": "application/json"},
                ) as resp:
                    if resp.status != 200:
                        log.debug("operatormarket HTTP error", status=resp.status)
                        return None
                    raw = await resp.json(content_type=None)
                    return self._parse_operator_market(raw)
        except Exception as exc:
            log.debug("operatormarket.ua fetch failed", exc=str(exc))
            return None

    def _parse_operator_market(self, raw: dict) -> Optional[list[float]]:
        """Parse the operatormarket.ua JSON response into 24 UAH/MWh values."""
        try:
            # The API returns a list of period objects or a data key
            data = raw.get("data") or raw.get("items") or raw
            if isinstance(data, list) and len(data) >= 24:
                prices = []
                for item in data[:24]:
                    if isinstance(item, (int, float)):
                        prices.append(float(item))
                    elif isinstance(item, dict):
                        v = item.get("price") or item.get("value") or item.get("p")
                        prices.append(float(v))
                return prices if len(prices) == 24 else None
        except Exception as exc:
            log.debug("operatormarket parse error", exc=str(exc))
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
