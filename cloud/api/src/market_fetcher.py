"""
Real Ukrainian DAM (РДН) price fetcher — headless-browser scraper for
oree.com.ua, the actual market operator site.

Why a browser and not a plain HTTP client: the site's hourly price table is
loaded client-side via an internal AJAX call, and that endpoint returns an
empty "no data" response to plain scripted requests regardless of correct
parameters, a real session cookie, or browser-matching headers — verified
directly against the live site. A real browser executing the page's own JS
gets through fine, which is what this module does with headless Chromium.

Runs on the cloud VM (not the resource-constrained edge) on a daily
schedule, and publishes the result down to edge over the existing MQTT
market-data channel (see EdgeOrchestrator._apply_market_push in
edge/src/main.py) rather than requiring edge to run a browser itself.
"""

from __future__ import annotations

import re
from datetime import date
from typing import Optional

from .logging_config import get_logger

log = get_logger(__name__)

try:
    from playwright.async_api import async_playwright  # type: ignore
    _PLAYWRIGHT_OK = True
except ImportError:
    _PLAYWRIGHT_OK = False

_OREE_PRICE_PAGE = "https://www.oree.com.ua/index.php/pricectr"
_PAGE_TIMEOUT_MS = 30_000


async def fetch_dam_prices(for_date: date) -> Optional[list[float]]:
    """Fetch 24 real hourly DAM prices [UAH/MWh] for `for_date` from
    oree.com.ua. Returns None if the site is unreachable, the row for that
    date isn't in the currently displayed month (a page for a different
    month would need date-picker navigation, not implemented), or the row
    can't be parsed — callers should treat that as "try the next source".
    """
    if not _PLAYWRIGHT_OK:
        log.warning("playwright not installed — oree.com.ua fetch disabled")
        return None

    target = for_date.strftime("%d.%m.%Y")

    try:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                args=["--disable-dev-shm-usage", "--no-sandbox"]
            )
            try:
                page = await browser.new_page()
                await page.goto(_OREE_PRICE_PAGE, timeout=_PAGE_TIMEOUT_MS,
                                 wait_until="domcontentloaded")
                await page.wait_for_selector(
                    "#price_table tbody tr", timeout=_PAGE_TIMEOUT_MS
                )

                rows = await page.locator("#price_table tbody tr").all()
                for row in rows:
                    cells = await row.locator("td").all_inner_texts()
                    if not cells or cells[0].strip() != target:
                        continue
                    values = cells[1:25]
                    if len(values) != 24:
                        log.warning("oree.com.ua row has unexpected cell count",
                                    date=target, cells=len(values))
                        return None
                    try:
                        return [float(re.sub(r"[^\d.]", "", v)) for v in values]
                    except ValueError:
                        log.warning("oree.com.ua row has non-numeric cell",
                                    date=target, raw=values)
                        return None

                log.info("Date not found in current month view on oree.com.ua "
                         "(month navigation not implemented)", date=target)
                return None
            finally:
                await browser.close()
    except Exception as exc:
        log.warning("oree.com.ua headless fetch failed", exc=str(exc))
        return None
