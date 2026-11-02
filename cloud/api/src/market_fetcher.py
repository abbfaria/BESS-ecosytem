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

import asyncio
import re
from datetime import date
from typing import Optional

from .logging_config import get_logger
from edge_src.market_data.timebase import local_day_hours, local_hours_to_utc

log = get_logger(__name__)

try:
    from playwright.async_api import async_playwright  # type: ignore
    _PLAYWRIGHT_OK = True
except ImportError:
    _PLAYWRIGHT_OK = False

_OREE_PRICE_PAGE = "https://www.oree.com.ua/index.php/pricectr"
_PAGE_TIMEOUT_MS = 30_000
# Hard ceiling for one complete fetch attempt, including browser
# startup and teardown — the parts no per-operation timeout covers.
_FETCH_TIMEOUT_S = 90

# The page opens on the month of the latest published day, so the last
# afternoon of a month already shows the *next* month, and any earlier day
# lives in another month's table. The page's own AJAX endpoint serves any
# month; it answers {"caption", "content": "<table id=price_table>..."} for
# a published month and a full HTML page otherwise. Called from inside the
# loaded page so it carries the session the endpoint insists on, once the
# page's own initial request has settled — overlapping it intermittently
# gets the HTML page back even for a published month, hence the retries.
_LOAD_MONTH_JS = """async (month) => {
    const r = await fetch('/index.php/pricectr/data_view', {
        method: 'POST',
        headers: {'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                  'X-Requested-With': 'XMLHttpRequest'},
        body: 'date=' + month + '&market=DAM&zone=IPS'});
    const text = await r.text();
    let data;
    try { data = JSON.parse(text); } catch (e) { return 'HTTP ' + r.status + ': ' + text.slice(0, 80); }
    if (!data.content || !data.content.includes('price_table')) return 'no price_table in reply';
    document.querySelector('#price_table').outerHTML = data.content;
    return 'ok';
}"""
_MONTH_ATTEMPTS = 3

_NOT_FOUND = object()


async def fetch_dam_prices(for_date: date) -> Optional[list[float]]:
    """Fetch 24 real hourly DAM prices [UAH/MWh] for `for_date` from
    oree.com.ua. Returns None if the site is unreachable, the date isn't
    published (yet), or the row can't be parsed — callers should treat that
    as "try the next source".

    The whole browser session is bounded by `_FETCH_TIMEOUT_S`. The
    per-operation timeouts below cover goto/wait_for, but nothing covered
    browser startup or `browser.close()`, and those are exactly what hangs
    when the host suspends mid-session: Chromium is killed, while the
    Python side keeps awaiting a pipe that will never answer. With no
    outer bound that await never returns, and because it is awaited from a
    long-lived background task the whole task stops silently — no
    exception, no log line, just a loop that never comes round again.
    """
    if not _PLAYWRIGHT_OK:
        log.warning("playwright not installed — oree.com.ua fetch disabled")
        return None

    try:
        return await asyncio.wait_for(
            _fetch_dam_prices_inner(for_date), timeout=_FETCH_TIMEOUT_S
        )
    except asyncio.TimeoutError:
        log.warning("oree.com.ua fetch exceeded its overall budget — abandoning",
                    date=for_date.isoformat(), budget_s=_FETCH_TIMEOUT_S)
        return None


async def _fetch_dam_prices_inner(for_date: date) -> Optional[list[float]]:
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

                result = await _read_row(page, target, for_date)
                month = for_date.strftime("%m.%Y")
                shown = await page.input_value("#price_date")
                if result is _NOT_FOUND and month != shown:
                    try:
                        await page.wait_for_load_state("networkidle",
                                                       timeout=_PAGE_TIMEOUT_MS)
                    except Exception:
                        pass   # best effort; the retries below still apply
                    for attempt in range(1, _MONTH_ATTEMPTS + 1):
                        status = await page.evaluate(_LOAD_MONTH_JS, month)
                        if status == "ok":
                            result = await _read_row(page, target, for_date)
                            break
                        if attempt == _MONTH_ATTEMPTS:
                            log.info("oree.com.ua month table unavailable",
                                     month=month, reply=status)
                        else:
                            await asyncio.sleep(2 * attempt)
                if result is _NOT_FOUND:
                    log.info("Date not found on oree.com.ua", date=target)
                    return None
                return result
            finally:
                await browser.close()
    except Exception as exc:
        log.warning("oree.com.ua headless fetch failed", exc=str(exc))
        return None


async def _read_row(page, target: str, for_date: date):
    """Prices for `target` (dd.mm.YYYY) from the table currently in the page,
    None if its row is malformed, or `_NOT_FOUND` if there is no such row."""
    rows = await page.locator("#price_table tbody tr").all()
    for row in rows:
        cells = await row.locator("td").all_inner_texts()
        if not cells or cells[0].strip() != target:
            continue
        # 24 delivery hours normally, 23/25 on the DST-change days; a fixed
        # cells[1:25] silently dropped the 25th hour and shifted the rest.
        n = local_day_hours(for_date)
        values = [v for v in cells[1:] if v.strip()][:n]
        if len(values) != n:
            log.warning("oree.com.ua row has unexpected cell count",
                        date=target, cells=len(values))
            return None
        try:
            local = [float(re.sub(r"[^\d.]", "", v)) for v in values]
        except ValueError:
            log.warning("oree.com.ua row has non-numeric cell",
                        date=target, raw=values)
            return None
        # The table's columns are Kyiv local delivery hours; every
        # consumer downstream indexes by UTC hour. See timebase.
        return local_hours_to_utc(local, for_date)
    return _NOT_FOUND
