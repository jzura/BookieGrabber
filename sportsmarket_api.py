"""
SportsMarket API integration.

Fetches order history from SportsMarket Pro and calculates
weighted average matched odds per order. Matches orders to
master spreadsheet rows by date + teams + market type.
"""

import os
import json
import logging
import requests
from pathlib import Path
from datetime import datetime, date
from difflib import SequenceMatcher
from collections import defaultdict

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("sportsmarket_api")

SM_USERNAME = os.getenv("SM_USERNAME", "joelbrown95")
SM_BASE = "https://pro.sportmarket.com/v1"
from constants import PROJECT_ROOT
SM_COOKIE_FILE = PROJECT_ROOT / ".sm_cookie"

def _load_sm_session():
    """Load SM session token. Tries cached file first, then auto-login."""
    # Try cached session
    if SM_COOKIE_FILE.exists():
        cookie = SM_COOKIE_FILE.read_text().strip()
        for pair in cookie.split("; "):
            if pair.startswith("root-session="):
                token = pair.split("=", 1)[1]
                if token:
                    return token
    # Try env var
    session = os.getenv("SM_SESSION", "")
    if session:
        return session
    return ""


def _test_session(token):
    """Check if a session token is still valid."""
    try:
        r = requests.get(f"{SM_BASE}/orders/",
                        params={"placer": SM_USERNAME, "page_size": 1},
                        headers={"Accept": "application/json", "session": token,
                                 "x-molly-client-name": "sonic"},
                        timeout=15)
        return r.status_code == 200 and r.json().get("status") == "ok"
    except Exception:
        return False


def _auto_login_worker():
    """Worker function for auto_login — runs Playwright sync API.
    Must run in a thread without a running asyncio event loop.
    """
    from playwright.sync_api import sync_playwright
    username = SM_USERNAME
    password = os.getenv("SM_PASSWORD", "")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        page.goto("https://pro.sportmarket.com/login", timeout=30000)
        page.wait_for_selector('input[type="text"]', timeout=10000)

        # Fill login form
        page.fill('input[type="text"]', username)
        page.fill('input[type="password"]', password)
        page.get_by_role("button", name="log In").click()

        # Wait for redirect (successful login navigates away from /login)
        page.wait_for_url(lambda url: "/sportsbook" in url or "/trade" in url,
                          timeout=30000)

        # Extract cookies
        cookies = context.cookies()
        session_token = ""
        cookie_parts = []
        for c in cookies:
            cookie_parts.append(f"{c['name']}={c['value']}")
            if c['name'] == 'root-session':
                session_token = c['value']

        browser.close()

        if session_token:
            SM_COOKIE_FILE.write_text("; ".join(cookie_parts))
            logger.info(f"Auto-login successful, session saved to {SM_COOKIE_FILE}")
            return session_token
        else:
            logger.error("Auto-login: no root-session cookie found after login")
            return ""


def auto_login():
    """Log into SportsMarket using Playwright headless browser.
    Extracts the root-session cookie and saves it to .sm_cookie.
    Returns the session token, or empty string on failure.

    Runs Playwright in a separate thread to avoid conflicts with
    any running asyncio event loop (e.g. after place_order_playwright).
    """
    password = os.getenv("SM_PASSWORD", "")
    if not password:
        logger.error("SM_PASSWORD not set in .env — cannot auto-login")
        return ""

    logger.info("Auto-logging into SportsMarket...")
    try:
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(_auto_login_worker)
            return future.result(timeout=60)
    except Exception as e:
        logger.exception(f"Auto-login failed: {e}")
        return ""


def get_session():
    """Get a valid SM session token, auto-logging in if needed."""
    token = _load_sm_session()
    if token and _test_session(token):
        return token

    logger.info("SM session expired or missing — attempting auto-login")
    token = auto_login()
    if token and _test_session(token):
        return token

    logger.error("Could not obtain valid SM session")
    return ""

# Map SM bet_type_description to master bet types
BET_TYPE_MAP = {
    "Under 1.5 (Asian)": "1.5G",
    "Over 1.5 (Asian)": "1.5G",
    "Under 2.5 (Asian)": "2.5G",
    "Over 2.5 (Asian)": "2.5G",
    "Under 3.5 (Asian)": "3.5G",
    "Over 3.5 (Asian)": "3.5G",
    "Both teams to score": "BTTS",
    "At least one team to not score": "BTTS",
    "Under 1.5": "1.5G",
    "Over 1.5": "1.5G",
    "Under 2.5": "2.5G",
    "Over 2.5": "2.5G",
    "Under 3.5": "3.5G",
    "Over 3.5": "3.5G",
}


def fetch_orders(page_size=100, page=1):
    """Fetch orders from SportsMarket API.

    Returns list of order dicts, or empty list on failure.
    """
    session_token = get_session()
    if not session_token:
        return []

    url = f"{SM_BASE}/orders/"
    params = {
        "placer": SM_USERNAME,
        "page_size": page_size,
        "page": page,
    }
    headers = {
        "Accept": "application/json",
        "session": session_token,
        "x-molly-client-name": "sonic",
    }

    try:
        r = requests.get(url, params=params, headers=headers, timeout=30)
        r.raise_for_status()
        data = r.json()
        if data.get("status") == "ok":
            return data.get("data", [])
        elif data.get("code") == "auth_error":
            logger.error("SportsMarket auth failed — session cookie expired. "
                        "Re-login in Chrome and update SM_COOKIE in .env")
            return []
        else:
            logger.error(f"SportsMarket API error: {data}")
            return []
    except Exception as e:
        logger.error(f"SportsMarket API request failed: {e}")
        return []


def fetch_all_orders(max_pages=20):
    """Fetch all orders across multiple pages."""
    all_orders = []
    for page in range(1, max_pages + 1):
        orders = fetch_orders(page_size=100, page=page)
        if not orders:
            break
        all_orders.extend(orders)
        if len(orders) < 100:
            break  # last page
    logger.info(f"Fetched {len(all_orders)} SportsMarket orders")
    return all_orders


def parse_order(order):
    """Extract key fields from an SM order.

    Returns dict with: date, home, away, bet_type, avg_odds, total_stake,
    total_pl, status, competition. Or None if not parseable.
    """
    event = order.get("event_info", {})
    if not event:
        return None

    # Map bet type
    desc = order.get("bet_type_description", "")
    bt = BET_TYPE_MAP.get(desc)
    if not bt:
        return None

    # Calculate weighted average matched odds
    bets = order.get("bets", [])
    total_stake = 0
    weighted_price = 0
    total_pl = 0

    for b in bets:
        got_stake = b.get("got_stake")
        got_price = b.get("got_price")
        pl = b.get("profit_loss")

        stake = got_stake[1] if got_stake and len(got_stake) > 1 else 0
        price = got_price if got_price else 0
        profit = pl[1] if pl and len(pl) > 1 else 0

        if stake and price:
            weighted_price += price * stake
            total_stake += stake
        total_pl += profit

    avg_odds = weighted_price / total_stake if total_stake > 0 else None

    match_date = event.get("date")
    if match_date:
        try:
            match_date = datetime.strptime(match_date, "%Y-%m-%d").date()
        except (ValueError, TypeError):
            match_date = None

    return {
        "date": match_date,
        "home": event.get("home_team", ""),
        "away": event.get("away_team", ""),
        "competition": event.get("competition_name", ""),
        "bet_type": bt,
        "bet_type_desc": desc,
        "avg_odds": round(avg_odds, 3) if avg_odds else None,
        "total_stake": round(total_stake, 2),
        "total_pl": round(total_pl, 2),
        "status": order.get("status"),
        "want_price": order.get("want_price"),
    }


def normalize(name):
    """Normalize team name for fuzzy matching."""
    if not name:
        return ""
    s = name.lower()
    for pat in ['fc ', ' fc', 'sc ', ' sc', 'sv ', ' sv', 'fk ', ' fk',
                'ac ', ' ac', '1. ', 'sk ', ' sk', 'nk ', ' nk',
                'if ', ' if', 'bk ', ' bk', 'sl ', ' sl']:
        s = s.replace(pat, ' ')
    s = ''.join(c for c in s if c.isalnum() or c == ' ')
    return ' '.join(s.split()).strip()


def match_score(a, b):
    """Score how well two team names match."""
    na, nb = normalize(a), normalize(b)
    if na == nb:
        return 1.0
    if na in nb or nb in na:
        if min(len(na), len(nb)) > 3:
            return 0.9
    return SequenceMatcher(None, na, nb).ratio()


def match_orders_to_sheet(orders_parsed, ws, last_row):
    """Match parsed SM orders to master sheet rows.

    Returns list of (row_number, avg_odds) tuples.
    """
    from datetime import datetime as dt, timedelta

    # Build lookup from master sheet: (bt, date) -> [(row, home, away)]
    tgt = defaultdict(list)
    for r in range(2, last_row + 1):
        bt = ws.cell(row=r, column=1).value
        d = ws.cell(row=r, column=2).value
        if isinstance(d, dt):
            d = d.date()
        home = ws.cell(row=r, column=3).value or ''
        away = ws.cell(row=r, column=4).value or ''
        tgt[(str(bt).strip(), d)].append({
            'r': r,
            'home': str(home).strip(),
            'away': str(away).strip(),
        })

    matches = []
    unmatched = []

    for o in orders_parsed:
        if not o or not o['avg_odds'] or not o['date']:
            continue

        # Collect candidates from exact date AND ±1 day
        all_candidates = []
        for offset in [0, -1, 1]:
            key = (o['bet_type'], o['date'] + timedelta(days=offset))
            all_candidates.extend(tgt.get(key, []))

        if not all_candidates:
            unmatched.append(o)
            continue

        # Score each candidate — require BOTH home AND away to match reasonably
        best = None
        best_score = 0
        for c in all_candidates:
            h = match_score(o['home'], c['home'])
            a = match_score(o['away'], c['away'])
            # Both teams must match independently (not just average)
            if h < 0.4 or a < 0.4:
                continue
            combined = (h + a) / 2
            if combined > best_score:
                best_score = combined
                best = c

        if best_score < 0.55:
            unmatched.append(o)
            continue

        matches.append((best['r'], o['avg_odds'], o))

    return matches, unmatched


# ─── Playwright auto-placement ───

SM_SPORTSBOOK_BASE = "https://pro.sportmarket.com/sportsbook/football"
SCREENSHOT_DIR = PROJECT_ROOT / "debug_screenshots"

# SM country/league URL prefixes — maps our config slug to SM's URL path
SM_LEAGUE_PREFIXES = {
    "english_premier_league": "XE/1",
    "english_sky_bet_championship": "XE/2",
    "french_ligue_1": "FR/38",
    "german_bundesliga": "DE/12",
    "italian_serie_a": "IT/19",
    "spanish_la_liga": "ES/16",
    "portuguese_primeira_liga": "PT/130",
    "belgian_pro_league": "BE/85",
    "dutch_eredivisie": "NL/81",
    "turkish_super_league": "TR/160",
    "greek_super_league": "GR/119",
    "romanian_liga_i": "RO/167",
    "swiss_super_league": "CH/251",
    "danish_superliga": "DK/76",
    "croatian_hnl": "HR/189",
    "polish_ekstraklasa": "PL/134",
    "serbian_super_league": "RS/518",
    "austrian_bundesliga": "AT/116",
    "czech_1_liga": "CZ/154",
    "scottish_premiership": "XS/8",
    "norwegian_eliteserien": "NO/111",
    "swedish_allsvenskan": "SE/104",
    "uefa_champions_league": "XE/1",
    "uefa_europa_league": "XE/1",
    "uefa_europa_conference_league": "XE/1",
}

# Map (bet_type, prediction) to the UI button text we need to click
# prediction: 0 = Under/No, 1 = Over/Yes
UI_BUTTON_MAP = {
    ("1.5G", 0): {"market": "Goals", "line": "1.5", "side": "Under"},
    ("1.5G", 1): {"market": "Goals", "line": "1.5", "side": "Over"},
    ("2.5G", 0): {"market": "Goals", "line": "2.5", "side": "Under"},
    ("2.5G", 1): {"market": "Goals", "line": "2.5", "side": "Over"},
    ("3.5G", 0): {"market": "Goals", "line": "3.5", "side": "Under"},
    ("3.5G", 1): {"market": "Goals", "line": "3.5", "side": "Over"},
    ("BTTS", 0): {"market": "BTTS", "line": None, "side": "No"},
    ("BTTS", 1): {"market": "BTTS", "line": None, "side": "Yes"},
}


def place_order_playwright(sm_event_id, bet_type, prediction, stake_eur=250.0,
                           want_price=None, headless=True, duration_hours=72,
                           league_slug=None):
    """Place an order on SportsMarket via Playwright browser automation.

    Args:
        sm_event_id: SM event ID format "YYYY-MM-DD,home_id,away_id"
        bet_type: e.g. "1.5G", "2.5G", "3.5G", "BTTS"
        prediction: 0 (Under/No) or 1 (Over/Yes)
        stake_eur: stake amount in EUR
        want_price: minimum acceptable odds (None = take best available)
        headless: run browser headless (False for debugging)
        duration_hours: order duration in hours (default 72)
        league_slug: config slug for league (used to build SM URL)

    Returns:
        (success: bool, message: str)
    """
    ui_info = UI_BUTTON_MAP.get((bet_type, int(prediction)))
    if not ui_info:
        return False, f"Unknown bet type mapping: {bet_type} pred={prediction}"

    password = os.getenv("SM_PASSWORD", "")
    if not password:
        return False, "SM_PASSWORD not set in .env"

    SCREENSHOT_DIR.mkdir(exist_ok=True)

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return False, "Playwright not installed"

    logger.info(f"Placing SM order via Playwright: {sm_event_id} "
                f"{bet_type} pred={prediction} stake=€{stake_eur}")

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=headless)
            context = browser.new_context(viewport={"width": 1400, "height": 900})
            page = context.new_page()

            # --- Step 1: Login ---
            page.goto("https://pro.sportmarket.com/login", timeout=30000)
            page.wait_for_selector('input[type="text"]', timeout=10000)
            page.fill('input[type="text"]', SM_USERNAME)
            page.fill('input[type="password"]', password)
            page.get_by_role("button", name="log In").click()
            # Wait for redirect after login — SM may go to /sportsbook or /trade
            page.wait_for_url(lambda url: "/sportsbook" in url or "/trade" in url,
                              timeout=30000)
            logger.info("Logged in to SM")

            # --- Step 2: Navigate to event page ---
            prefix = SM_LEAGUE_PREFIXES.get(league_slug, "NO/111")
            event_url = f"{SM_SPORTSBOOK_BASE}/{prefix}/{sm_event_id}?origin=sportsbook"
            page.goto(event_url, timeout=30000)
            page.wait_for_load_state("networkidle", timeout=15000)
            logger.info(f"Navigated to event: {event_url}")
            page.screenshot(path=str(SCREENSHOT_DIR / "01_event_page.png"))

            # --- Step 3: Find and click the correct odds button ---
            success, msg, market_odds = _click_odds_button(page, ui_info)
            if not success:
                page.screenshot(path=str(SCREENSHOT_DIR / "err_odds_button.png"))
                browser.close()
                return False, msg

            page.screenshot(path=str(SCREENSHOT_DIR / "02_odds_clicked.png"))
            logger.info(f"Clicked odds button: {ui_info}")

            # --- Step 4: Fill in the betslip ---
            # Wait for betslip to appear and load average price
            page.wait_for_timeout(2000)

            # Read the average price from the betslip (more accurate than button price)
            betslip_price = _read_betslip_price(page)
            price = want_price or betslip_price or market_odds
            logger.info(f"Using price: {price} (betslip={betslip_price}, button={market_odds})")
            success, msg = _fill_betslip(page, stake_eur, price, duration_hours)
            if not success:
                page.screenshot(path=str(SCREENSHOT_DIR / "err_betslip.png"))
                browser.close()
                return False, msg

            page.screenshot(path=str(SCREENSHOT_DIR / "03_betslip_filled.png"))

            # --- Step 5: Click Place Order ---
            success, msg = _click_place_order(page)
            if not success:
                page.screenshot(path=str(SCREENSHOT_DIR / "err_place_order.png"))
                browser.close()
                return False, msg

            page.screenshot(path=str(SCREENSHOT_DIR / "04_order_placed.png"))

            # --- Step 6: Wait and capture confirmation ---
            import time
            time.sleep(5)
            page.screenshot(path=str(SCREENSHOT_DIR / "05_confirmation.png"))

            browser.close()

            # --- Step 7: Check order fills via API ---
            order_info = _check_latest_order_fill(sm_event_id, bet_type, prediction)
            return True, order_info or f"Order placed at price {price}"

    except Exception as e:
        logger.exception(f"Playwright placement failed: {e}")
        return False, f"Playwright error: {e}"


def _click_odds_button(page, ui_info):
    """Find and click the correct odds button on the SM event page.

    Uses ONLY text-based matching — no CSS class selectors, so it survives
    SM deployments that rotate obfuscated class names.

    Strategy:
    1. Find all spans containing market header text ("asian total goals" / "both teams to score")
    2. Walk up to the section container (parent with multiple children)
    3. Find line labels ("1.5", "2.5", "3.5") and side buttons ("over"/"under"/"yes"/"no")
    4. Match by text content only

    Returns (success, message, market_odds) where market_odds is a float or None.
    """
    market = ui_info["market"]
    line = ui_info["line"]
    side = ui_info["side"].lower()

    logger.info("Waiting for odds to load...")
    page.wait_for_timeout(3000)

    try:
        # Click ALL "Show all lines" buttons on the page to reveal all markets
        try:
            page.evaluate('''() => {
                const divs = document.querySelectorAll("div");
                for (const div of divs) {
                    const t = div.textContent.trim();
                    if (t === "Show all lines") div.click();
                }
            }''')
            page.wait_for_timeout(2000)
        except Exception:
            pass

        clicked = page.evaluate('''(args) => {
            const {market, side, targetLine} = args;
            const headerText = market === "BTTS" ? "both teams to score" : "asian total goals";

            // Step 1: Find the market header span
            const allSpans = document.querySelectorAll("span");
            let sectionEl = null;
            for (const span of allSpans) {
                if (span.textContent.trim().toLowerCase() === headerText) {
                    // Step 2: Walk up to find container that has over/under or yes/no spans
                    let parent = span;
                    for (let i = 0; i < 10; i++) {
                        parent = parent.parentElement;
                        if (!parent) break;
                        const childSpans = parent.querySelectorAll("span");
                        let hasButtons = false;
                        for (const s of childSpans) {
                            const t = s.textContent.trim().toLowerCase();
                            if (t === "over" || t === "under" || t === "yes" || t === "no") {
                                hasButtons = true;
                                break;
                            }
                        }
                        if (hasButtons) {
                            sectionEl = parent;
                            break;
                        }
                    }
                    break;
                }
            }

            if (!sectionEl) {
                return {ok: false, error: headerText + " section not found"};
            }

            if (market === "BTTS") {
                // Find yes/no buttons by span text within the section
                const spans = sectionEl.querySelectorAll("span");
                for (const span of spans) {
                    if (span.textContent.trim().toLowerCase() === side) {
                        // Click the parent div (the button container)
                        const btn = span.parentElement;
                        if (btn) {
                            btn.click();
                            // Find odds value — sibling span with a number
                            const siblings = btn.querySelectorAll("span");
                            let odds = "?";
                            for (const s of siblings) {
                                const t = s.textContent.trim();
                                if (/^\d+\.\d+$/.test(t)) { odds = t; break; }
                            }
                            return {ok: true, label: span.textContent.trim(), odds: odds};
                        }
                    }
                }
                return {ok: false, error: "BTTS " + side + " button not found in section"};
            }

            // Goals market — find the right line, then the right side button
            // Line labels are leaf divs with text exactly "1.5", "2.5", or "3.5"
            const allDivs = sectionEl.querySelectorAll("div");
            let allLines = [];
            const validLines = new Set(["0.5", "1.5", "2.5", "3.5", "4.5", "5.5"]);

            for (const div of allDivs) {
                if (div.children.length > 0) continue;
                const t = div.textContent.trim();
                if (!validLines.has(t)) continue;
                allLines.push(t);
                if (t !== targetLine) continue;

                // Found the line — now find the over/under button in the same row
                // The line label and buttons share a common parent (the row container)
                let row = div.parentElement;
                // Walk up until we find a container with over/under spans
                for (let i = 0; i < 4; i++) {
                    if (!row) break;
                    const rowSpans = row.querySelectorAll("span");
                    let hasOverUnder = false;
                    for (const s of rowSpans) {
                        const st = s.textContent.trim().toLowerCase();
                        if (st === "over" || st === "under") { hasOverUnder = true; break; }
                    }
                    if (hasOverUnder) break;
                    row = row.parentElement;
                }

                if (!row) continue;

                // Click the matching side button
                const rowSpans = row.querySelectorAll("span");
                for (const span of rowSpans) {
                    if (span.textContent.trim().toLowerCase() === side) {
                        const btn = span.parentElement;
                        if (btn) {
                            btn.scrollIntoView({behavior: "instant", block: "center"});
                            btn.click();
                            const siblings = btn.querySelectorAll("span");
                            let odds = "?";
                            for (const s of siblings) {
                                const st = s.textContent.trim();
                                if (/^\d+\.\d+$/.test(st)) { odds = st; break; }
                            }
                            return {ok: true, label: span.textContent.trim(),
                                    line: t, odds: odds};
                        }
                    }
                }
                return {ok: false, error: side + " button not found in line " + t};
            }

            if (allLines.length === 0) {
                return {ok: false, error: "no line labels found in " + headerText + " section"};
            }
            return {ok: false, error: "line " + targetLine + " not found. Available: " + allLines.join(", ")};
        }''', {"market": market, "side": side, "targetLine": line})

        if clicked.get("ok"):
            odds_str = clicked.get("odds", "")
            market_odds = _parse_odds_text(odds_str)
            label = clicked.get("label", side)
            line_found = clicked.get("line", line or "")
            log_msg = f"Clicked {label} {line_found}" if line_found else f"Clicked {label}"
            logger.info(f"{log_msg} (odds: {odds_str}, parsed: {market_odds})")
            return True, f"Clicked {side} {line} button (odds: {odds_str})", market_odds
        return False, f"Could not find {side} {line} button: {clicked.get('error', '?')}", None

    except Exception as e:
        return False, f"Error clicking odds button: {e}", None


def _read_betslip_price(page):
    """Read the average market price from the betslip exchange table.

    After clicking an odds button, the betslip shows a table with bookmaker
    prices. The "All Bookies" row has 3 price columns:
      - Column 0: highest/best back price
      - Column 1: average price  <-- this is what we want
      - Column 2: lowest price

    Uses text-based matching only — no CSS class selectors.
    Returns float or None.
    """
    try:
        prices = page.evaluate(r'''() => {
            // Find "All Bookies" text, walk up to row, extract price-like spans
            const allEls = document.querySelectorAll("*");
            for (const el of allEls) {
                if (el.children.length === 0 && el.textContent.trim() === "All Bookies") {
                    let parent = el.parentElement;
                    for (let i = 0; i < 8 && parent; i++) {
                        // Find all leaf spans/divs with price-like text (e.g. "1.567")
                        const leaves = parent.querySelectorAll("span, div");
                        const priceVals = [];
                        for (const leaf of leaves) {
                            if (leaf.children.length > 0) continue;
                            const t = leaf.textContent.trim();
                            if (/^\d+\.\d{2,3}$/.test(t) && parseFloat(t) > 1.0 && parseFloat(t) < 50) {
                                priceVals.push(t);
                            }
                        }
                        if (priceVals.length >= 2) {
                            return priceVals;
                        }
                        parent = parent.parentElement;
                    }
                }
            }
            return null;
        }''')
        if prices and len(prices) >= 2:
            avg_price = float(prices[1])
            logger.info(f"Betslip prices: highest={prices[0]}, avg={prices[1]}, "
                       f"lowest={prices[2] if len(prices) > 2 else '?'} — using avg={avg_price}")
            return avg_price
        elif prices and len(prices) == 1:
            val = float(prices[0])
            logger.info(f"Betslip single price: {val}")
            return val
    except Exception as e:
        logger.warning(f"Could not read betslip price: {e}")
    return None


def _parse_odds_text(odds_str):
    """Parse SM odds text like '1.695' or '1.695/2.100' into a float.
    Returns the first (back) price, or None if unparseable.
    """
    if not odds_str or odds_str == "-/-" or odds_str == "?":
        return None
    try:
        # Could be "1.695" or "1.695/2.100" (back/volume or back/lay)
        parts = odds_str.split("/")
        return float(parts[0])
    except (ValueError, IndexError):
        return None


def _fill_betslip(page, stake_eur, want_price=None, duration_hours=72):
    """Fill in the betslip with stake and optional price.

    After clicking an odds button, SM shows a betslip panel (right side
    or bottom bar). We need to find the stake input and fill it.

    Returns (success, message).
    """
    try:
        # Wait for betslip to populate after clicking odds
        page.wait_for_timeout(2000)

        # Screenshot the current state to help debug
        page.screenshot(path=str(SCREENSHOT_DIR / "02b_pre_betslip.png"))

        # Inspect the betslip area to find inputs
        betslip_info = page.evaluate('''() => {
            // Find all visible inputs on the page
            const inputs = document.querySelectorAll("input");
            let info = [];
            inputs.forEach(inp => {
                if (inp.offsetParent !== null) {  // visible
                    info.push({
                        type: inp.type,
                        placeholder: inp.placeholder,
                        name: inp.name,
                        value: inp.value,
                        className: inp.className.substring(0, 80),
                        id: inp.id,
                        parentClass: inp.parentElement ?
                            inp.parentElement.className.substring(0, 80) : "",
                    });
                }
            });
            return info;
        }''')

        logger.info(f"Found {len(betslip_info)} visible inputs: "
                    f"{json.dumps(betslip_info, indent=2)}")

        # Use the known SM class selectors for stake and price inputs
        stake_input = page.locator("input.stake-input").first
        try:
            stake_input.wait_for(state="visible", timeout=5000)
        except Exception:
            return False, (f"Could not find stake input (input.stake-input). "
                          f"Visible inputs: {json.dumps(betslip_info)}")

        # Fill stake
        stake_input.click()
        stake_input.fill(str(int(stake_eur)))
        logger.info(f"Entered stake: €{stake_eur}")

        # Fill price — SM requires this to enable the Place button
        if want_price:
            price_input = page.locator("input.price-input").first
            try:
                price_input.wait_for(state="visible", timeout=3000)
                price_input.click()
                price_input.fill(str(want_price))
                logger.info(f"Set price: {want_price}")
            except Exception:
                logger.warning("Could not find price input — Place button may be disabled")
        else:
            logger.warning("No price available — Place button may be disabled")

        return True, "Betslip filled"

    except Exception as e:
        return False, f"Error filling betslip: {e}"


def _click_place_order(page):
    """Click the Place Order button, then confirm the confirmation dialog.

    SM has a two-step flow:
    1. Click "Place" in the betslip
    2. A confirmation dialog appears ("Are you sure you want to place this order?")
    3. Click "Place Order" in the confirmation dialog

    Returns (success, message).
    """
    try:
        # Wait a moment for the button to become enabled after filling fields
        page.wait_for_timeout(1000)

        # Step 1: Click the initial Place button in the betslip
        btn = page.locator('button:has-text("Place")').first
        try:
            btn.wait_for(state="visible", timeout=5000)
        except Exception:
            return False, "Could not find Place button"

        is_disabled = btn.get_attribute("disabled")
        if is_disabled is not None:
            page.screenshot(path=str(SCREENSHOT_DIR / "err_button_disabled.png"))
            return False, "Place button is disabled — stake or price may be invalid"

        btn.click()
        logger.info("Clicked initial Place button")
        page.screenshot(path=str(SCREENSHOT_DIR / "04a_confirmation_dialog.png"))

        # Step 2: Wait for and click the confirmation dialog's "Place Order" button
        page.wait_for_timeout(1500)
        confirm_btn = page.locator('button:has-text("Place Order")').first
        try:
            confirm_btn.wait_for(state="visible", timeout=5000)
        except Exception:
            # Maybe there's no confirmation dialog — order went through directly
            logger.info("No confirmation dialog found — order may have been placed directly")
            return True, "Order submitted (no confirmation dialog)"

        confirm_btn.click()
        logger.info("Clicked confirmation Place Order button")
        return True, "Order confirmed and submitted"

    except Exception as e:
        return False, f"Error clicking place order: {e}"


def _check_latest_order_fill(sm_event_id, bet_type, prediction):
    """Check the most recent SM order for fill details.

    Fetches recent orders from the API and finds the one matching
    this event/bet type, then returns fill summary.
    """
    import time
    time.sleep(5)  # Give SM time to process fills

    try:
        # Use cached session directly — don't call get_session() which may
        # trigger auto_login() and fail if Playwright event loop is active
        token = _load_sm_session()
        if not token:
            return None

        orders = fetch_orders(page_size=5, page=1)
        if not orders:
            return None

        # Find the matching order (most recent first)
        for order in orders:
            ev = order.get("event_info", {})
            eid = ev.get("event_id", "")
            if eid != sm_event_id:
                continue

            order_id = order.get("order_id", "?")
            status = order.get("status", "?")
            bets = order.get("bets", [])

            total_stake = 0
            weighted_price = 0
            for b in bets:
                got_stake = b.get("got_stake", [None, 0])
                got_price = b.get("got_price", 0)
                stake = got_stake[1] if got_stake and len(got_stake) > 1 else 0
                if stake and got_price:
                    weighted_price += got_price * stake
                    total_stake += stake

            if total_stake > 0:
                avg_odds = weighted_price / total_stake
                fills = len(bets)
                exchanges = ", ".join(set(b.get("bookie", "?") for b in bets))
                return (f"Order {order_id} — {status}\n"
                        f"Matched odds: {avg_odds:.3f}\n"
                        f"Filled: €{total_stake:.2f} across {fills} fill(s)\n"
                        f"Exchanges: {exchanges}")
            else:
                return f"Order {order_id} — {status}, pending fill..."

        return None
    except Exception as e:
        logger.warning(f"Could not check order fills: {e}")
        return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                       format="%(asctime)s [%(levelname)s] %(message)s")

    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "place-test":
        # Test placement with visible browser
        # Usage: python sportsmarket_api.py place-test <event_id> <bet_type> <pred>
        # Example: python sportsmarket_api.py place-test 2026-04-15,527,515 3.5G 0
        if len(sys.argv) < 5:
            print("Usage: python sportsmarket_api.py place-test <event_id> <bet_type> <prediction>")
            print("Example: python sportsmarket_api.py place-test 2026-04-15,527,515 3.5G 0")
            sys.exit(1)
        eid = sys.argv[2]
        bt = sys.argv[3]
        pred = int(sys.argv[4])
        stake = float(sys.argv[5]) if len(sys.argv) > 5 else 1.0  # €1 for testing
        print(f"Test placement: event={eid} type={bt} pred={pred} stake=€{stake}")
        ok, msg = place_order_playwright(eid, bt, pred, stake_eur=stake, headless=False)
        print(f"Result: {'SUCCESS' if ok else 'FAILED'} — {msg}")
    else:
        orders = fetch_all_orders()
        print(f"\nFetched {len(orders)} orders")

        for o in orders[:5]:
            parsed = parse_order(o)
            if parsed:
                print(f"  {parsed['date']} {parsed['bet_type']} "
                      f"{parsed['home']} vs {parsed['away']} "
                      f"avg={parsed['avg_odds']} stake={parsed['total_stake']} "
                      f"P/L={parsed['total_pl']}")