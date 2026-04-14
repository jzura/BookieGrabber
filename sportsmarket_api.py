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
PROJECT_ROOT = Path(__file__).resolve().parent
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


def auto_login():
    """Log into SportsMarket using Playwright headless browser.
    Extracts the root-session cookie and saves it to .sm_cookie.
    Returns the session token, or empty string on failure.
    """
    username = SM_USERNAME
    password = os.getenv("SM_PASSWORD", "")
    if not password:
        logger.error("SM_PASSWORD not set in .env — cannot auto-login")
        return ""

    logger.info("Auto-logging into SportsMarket...")
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context()
            page = context.new_page()

            page.goto("https://pro.sportmarket.com/login", timeout=30000)
            page.wait_for_selector('input[type="text"]', timeout=10000)

            # Fill login form
            page.fill('input[type="text"]', username)
            page.fill('input[type="password"]', password)
            page.click('button[data-testid="35eb9af8"]')

            # Wait for redirect (successful login navigates away from /login)
            page.wait_for_url("**/sportsbook**", timeout=30000)

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
                # Save to cookie file
                SM_COOKIE_FILE.write_text("; ".join(cookie_parts))
                logger.info(f"Auto-login successful, session saved to {SM_COOKIE_FILE}")
                return session_token
            else:
                logger.error("Auto-login: no root-session cookie found after login")
                return ""

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
    # Build lookup from master sheet: (bt, date) -> [(row, home, away)]
    from datetime import datetime as dt
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

        key = (o['bet_type'], o['date'])
        candidates = tgt.get(key, [])

        # Try +/- 1 day for timezone
        if not candidates:
            from datetime import timedelta
            for offset in [-1, 1]:
                alt = (o['bet_type'], o['date'] + timedelta(days=offset))
                candidates = tgt.get(alt, [])
                if candidates:
                    break

        if not candidates:
            unmatched.append(o)
            continue

        best = None
        best_score = 0
        for c in candidates:
            h = match_score(o['home'], c['home'])
            a = match_score(o['away'], c['away'])
            combined = (h + a) / 2
            if combined > best_score:
                best_score = combined
                best = c

        if best_score < 0.5:
            unmatched.append(o)
            continue

        matches.append((best['r'], o['avg_odds'], o))

    return matches, unmatched


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                       format="%(asctime)s [%(levelname)s] %(message)s")

    orders = fetch_all_orders()
    print(f"\nFetched {len(orders)} orders")

    for o in orders[:5]:
        parsed = parse_order(o)
        if parsed:
            print(f"  {parsed['date']} {parsed['bet_type']} "
                  f"{parsed['home']} vs {parsed['away']} "
                  f"avg={parsed['avg_odds']} stake={parsed['total_stake']} "
                  f"P/L={parsed['total_pl']}")