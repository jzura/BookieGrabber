"""
AUTHOR: JZ
DATE: 4 Dec 2025 
DESCRIPTION: Betfair API integration to fetch Over/Under market volumes for specified leagues.
"""

import os
import json
import yaml
import pandas as pd
import requests
from dotenv import load_dotenv

# ---------------------------
# Load .env credentials
# ---------------------------
load_dotenv()

APP_KEY = os.getenv("BETFAIR_API_KEY")
USERNAME = os.getenv("BETFAIR_USERNAME")
PASSWORD = os.getenv("BETFAIR_PASSWORD")

CERT_FILE = "client-2048.cer"
KEY_FILE = "client-2048.key"

BETFAIR_API_URL = "https://api.betfair.com/exchange/betting/json-rpc/v1"
SSO_CERT_URL = "https://identitysso-cert.betfair.com/api/certlogin"

MARKETS = ["OVER_UNDER_15", "OVER_UNDER_25", "OVER_UNDER_35", "BOTH_TEAMS_TO_SCORE"]

# ---------------------------
# Load leagues from YAML
# ---------------------------
with open("config.yaml", "r") as f:
    CONFIG = yaml.safe_load(f)

LEAGUES = CONFIG.get("leagues", [])

# ---------------------------
# JSON-RPC helper
# ---------------------------
def make_request(app_key:str, session_token:str, payload, max_attempts:int=3):
    """Send a Betfair JSON-RPC request with retries on transient failures."""
    import time
    headers = {
        "X-Application": app_key,
        "X-Authentication": session_token,
        "Content-Type": "application/json"
    }
    backoff = [2, 5]
    last_err = None
    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.post(BETFAIR_API_URL, data=json.dumps(payload),
                                     headers=headers, timeout=30)
            response.raise_for_status()
            data = response.json()
            # Betfair returns errors inside the JSON body — surface them so we can retry
            if isinstance(data, dict) and data.get("error"):
                err = data["error"]
                # APING errors with a 'data' field are usually permanent (bad params),
                # but transient ones (TOO_MUCH_DATA, TIMEOUT_ERROR) should retry
                err_code = (err.get("data", {}) or {}).get("APINGException", {}).get("errorCode", "")
                if err_code in ("TIMEOUT_ERROR", "REQUEST_SIZE_EXCEEDS_LIMIT", "SERVICE_BUSY",
                                "INSUFFICIENT_FUNDS", "TOO_MUCH_DATA"):
                    last_err = f"Betfair API transient error: {err_code}"
                    print(f"  [BF] Transient error (attempt {attempt}): {err_code}")
                else:
                    # Permanent error — return as-is so caller can decide
                    return data
            else:
                return data
        except Exception as e:
            last_err = e
            print(f"  [BF] Request failed (attempt {attempt}/{max_attempts}): {e}")

        if attempt < max_attempts:
            time.sleep(backoff[min(attempt - 1, len(backoff) - 1)])

    raise Exception(f"Betfair API failed after {max_attempts} attempts: {last_err}")

# ---------------------------
# Betfair certificate login
# ---------------------------
def get_session_token():
    data = {"username": USERNAME, "password": PASSWORD}
    headers = {"X-Application": APP_KEY, "Content-Type": "application/x-www-form-urlencoded"}

    response = requests.post(SSO_CERT_URL, data=data, headers=headers, cert=(CERT_FILE, KEY_FILE), timeout=30)
    result = response.json()

    if result.get("loginStatus") == "SUCCESS":
        return result["sessionToken"]
    else:
        raise Exception("Login failed: " + result.get("loginStatus", "Unknown error"))

# ---------------------------
# Generic league functions
# ---------------------------
def get_competition_id(session_token:str, league_name):
    payload = {
        "jsonrpc": "2.0",
        "method": "SportsAPING/v1.0/listCompetitions",
        "params": {"filter": {"eventTypeIds": ["1"]}},  # 1 = Soccer
        "id": 1
    }

    data = make_request(APP_KEY, session_token, payload)

    for item in data["result"]:
        if item["competition"]["name"].lower() == league_name.lower():
            return item["competition"]["id"]

    raise ValueError(f"Competition '{league_name}' not found.")

def get_over_under_markets(session_token:str, competition_id):
    payload = {
        "jsonrpc": "2.0",
        "method": "SportsAPING/v1.0/listMarketCatalogue",
        "params": {
            "filter": {
                "competitionIds": [competition_id],
                "marketTypeCodes": ["OVER_UNDER_15", "OVER_UNDER_25", "OVER_UNDER_35", "BOTH_TEAMS_TO_SCORE"],
            },
            "maxResults": "200",
            "marketProjection": ["EVENT", "MARKET_DESCRIPTION", "RUNNER_DESCRIPTION"]
        },
        "id": 1
    }

    data = make_request(APP_KEY, session_token, payload)
    return data["result"]

def get_ou_volume(session_token:str, league_name:str, max_attempts:int=3):
    """Fetch OU/BTTS volume for a league with retries.

    Returns (df_volume, market_catalogue) so the caller can reuse
    the catalogue for price fallback without a redundant API call.
    """
    import time
    competition_id = get_competition_id(session_token, league_name)
    last_markets = []

    for attempt in range(1, max_attempts + 1):
        try:
            markets = get_over_under_markets(session_token, competition_id)
            if markets:
                last_markets = markets
                rows = [{
                    "marketId": m["marketId"],
                    "line": m["marketName"],
                    "total_volume": m["totalMatched"],
                    "event": m["event"]["name"],
                } for m in markets]
                if attempt > 1:
                    print(f"  [BF] Got {len(markets)} markets for {league_name} on attempt {attempt}")
                return pd.DataFrame(rows), markets
            print(f"  [BF] No markets returned for {league_name} (attempt {attempt}/{max_attempts})")
        except Exception as e:
            print(f"  [BF] get_over_under_markets failed for {league_name} "
                  f"(attempt {attempt}/{max_attempts}): {e}")
        if attempt < max_attempts:
            time.sleep(3)

    print(f"  [BF] Returning empty markets for {league_name} after {max_attempts} attempts")
    return pd.DataFrame(), last_markets


# ---------------------------
# Direct price fetching (fallback when Odds API omits BF Exchange)
# ---------------------------

def build_runner_lookup(market_catalogue: list) -> dict:
    """From listMarketCatalogue results build {marketId: {selectionId: runnerName}}."""
    lookup = {}
    for m in market_catalogue:
        mid = m["marketId"]
        lookup[mid] = {}
        for r in m.get("runners", []):
            lookup[mid][r["selectionId"]] = r["runnerName"]
    return lookup


def get_market_prices(session_token: str, market_ids: list) -> dict:
    """Fetch best back prices for given market IDs via listMarketBook.

    Returns {marketId: {selectionId: best_back_price, ...}, ...}.
    """
    if not market_ids:
        return {}
    payload = {
        "jsonrpc": "2.0",
        "method": "SportsAPING/v1.0/listMarketBook",
        "params": {
            "marketIds": market_ids,
            "priceProjection": {"priceData": ["EX_BEST_OFFERS"]},
        },
        "id": 1,
    }
    data = make_request(APP_KEY, session_token, payload)
    result = {}
    for book in data.get("result", []):
        mid = book["marketId"]
        runners = {}
        for r in book.get("runners", []):
            backs = r.get("ex", {}).get("availableToBack", [])
            if backs:
                runners[r["selectionId"]] = backs[0]["price"]
        result[mid] = runners
    return result


# Market-type to HDP mapping
_MARKET_TYPE_HDP = {
    "OVER_UNDER_15": 1.5,
    "OVER_UNDER_25": 2.5,
    "OVER_UNDER_35": 3.5,
}


def fetch_bf_odds_for_event(session_token, bf_event_key, market_catalogue):
    """Fetch Betfair Exchange back prices for a single event.

    Args:
        session_token: active BF session
        bf_event_key: e.g. "Arsenal v Bournemouth"
        market_catalogue: list from get_over_under_markets()

    Returns:
        totals_rows: list of dicts matching extract_totals() format
        btts_rows:   list of dicts matching extract_btts() format
    """
    # Find markets for this event
    event_markets = [m for m in market_catalogue
                     if m.get("event", {}).get("name") == bf_event_key]
    if not event_markets:
        return [], []

    market_ids = [m["marketId"] for m in event_markets]
    runner_lookup = build_runner_lookup(event_markets)
    prices = get_market_prices(session_token, market_ids)

    totals_rows = []
    btts_rows = []

    for m in event_markets:
        mid = m["marketId"]
        mtype = m.get("description", {}).get("marketType", "")
        runners_prices = prices.get(mid, {})
        names = runner_lookup.get(mid, {})

        # Map selectionId -> (name, price)
        named = {}
        for sid, price in runners_prices.items():
            rname = names.get(sid, "")
            named[rname.lower()] = price

        if mtype in _MARKET_TYPE_HDP:
            hdp = _MARKET_TYPE_HDP[mtype]
            over = None
            under = None
            for rname, price in named.items():
                if "over" in rname:
                    over = price
                elif "under" in rname:
                    under = price
            if over and under:
                totals_rows.append({
                    "bookmaker": "Betfair Exchange",
                    "market_name": "Totals",
                    "hdp": hdp,
                    "over_odds": over,
                    "under_odds": under,
                })

        elif mtype == "BOTH_TEAMS_TO_SCORE":
            yes_p = named.get("yes")
            no_p = named.get("no")
            if yes_p and no_p:
                btts_rows.append({
                    "bookmaker": "Betfair Exchange",
                    "yes": yes_p,
                    "no": no_p,
                })

    return totals_rows, btts_rows
