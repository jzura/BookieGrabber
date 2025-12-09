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

CERT_FILE = "client-2048.crt"
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
def make_request(app_key:str, session_token:str, payload):
    headers = {
        "X-Application": app_key,
        "X-Authentication": session_token,
        "Content-Type": "application/json"
    }
    response = requests.post(BETFAIR_API_URL, data=json.dumps(payload), headers=headers)
    response.raise_for_status()
    return response.json()

# ---------------------------
# Betfair certificate login
# ---------------------------
def get_session_token():
    data = {"username": USERNAME, "password": PASSWORD}
    headers = {"X-Application": APP_KEY, "Content-Type": "application/x-www-form-urlencoded"}

    response = requests.post(SSO_CERT_URL, data=data, headers=headers, cert=(CERT_FILE, KEY_FILE))
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

def get_ou_volume(session_token:str, league_name:str):
    competition_id = get_competition_id(session_token, league_name)
    markets = get_over_under_markets(session_token, competition_id)

    rows = []
    for m in markets:
        rows.append({
            "marketId": m["marketId"],
            "line": m["marketName"],
            "total_volume": m["totalMatched"],
            "event": m["event"]["name"],
        })

    return pd.DataFrame(rows)

# ---------------------------
# Example usage
# ---------------------------
# if __name__ == "__main__":
#     session_token = get_session_token()
#     print("SESSION TOKEN:", session_token)

#     # Pick a league from config (for example, first league)
#     league_name = LEAGUES[0]["name"]
#     df = get_ou_volume(session_token, league_name)
#     print(df.head())
