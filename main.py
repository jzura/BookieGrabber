"""
AUTHOR: RUDOLF
DATE: 24 Oct 2025
DESCRIPTION: Fetch Team Goals Over/Under (Full Match) odds for Champions League and save into a DataFrame
"""

import os
import yaml
import pytz
import requests
import numpy as np
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

# -------------------------------------------------------------
# Debug / Dry Run Controls
# -------------------------------------------------------------
API_BASE = "https://api.odds-api.io/v3"
BOOKMAKERS = "Bet365,Betfair Exchange"

# -------------------------------------------------------------
# Environment variables
# -------------------------------------------------------------
def load_env():
    load_dotenv()
    api_key = os.getenv("ODDS_API_KEY")
    if not api_key:
        raise ValueError("ODDS_API_KEY not found in .env file")
    return api_key


def get_available_sport():
    """Check if football is available"""
    url = "https://api.odds-api.io/v3/sports"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data
    except Exception as e:
        print(f"[ERROR] Failed to fetch league events: {e}")
        return []

def get_league_events(api_key, league="england-premier-league", limit=10):
    """Fetch list of upcoming events for a given league."""
    url = f"{API_BASE}/events?apiKey={api_key}&sport=football&league={league}&limit={limit}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data
    except Exception as e:
        print(f"[ERROR] Failed to fetch league events: {e}")
        return []


def get_event_odds(api_key, event_id):
    """Fetch odds data for a specific event."""
    url = f"{API_BASE}/odds?apiKey={api_key}&eventId={event_id}&bookmakers={BOOKMAKERS}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        odds_data = response.json()
        return odds_data
    except Exception as e:
        print(f"[ERROR] Failed to fetch odds for event {event_id}: {e}")
        return {}


def extract_events_to_df(events):
    """Convert event list to DataFrame with timezone-aware match times."""
    perth_tz = pytz.timezone("Australia/Perth")
    rows = []

    for e in events:
        # Convert UTC timestamp ('Z' → '+00:00') to Perth local datetime
        raw_date = e.get("date")
        if raw_date:
            match_time_utc = datetime.fromisoformat(raw_date.replace("Z", "+00:00"))
            match_time_local = match_time_utc.astimezone(perth_tz)
        else:
            match_time_local = None
        rows.append({
            "event_id": e.get("id"),
            "date": match_time_local.strftime("%Y-%m-%d") if match_time_local else None,
            "home_team": e.get("home"),
            "away_team": e.get("away"),
            "competition": e.get("league", {}).get("name"),
            "match_time": match_time_local,  # timezone-aware datetime
            "odds_time": datetime.now(perth_tz).strftime("%Y-%m-%d %H:%M:%S")
        })
    return pd.DataFrame(rows)


def extract_totals(odds_data):
    """Extract Totals market from odds data."""
    totals = []
    for bookmaker, markets in odds_data.get("bookmakers", {}).items():
        for market in markets:
            if market.get("name") == "Goals Over/Under" or market.get("name") == "Totals" or market.get("name") == "Alternative Total Goals":
                for o in market.get("odds", []):
                    if o.get("hdp") in [1.5, 2.5, 3.5]:
                        totals.append({
                            "bookmaker": bookmaker,
                            "hdp": o.get("hdp"),
                            "over_odds": o.get("over"),
                            "under_odds": o.get("under")
                        })
    return totals

def extract_btts(odds_data):
    """Extract Both Teams to Score market from odds data."""
    btts = []
    for bookmaker, markets in odds_data.get("bookmakers", {}).items():
        for market in markets:
            if market.get("name") == "Both Teams to Score" or market.get("name") == "Both Teams To Score":
                for o in market.get("odds", []):
                        btts.append({
                            "bookmaker": bookmaker,
                            "yes": o.get("yes"),
                            "no": o.get("no"),
                        })
    return btts

def pivot_totals(df_totals):
    """Pivot totals DataFrame so each bookmaker × odds type × hdp becomes a column."""
    if df_totals.empty:
        return df_totals

    # Ensure clean bookmaker names for columns
    df_totals["bookmaker_clean"] = df_totals["bookmaker"].str.replace(" ", "_")
    # Create unique column keys like Bet365_over_odds_2.5
    df_long = df_totals.melt(
        id_vars=[
            "event_id", "home_team", "away_team", "competition", "match_time", "bookmaker_clean", "hdp", "odds_time"
        ],
        value_vars=["over_odds", "under_odds"],
        var_name="odds_type",
        value_name="odds_value"
    )
    df_long["col_name"] = (
        df_long["bookmaker_clean"] + "_" + df_long["odds_type"] + "_" + df_long["hdp"].astype(str)
    )
    # Pivot to wide format
    df_pivot = df_long.pivot_table(
        index=["event_id", "home_team", "away_team", "competition", "match_time", "odds_time"],
        columns="col_name",
        values="odds_value",
        aggfunc="first"
    ).reset_index()
    # Flatten MultiIndex columns
    df_pivot.columns.name = None

    return df_pivot

def ensure_dir(path):
    """Ensure directory exists."""
    os.makedirs(path, exist_ok=True)

def save_dataframe(df, name, folder="data/exports"):
    """Save a DataFrame with timestamp in the given folder."""
    ensure_dir(folder)
    date = datetime.now().strftime("%Y%m%d")
    filename = f"{name}_{date}.csv"
    filepath = os.path.join(folder, filename)
    df.to_csv(filepath, index=False)
    print(f"Saved: {filepath}")
    return filepath

def main():
    """Fetch EPL events, extract Totals (by line) and BTTS, and save separately."""
    api_key = load_env()
    events = get_league_events(api_key)
    if not events:
        print("No events found.")
        return pd.DataFrame(), pd.DataFrame()

    df_events = extract_events_to_df(events)

    totals_rows = []
    btts_rows = []

    for _, row in df_events.iterrows():
        event_id = row["event_id"]
        print(f"Fetching odds for {row['home_team']} vs {row['away_team']}")

        odds_data = get_event_odds(api_key, event_id)
        if not odds_data:
            continue

        # --- Totals ---
        totals = extract_totals(odds_data)
        for t in totals:
            totals_rows.append({
                "event_id": event_id,
                "home_team": row["home_team"],
                "away_team": row["away_team"],
                "competition": row["competition"],
                "match_time": row["match_time"],
                "bookmaker": t["bookmaker"],
                "hdp": t["hdp"],
                "over_odds": t["over_odds"],
                "under_odds": t["under_odds"],
                "odds_time": row["odds_time"]
            })

        # --- BTTS ---
        btts = extract_btts(odds_data)
        for b in btts:
            btts_rows.append({
                "event_id": event_id,
                "home_team": row["home_team"],
                "away_team": row["away_team"],
                "competition": row["competition"],
                "match_time": row["match_time"],
                "bookmaker": b["bookmaker"],
                "yes_odds": b["yes"],
                "no_odds": b["no"],
                "odds_time": row["odds_time"]
            })

    # --- Build DataFrames ---
    df_totals_all = pd.DataFrame(totals_rows)
    df_btts = pd.DataFrame(btts_rows)

    if not df_totals_all.empty:
        for hdp in [1.5, 2.5, 3.5]:
            df_hdp = df_totals_all[df_totals_all["hdp"] == hdp]
            df_totals_pivoted = pivot_totals(df_hdp)
            if not df_totals_pivoted.empty:
                save_dataframe(df_totals_pivoted, f"totals_hdp_{hdp}", folder="data/exports/totals")
            else:
                print(f"No data for hdp={hdp}")
    else:
        print("No totals data found")

    # --- Save BTTS data ---
    if not df_btts.empty:
        save_dataframe(df_btts, "btts", folder="data/exports/btts")
    else:
        print("No BTTS data found")

    # return df_totals_all, df_btts


if __name__ == "__main__":
    main()
