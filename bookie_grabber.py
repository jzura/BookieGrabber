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
from dateutil import parser
from datetime import datetime
from dotenv import load_dotenv

# -------------------------------------------------------------
# Debug / Dry Run Controls
# -------------------------------------------------------------
API_BASE = "https://api.odds-api.io/v3"
BOOKMAKERS = "Bet365,Betfair Exchange"

TOTAL_MARKETS = {
"Bet365": ["Goals Over/Under", "Alternative Total Goals"],
"Betfair Exchange": ["Totals"],
}

TARGET_HDPS = [1.5, 2.5, 3.5]
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
    perth_tz = pytz.timezone("Australia/Perth")
    rows = []

    for e in events:
        raw_date = e.get("date")
        if raw_date:
            match_time_utc = parser.isoparse(raw_date)
            match_time_local = match_time_utc.astimezone(perth_tz)
        else:
            match_time_local = None

        rows.append({
            "event_id": e.get("id"),
            "date": match_time_local.strftime("%Y-%m-%d") if match_time_local else None,
            "home_team": e.get("home"),
            "away_team": e.get("away"),
            "competition": e.get("league", {}).get("name"),
            "match_time": match_time_local,
            "odds_time": datetime.now(perth_tz).strftime("%Y-%m-%d %H:%M:%S")
        })

    return pd.DataFrame(rows)


def extract_totals(odds_data):
    """Extract Totals market from odds data."""
    totals = []
    for bookmaker, markets in odds_data.get("bookmakers", {}).items():
        allowed_markets = TOTAL_MARKETS.get(bookmaker, [])
        for market in markets:
            # Skip irrelevant markets
            if market.get("name") not in allowed_markets:
                continue
            for o in market.get("odds", []):
                hdp = o.get("hdp")
                if hdp not in TARGET_HDPS:
                    continue
                totals.append({
                    "bookmaker": bookmaker,
                    "market_name": market.get("name"),
                    "hdp": hdp,
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

    # Always make an explicit copy so we can safely modify columns
    df_totals = df_totals.copy()

    # Clean bookmaker names for column naming
    df_totals.loc[:, "bookmaker_clean"] = df_totals["bookmaker"].str.replace(" ", "_", regex=False)

    # Melt into long form (over/under values stacked)
    df_long = df_totals.melt(
        id_vars=[
            "event_id", "home_team", "away_team", "competition",
            "match_time", "bookmaker_clean", "hdp", "odds_time"
        ],
        value_vars=["over_odds", "under_odds"],
        var_name="odds_type",
        value_name="odds_value"
    )

    # Build clean column names like Bet365_over_odds_2.5
    df_long.loc[:, "col_name"] = (
        df_long["bookmaker_clean"] + "_" + df_long["odds_type"] + "_" + df_long["hdp"].astype(str)
    )

    # Pivot back to wide format
    df_pivot = df_long.pivot_table(
        index=["event_id", "home_team", "away_team", "competition", "match_time", "odds_time"],
        columns="col_name",
        values="odds_value",
        aggfunc="first"
    ).reset_index()

    # Flatten MultiIndex columns (remove hierarchy)
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


def load_env():
    load_dotenv()
    api_key = os.getenv("ODDS_API_KEY")
    if not api_key:
        raise ValueError("ODDS_API_KEY not found in .env file")
    return api_key


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
            df_hdp = df_totals_all[df_totals_all["hdp"] == hdp].copy()
            df_totals_pivoted = pivot_totals(df_hdp)
    
            if df_totals_pivoted.empty:
                print(f"No data for hdp={hdp}")
                continue
            
            # Define columns
            col_365_over = f"Bet365_over_odds_{hdp}"
            col_365_under = f"Bet365_under_odds_{hdp}"
            col_bf_over = f"Betfair_Exchange_over_odds_{hdp}"
            col_bf_under = f"Betfair_Exchange_under_odds_{hdp}"
    
            # Convert odds columns to numeric (coerce invalid strings to NaN)
            for col in [col_365_over, col_365_under, col_bf_over, col_bf_under]:
                df_totals_pivoted[col] = pd.to_numeric(df_totals_pivoted[col], errors="coerce")
    
            # --- Vectorized RPD calculation ---
            # =IF(OR(ISBLANK(G2), ISBLANK(I2)), "", IF(G2 > I2, 1, IF(ABS(G2 - I2) / ((G2 + I2) / 2) * 100 < 1, 1, ABS(G2 - I2) / ((G2 + I2) / 2) * 100)))
            # Over RPD
            o1 = df_totals_pivoted[col_365_over]
            o2 = df_totals_pivoted[col_bf_over]
            opct = abs(o1 - o2) / ((o1 + o2) / 2) * 100
            df_totals_pivoted["Over RPD"] = np.where(
                o1.isna() | o2.isna(),
                np.nan,
                np.where(
                    o1 > o2,
                    1,
                    np.where(
                        opct < 1,
                        1,
                        opct
                    )
                )
            ).round(3)
    
            # Under RPD
            u1 = df_totals_pivoted[col_365_under]
            u2 = df_totals_pivoted[col_bf_under]
            upct = abs(u1 - u2) / ((u1 + u2) / 2) * 100
            df_totals_pivoted["Under RPD"] = np.where(
                u1.isna() | u2.isna(),
                np.nan,
                np.where(
                    u1 > u2,
                    1,
                    np.where(
                        upct < 1,
                        1,
                        upct
                    )
                )
            ).round(3)
    
            # Optionally, drop rows where both RPDs are NaN
            df_totals_pivoted = df_totals_pivoted.dropna(subset=["Over RPD", "Under RPD"], how="all")
    
            save_dataframe(df_totals_pivoted, f"totals_hdp_{hdp}", folder="data/exports/totals")
    else:
        print("No totals data found")


    # --- Save BTTS data ---
    if not df_btts.empty:
        save_dataframe(df_btts, "btts", folder="data/exports/btts")
    else:
        print("No BTTS data found")

    return df_totals_all, df_btts


if __name__ == "__main__":
        main()  