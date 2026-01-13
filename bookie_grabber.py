"""
AUTHOR: JZ
DATE: 24 Oct 2025
DESCRIPTION: Config-driven fetcher to pull Totals/BTTS odds from Odds-API,
            pivot results, compute RPDs and save CSVs. Only replaces odds
            following a 'target hours before KO' rule (e.g. 9 hours).
"""

import os
import re
import json
import yaml
import pytz
import shutil
from pathlib import Path
import requests
import numpy as np
import pandas as pd
from dateutil import parser
from datetime import datetime
from betfair_api import *
from bookie_postproc import run_postprocessing_and_exports
from bookie_emailer import email_workbook
from dotenv import load_dotenv

# -------------------------------------------------------------
# Configurable constants
# -------------------------------------------------------------

API_BASE_OLD = "https://api.odds-api.io/v3"
API_BASE = "https://api2.odds-api.io/v3"
BOOKMAKERS = "Bet365,Betfair Exchange"
TOTAL_MARKETS = {
    "Bet365": ["Goals Over/Under", "Alternative Goal Line"],
    "Betfair Exchange": ["Totals"],
}
TARGET_HDPS = [1.5, 2.5, 3.5]
# folders
EXPORT_ROOT = "data/exports"
# Betfair market line for total volume
BF_BTTS_MARKET_NAME = 'Both teams to Score?'
BF_TOTALS_MARKET_NAME = {
    1.5: 'Over/Under 1.5 Goals',
    2.5: 'Over/Under 2.5 Goals',
    3.5: 'Over/Under 3.5 Goals'
}

# -------------------------------------------------------------
# Helpers: IO, env, slugify
# -------------------------------------------------------------

def load_env():
    load_dotenv()
    api_key = os.getenv("ODDS_API_KEY")
    if not api_key:
        raise ValueError("ODDS_API_KEY not found in .env file")
    return api_key

def load_team_map(path):
    with open(path, "r") as f:
        return json.load(f)

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

# -------------------------------------------------------------
# Config loader
# -------------------------------------------------------------

def load_config(path: str = "config.yaml"):
    with open(path, "r") as fh:
        cfg = yaml.safe_load(fh)
    if not cfg or "leagues" not in cfg:
        raise ValueError("config.yaml must have a top-level 'leagues' list")
    return cfg

# -------------------------------------------------------------
# API fetchers
# -------------------------------------------------------------

def get_league_events(api_key: str, league: str, limit: int = 200):
    """Fetch list of upcoming events for a given league from Odds-API."""
    url = f"{API_BASE}/events"
    # "status": "pending"
    params = {"apiKey": api_key, "sport": "football", "league": league, "limit": limit}
    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        # The API might put the list under data or return a list directly
        # Try common fields first:
        if isinstance(data, dict):
            for key in ("data", "events", "results"):
                if key in data:
                    return data[key]
            # fallback: maybe top-level "data" missing but content is in 'response'
            return data.get("data", [])
        return data
    except Exception as e:
        print(f"[ERROR] Failed to fetch league events for {league}: {e}")
        return []

def get_event_odds(api_key: str, event_id: int):
    """Fetch odds data for a specific event from Odds-API."""
    url = f"{API_BASE}/odds"
    params = {"apiKey": api_key, "eventId": event_id, "bookmakers": BOOKMAKERS}
    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[ERROR] Failed to fetch odds for event {event_id}: {e}")
        return {}

# -------------------------------------------------------------
# Parsing / extraction helpers
# -------------------------------------------------------------

PERTH = pytz.timezone("Australia/Perth")

def parse_api_datetime_to_perth(raw_date: str):
    """Parse ISO-8601 string from API (assumed UTC if 'Z') into Perth tz-aware datetime."""
    if not raw_date:
        return None
    dt = parser.isoparse(raw_date)  # produces tz-aware datetime if string contains timezone or Z
    # If parsed dt is naive, assume UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=pytz.UTC)
    return dt.astimezone(PERTH)

def extract_events_to_df(events):
    """Convert event list to DataFrame and add tz-aware match_time and odds_time (Perth)."""
    rows = []
    now_perth = datetime.now(PERTH)
    for e in events:
        raw_date = e.get("date")
        match_time_local = parse_api_datetime_to_perth(raw_date) if raw_date else None
        rows.append({
            "event_id": e.get("id"),
            "date": match_time_local.strftime("%Y-%m-%d") if match_time_local else None,
            "home_team": e.get("home"),
            "away_team": e.get("away"),
            "competition": e.get("league", {}).get("name") if isinstance(e.get("league"), dict) else e.get("league"),
            "match_time": match_time_local,
            "odds_time": now_perth  # store as tz-aware datetime (not string) for easier comparisons
        })
    return pd.DataFrame(rows)

def extract_totals(odds_data):
    """Extract Totals markets for configured bookmakers and target HDPs."""
    totals = []
    # Many API responses use a dict 'bookmakers' mapping to lists; others nest differently.
    bookmakers_block = odds_data.get("bookmakers") or odds_data.get("bookmakers_data") or odds_data.get("data", {}).get("bookmakers", [])
    # If bookmakers_block is a dict keyed by bookmaker name or a list of bookmakers
    if isinstance(bookmakers_block, dict):
        iter_bookmakers = bookmakers_block.items()
    else:
        # If it's a list with objects containing 'key' or 'title'
        iter_bookmakers = []
        for b in bookmakers_block:
            name = b.get("title") or b.get("key") or b.get("name")
            markets = b.get("markets") or b.get("markets", []) or b.get("markets", [])
            iter_bookmakers.append((name, markets))

    for bookmaker, markets in iter_bookmakers:
        allowed_markets = TOTAL_MARKETS.get(bookmaker, [])
        # markets might be dict or list
        if isinstance(markets, dict):
            markets_list = markets.get("markets", []) if "markets" in markets else []
        else:
            markets_list = markets or []

        for market in markets_list:
            if market.get("name") not in allowed_markets:
                continue
            for o in market.get("odds", []) if market.get("odds") else market.get("lines", []):
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
    """Extract BTTS from odds response."""
    btts = []
    bookmakers_block = odds_data.get("bookmakers") or []
    # normalize as above
    if isinstance(bookmakers_block, dict):
        iter_bookmakers = bookmakers_block.items()
    else:
        iter_bookmakers = []
        for b in bookmakers_block:
            name = b.get("title") or b.get("key") or b.get("name")
            markets = b.get("markets") or b.get("markets", []) or b.get("markets", [])
            iter_bookmakers.append((name, markets))

    for bookmaker, markets in iter_bookmakers:
        markets_list = markets or []
        for market in markets_list:
            if market.get("name") in ("Both Teams to Score", "Both Teams To Score"):
                for o in market.get("odds", []) if market.get("odds") else market.get("lines", []):
                    btts.append({
                        "bookmaker": bookmaker,
                        "yes": o.get("yes"),
                        "no": o.get("no")
                    })
    return btts

# -------------------------------------------------------------
# Pivot & calculations
# -------------------------------------------------------------

def pivot_odds_dataframe(df_totals:pd.DataFrame, id_cols:list, val_cols:list):
    """Pivot totals DataFrame so each bookmaker × odds type × hdp becomes a column."""
    if df_totals.empty:
        return df_totals

    df_totals = df_totals.copy()
    df_totals["bookmaker_clean"] = df_totals["bookmaker"].str.replace(" ", "_", regex=False)

    df_long = df_totals.melt(
        id_vars=id_cols + ["bookmaker_clean"],
        value_vars=val_cols,
        var_name="odds_type",
        value_name="odds_value"
    )

    df_long["col_name"] = (
        df_long["bookmaker_clean"] + "_" + df_long["odds_type"]
    )

    df_pivot = df_long.pivot_table(
        index=["event_id", "home_team", "away_team", "competition", "match_time", "odds_time"],
        columns="col_name",
        values="odds_value",
        aggfunc="first"
    ).reset_index()

    df_pivot.columns.name = None
    return df_pivot

def compute_rpds(df_pivot, btts=False):
    """
    Compute Over/Under (or Yes/No) RPDs.
    """
    df = df_pivot.copy()

    # Column name mapping
    prefix_365 = "Bet365"
    prefix_bf  = "Betfair_Exchange"

    if not btts:
        pairs = [
            (f"{prefix_365}_over_odds",  f"{prefix_bf}_over_odds",  "Over RPD"),
            (f"{prefix_365}_under_odds", f"{prefix_bf}_under_odds", "Under RPD"),
        ]
    else:
        pairs = [
            (f"{prefix_365}_yes_odds", f"{prefix_bf}_yes_odds", "Yes RPD"),
            (f"{prefix_365}_no_odds",  f"{prefix_bf}_no_odds",  "No RPD"),
        ]

    def calc_rpd(a, b):
        """
        Vectorised RPD:
            - if any missing → NaN
            - if a > b → 1
            - else → percent difference, minimum = 1
        """
        opct = (a - b).abs() / ((a + b) / 2) * 100
        return np.where(
            a.isna() | b.isna(),
            np.nan,
            np.where(a > b, 1, np.where(opct < 1, 1, opct))
        )

    # Process each pair
    for c1, c2, out_col in pairs:

        # if either column not present → skip gracefully
        if c1 not in df.columns or c2 not in df.columns:
            continue

        # numeric conversion (vectorised)
        a = pd.to_numeric(df[c1], errors="coerce")
        b = pd.to_numeric(df[c2], errors="coerce")

        df[out_col] = calc_rpd(a, b).round(3)

    return df

# -------------------------------------------------------------
# Existing data loader + merge decision logic
# -------------------------------------------------------------

def todays_filename(folder: str, prefix: str):
    date_str = datetime.now(PERTH).strftime("%Y%m%d")
    ensure_dir(folder)
    return os.path.join(folder, f"{prefix}_{date_str}.csv")

def load_existing_csv(path):
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        df = pd.read_csv(path)
        # Try to parse tz-aware datetimes — pandas will keep tzinfo if present in string
        if "match_time" in df.columns:
            df["match_time"] = pd.to_datetime(df["match_time"], utc=True).dt.tz_convert(PERTH)
        if "odds_time" in df.columns:
            df["odds_time"] = pd.to_datetime(df["odds_time"], utc=True).dt.tz_convert(PERTH)
        return df
    except Exception as e:
        print(f"[WARN] Could not load existing CSV {path}: {e}")
        return pd.DataFrame()

def decide_merge(existing_df: pd.DataFrame, new_df: pd.DataFrame, target_hours: int):
    """
    Merge existing and new DataFrames on event_id using the following rules:
        1) If existing odds are already within the target_hours window -> KEEP existing row.
        2) Else if new odds are within the target_hours window -> USE new row.
        3) Else (neither in window) -> choose the row with the LATER odds_time (newer snapshot).

    Returns final DataFrame with tz-aware datetimes and a new column:
        - hours_until_KO : (match_time - odds_time) in hours (float; can be negative if odds_time after match_time)
    """
    # Normalize dtypes & tz
    def _ensure_tz(df, col):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True).dt.tz_convert(PERTH)
        return df

    existing = existing_df.copy() if existing_df is not None else pd.DataFrame()
    new = new_df.copy() if new_df is not None else pd.DataFrame()

    existing = _ensure_tz(existing, "match_time")
    existing = _ensure_tz(existing, "odds_time")
    new = _ensure_tz(new, "match_time")
    new = _ensure_tz(new, "odds_time")

    # If no existing rows, just return new after computing hours_until_KO
    if existing.empty:
        if not new.empty:
            new["hours_until_KO"] = (new["match_time"] - new["odds_time"]).dt.total_seconds() / 3600.0
        return new

    # Outer merge so we see all event_ids
    merged = existing.merge(new, on="event_id", how="outer", suffixes=("_old", "_new"), indicator=True)

    chosen_rows = []
    for _, r in merged.iterrows():
        # helper to get fields safely
        def val(field, suffix):
            k = f"{field}{suffix}"
            return r[k] if k in r.index else None

        match_old = val("match_time", "_old")
        odds_old = val("odds_time", "_old")
        match_new = val("match_time", "_new")
        odds_new = val("odds_time", "_new")

        # If only new exists
        if pd.isna(match_old) and not pd.isna(match_new):
            chosen = {"event_id": r["event_id"], **{
                k.replace("_new", ""): r[k] for k in r.index if k.endswith("_new")
            }}
            chosen_rows.append(chosen)
            continue

        # If only old exists
        if pd.isna(match_new) and not pd.isna(match_old):
            chosen = {"event_id": r["event_id"], **{
                k.replace("_old", ""): r[k] for k in r.index if k.endswith("_old")
            }}
            chosen_rows.append(chosen)
            continue

        # Both exist (or both NaN) — compute hours until KO safely
        def hours_until_ko(match_dt, odds_dt):
            try:
                return (match_dt - odds_dt).total_seconds() / 3600.0
            except Exception:
                return float("inf")  # treat as "not in window" if something's wrong

        hours_old = hours_until_ko(match_old, odds_old)
        hours_new = hours_until_ko(match_new, odds_new)

        old_in_window = hours_old <= target_hours
        new_in_window = hours_new <= target_hours

        # 1) If existing is already in the window -> keep existing
        if old_in_window:
            chosen = {"event_id": r["event_id"], **{
                k.replace("_old", ""): r[k] for k in r.index if k.endswith("_old")
            }}
            chosen_rows.append(chosen)
            continue

        # 2) Else if new is in the window -> use new (we want the first/any snapshot inside window)
        if new_in_window:
            chosen = {"event_id": r["event_id"], **{
                k.replace("_new", ""): r[k] for k in r.index if k.endswith("_new")
            }}
            chosen_rows.append(chosen)
            continue

        # 3) Neither is in the window -> pick the later odds_time (newer snapshot)
        try:
            odds_old_ts = pd.to_datetime(odds_old) if odds_old is not None else pd.NaT
            odds_new_ts = pd.to_datetime(odds_new) if odds_new is not None else pd.NaT
            if pd.isna(odds_old_ts) and not pd.isna(odds_new_ts):
                chosen = {"event_id": r["event_id"], **{
                k.replace("_new", ""): r[k] for k in r.index if k.endswith("_new")
                }}
            elif pd.isna(odds_new_ts) and not pd.isna(odds_old_ts):
                chosen = {"event_id": r["event_id"], **{
                k.replace("_old", ""): r[k] for k in r.index if k.endswith("_old")
                }}
            else:
                # choose new if it's equal or later
                if odds_new_ts >= odds_old_ts:
                    chosen = {"event_id": r["event_id"], **{
                    k.replace("_new", ""): r[k] for k in r.index if k.endswith("_new")
                    }}
                else:
                    chosen = {"event_id": r["event_id"], **{
                    k.replace("_old", ""): r[k] for k in r.index if k.endswith("_old")
                    }}
        except Exception:
            # fallback prefer new
            chosen = {"event_id": r["event_id"], **{
                    k.replace("_new", ""): r[k] for k in r.index if k.endswith("_new")
                    }}
        chosen_rows.append(chosen)

    final = pd.DataFrame(chosen_rows)

    # Normalize datetimes again, ensure tz-awareness
    if "match_time" in final.columns:
        final["match_time"] = pd.to_datetime(final["match_time"], utc=True).dt.tz_convert(PERTH)
    if "odds_time" in final.columns:
        final["odds_time"] = pd.to_datetime(final["odds_time"], utc=True).dt.tz_convert(PERTH)

    # Add hours_until_KO column for debugging & downstream rules
    if "match_time" in final.columns and "odds_time" in final.columns:
        final["hours_until_KO"] = (final["match_time"] - final["odds_time"]).dt.total_seconds() / 3600.0
    else:
        final["hours_until_KO"] = float("nan")

    # Order the final csv by hours_until_KO ascending
    final = final.sort_values(by="hours_until_KO", ascending=True).reset_index(drop=True)

    return final

# -------------------------------------------------------------
# Main pipeline per-league
# -------------------------------------------------------------

def process_league(api_key: str, league_cfg: dict, limit=200):
    """
    For a single league config (name, sport_key, odds_time_limit),
    fetch events, fetch odds for each event, extract markets currently (totals, btts),
    pivot, compute RPDs and save files, applying merge rules.
    """
    league_name = league_cfg["name"]
    league_key = league_cfg["sport_key"]

    print(f"\n--- Processing league: {league_name} (sport_key={league_key}) ---")

    slug = league_cfg["slug"] 
    target_hours = int(league_cfg["odds_time_limit"])
    league_bf_map = load_team_map(f"mappings/{slug}/team_name_map.json")
    # Add betfair totalmatch volume from betfair_api
    session_token = get_session_token()
    df_bf_ou_volume = get_ou_volume(session_token, league_name)
    if df_bf_ou_volume.empty:
        print(f"No Betfair OU volume data for league {league_name}")

    # might be able to 
    events = get_league_events(api_key, league_key, limit=limit)
    if not events:
        print(f"No events fetched for {league_name}")
        return
    df_events = extract_events_to_df(events)
    if df_events.empty:
        print(f"No event rows for {league_name}")
        return
    
    totals_rows = []
    btts_rows = []

    # Iterate events and fetch odds
    for _, row in df_events.iterrows():
        event_id = row["event_id"]
        print(f"Fetching odds for {row['home_team']} vs {row['away_team']} (event_id={event_id})")
        odds_data = get_event_odds(api_key, event_id)
        if not odds_data:
            continue

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

    df_totals_all = pd.DataFrame(totals_rows)
    df_btts = pd.DataFrame(btts_rows)

    # Prepare export folders
    totals_folder = os.path.join(EXPORT_ROOT, slug, "totals")
    btts_folder = os.path.join(EXPORT_ROOT, slug, "btts")
    ensure_dir(totals_folder)
    ensure_dir(btts_folder)

    totals_exports = []
    btts_export = None
    # Save BTTS straightforwardly (merge-by-event not required here, but could be added)
    if not df_btts.empty:
        btts_out_path = todays_filename(btts_folder, "btts")
        # Convert tz-aware datetimes to ISO strings for CSV
        df_btts_copy = df_btts.copy()
        # pivot btts
        idbc = ['event_id', 'home_team', 'away_team', 'competition', 'match_time', 'bookmaker', 'odds_time']
        vc = ['no_odds', 'yes_odds']
        df_btts_pivoted = pivot_odds_dataframe(df_btts_copy, idbc, vc)
        # compute RPD
        df_btts_final = compute_rpds(df_btts_pivoted, btts=True)
        # Normalize datetimes to ISO strings for CSV writing, but keep tz-aware datetimes for merging
        df_for_merge = df_btts_final.copy()
        # ensure match_time and odds_time are tz-aware datetimes
        if "match_time" in df_for_merge.columns and df_for_merge["match_time"].dtype == object:
            df_for_merge["match_time"] = pd.to_datetime(df_for_merge["match_time"], utc=True).dt.tz_convert(PERTH)
        if "odds_time" in df_for_merge.columns and df_for_merge["odds_time"].dtype == object:
            df_for_merge["odds_time"] = pd.to_datetime(df_for_merge["odds_time"], utc=True).dt.tz_convert(PERTH)
        # existing file path (today's)
        out_path = todays_filename(btts_folder, f"btts")
        existing = load_existing_csv(out_path)
        # merge in total volume from betfair
        df_bf_btts = df_for_merge.copy()
        df_bf_btts['bf_team_name_home'] = df_bf_btts['home_team'].map(league_bf_map)
        df_bf_btts['bf_team_name_away'] = df_bf_btts['away_team'].map(league_bf_map)
        df_bf_btts['bf_merge_key'] = df_bf_btts['bf_team_name_home'] + " v " + df_bf_btts['bf_team_name_away']
        # you want to check all the df_bf_ou_volume event keys have beem mapped and exist in the df_bf_btts['bf_merge_key']
        for k in df_bf_ou_volume[df_bf_ou_volume.line == BF_BTTS_MARKET_NAME].event.unique():
            if k not in df_bf_btts['bf_merge_key'].unique():
                print(f"[WARN] Betfair BTTS volume event key '{k}' has no matching event in BTTS data")

        df_bf_btts_tv = df_bf_btts.merge(df_bf_ou_volume[df_bf_ou_volume.line == BF_BTTS_MARKET_NAME], how='inner', left_on='bf_merge_key', right_on='event')

        merged_final = decide_merge(existing, df_bf_btts_tv, target_hours)
        btts_export = merged_final
        merged_final.to_csv(btts_out_path, index=False)
        print(f"Saved BTTS: {btts_out_path}")
    else:
        print("No BTTS data for this run.")

    # Totals: process per HDP, pivot, compute RPD and merge with existing
    if not df_totals_all.empty:
        for hdp in TARGET_HDPS:
            # BF MARKET NAME for this hdp
            bf_market_name = BF_TOTALS_MARKET_NAME.get(hdp)
            df_hdp = df_totals_all[df_totals_all["hdp"] == hdp].copy()
            idtc = ["event_id", "home_team", "away_team", "competition", "match_time", "hdp", "odds_time"]
            vc = ["over_odds", "under_odds"]
            df_totals_pivoted = pivot_odds_dataframe(df_hdp, idtc, vc)
            if df_totals_pivoted.empty:
                print(f"No totals for hdp={hdp}")
                continue

            # compute RPD
            df_totals_final = compute_rpds(df_totals_pivoted)
            # Normalize datetimes to ISO strings for CSV writing, but keep tz-aware datetimes for merging
            df_for_merge = df_totals_final.copy()
            # ensure match_time and odds_time are tz-aware datetimes
            if "match_time" in df_for_merge.columns and df_for_merge["match_time"].dtype == object:
                df_for_merge["match_time"] = pd.to_datetime(df_for_merge["match_time"], utc=True).dt.tz_convert(PERTH)
            if "odds_time" in df_for_merge.columns and df_for_merge["odds_time"].dtype == object:
                df_for_merge["odds_time"] = pd.to_datetime(df_for_merge["odds_time"], utc=True).dt.tz_convert(PERTH)

            # existing file path (today's)
            out_path = todays_filename(totals_folder, f"totals_hdp_{hdp}")
            existing = load_existing_csv(out_path)

            # merge in total volume from betfair
            df_bf_total = df_for_merge.copy()
            df_bf_total['bf_team_name_home'] = df_bf_total['home_team'].map(league_bf_map)
            df_bf_total['bf_team_name_away'] = df_bf_total['away_team'].map(league_bf_map)
            df_bf_total['bf_merge_key'] = df_bf_total['bf_team_name_home'] + " v " + df_bf_total['bf_team_name_away']
            # you want to check all the df_bf_ou_volume event keys have beem mapped and exist in the df_bf_btts['bf_merge_key']
            for k in df_bf_ou_volume[df_bf_ou_volume.line == bf_market_name].event.unique():
                if k not in df_bf_total['bf_merge_key'].unique():
                    print(f"[WARN] Betfair BTTS volume event key '{k}' has no matching event in Total {hdp} data")

            df_bf_total_tv = df_bf_total.merge(df_bf_ou_volume[df_bf_ou_volume.line == bf_market_name], how='inner', left_on='bf_merge_key', right_on='event')

            merged_final = decide_merge(existing, df_bf_total_tv, target_hours)
            totals_exports.append(merged_final)
            merged_final.to_csv(out_path, index=False)
            print(f"Saved totals (hdp={hdp}): {out_path}")
    else:
        print("No totals data found for this run.")

    # Combine totals across all HDPs
    df_totals_export = pd.concat(totals_exports, ignore_index=True) if totals_exports else pd.DataFrame()
    df_btts_export = btts_export if btts_export is not None else pd.DataFrame()

    return df_totals_export, df_btts_export

# -------------------------------------------------------------
# Main entrypoint
# -------------------------------------------------------------

def main():
    api_key = load_env()
    cfg = load_config()

    all_totals = []
    all_btts = []

    for league in cfg["leagues"]:
        try:
            df_totals_export, df_btts_export = process_league(api_key, league, limit=10)

            # Skip if both are None or empty
            if (df_totals_export is None or df_totals_export.empty) and (df_btts_export is None or df_btts_export.empty):
                print(f"No data for league {league.get('name')}, skipping.")
                continue

            # Append to global lists for downstream pipeline
            all_totals.append(df_totals_export)
            all_btts.append(df_btts_export)

            # per-league postprocessing
            ready_games_workbook_path = run_postprocessing_and_exports(league['slug'], df_totals_export, df_btts_export, target_hours=league.get("odds_time_limit", 9))

            # email ready games
            if ready_games_workbook_path: 
                if os.path.exists(ready_games_workbook_path):
                    success = email_workbook(
                        ready_games_workbook_path,
                        subject=f"Ready Games - {league['name']}",
                        body="Attached is today's ready games workbook.",
                    )
                    if not success:
                        print(f"Failed to email workbook")
                    else:
                        sent_dir = Path("/Users/notbahd/Desktop/BookieGrabber/data/ready/sent")
                        sent_dir.mkdir(parents=True, exist_ok=True)

                        dest = sent_dir / Path(ready_games_workbook_path).name
                        shutil.move(ready_games_workbook_path, dest)
                        print(f"Workbook emailed and moved to: {dest}")
                else:
                    print(f"Ready games workbook does not exist: {ready_games_workbook_path}")

        except Exception as exc:
            print(f"League {league.get('name')} failed: {exc}")

    # df_totals_all = pd.concat(all_totals, ignore_index=True) if all_totals else pd.DataFrame()
    # df_btts_all = pd.concat(all_btts, ignore_index=True) if all_btts else pd.DataFrame()

    # Feed to prod pipeline
        # Combine all leagues
        # Take the latest totals + BTTS DataFrames (df_totals_all, df_btts_all).
        # Store them somewhere:
        # Database
        # Data lake / S3
        # Internal cache for downstream models
        # Possibly clean, normalize, or validate the data.
    # ingest_new_data(df_totals_all, df_btts_all)
        # This is the next step in your production workflow. Its purpose is to:
        # Take the ingested, cleaned data
        # Prepare it in the format your downstream consumers need:
        # ML models
        # Dashboards
        # Reporting
        # Trigger any downstream actions, e.g., retraining models, refreshing dashboards, etc.
    # export_model_ready()

if __name__ == "__main__":
    main()