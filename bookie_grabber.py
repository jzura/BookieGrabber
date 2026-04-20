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
from datetime import datetime, timedelta, timezone
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
    end_dt = (
            datetime.now(timezone.utc)
            .date() + timedelta(days=4)
        )

    end_rfc3339 = datetime(
            end_dt.year,
            end_dt.month,
            end_dt.day,
            23, 59, 59,
            tzinfo=timezone.utc
        ).isoformat().replace("+00:00", "Z")

    params = {"apiKey": api_key, "sport": "football", "league": league, "limit": limit, "to": end_rfc3339}
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
    """Fetch odds data for a specific event from Odds-API (single attempt).
    Higher-level code retries the entire event-bundle if data is incomplete."""
    url = f"{API_BASE}/odds"
    params = {"apiKey": api_key, "eventId": event_id, "bookmakers": BOOKMAKERS}
    try:
        r = requests.get(url, params=params, timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"  [ODDS-API] Error for event {event_id}: {e}")
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
            "event_id": str(e.get("id")),
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
            markets = b.get("markets") or []
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
            markets = b.get("markets") or []
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

    # Pivot on event identity only — odds_time is metadata, not a grouping key.
    # Including it would split rows when timestamps differ by microseconds
    # (e.g. Bet365 vs BF from different sources within the same bundle).
    pivot_index = ["event_id", "home_team", "away_team", "competition", "match_time"]
    df_pivot = df_long.pivot_table(
        index=pivot_index,
        columns="col_name",
        values="odds_value",
        aggfunc="first"
    ).reset_index()

    # Restore odds_time as a column (use the latest value per event)
    if "odds_time" in df_totals.columns:
        ot = df_totals.groupby("event_id")["odds_time"].max().reset_index()
        df_pivot = df_pivot.merge(ot, on="event_id", how="left")

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
        # Ensure event_id is always string (prevents int64/str merge crashes)
        if "event_id" in df.columns:
            df["event_id"] = df["event_id"].astype(str)
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

    # Columns that exist in only one DataFrame don't get a suffix from the merge.
    # We track them separately so we can include them when picking from that side.
    only_in_existing = set(existing.columns) - set(new.columns) - {"event_id"}
    only_in_new = set(new.columns) - set(existing.columns) - {"event_id"}

    def _pick(r, suffix):
        """Pick row fields for the chosen side (suffix='_old' or '_new').

        IMPORTANT: We use ONLY the chosen side's values — never mix old and new
        values for the same event. Mixing would combine prices/volumes captured at
        different points in time, which corrupts the snapshot atomicity. If a
        value is NaN/missing on the chosen side, it stays NaN.
        """
        out = {"event_id": r["event_id"]}
        # Suffixed columns: take from the chosen side only
        for k in r.index:
            if k.endswith(suffix):
                out[k[:-len(suffix)]] = r[k]
        # Unsuffixed columns: include only from the matching side
        # (columns only_in_existing belong to old, only_in_new belong to new)
        unsuffixed = only_in_existing if suffix == "_old" else only_in_new
        for col in unsuffixed:
            if col in r.index:
                out[col] = r[col]
        return out

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
            chosen_rows.append(_pick(r, "_new"))
            continue

        # If only old exists
        if pd.isna(match_new) and not pd.isna(match_old):
            chosen_rows.append(_pick(r, "_old"))
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
            chosen_rows.append(_pick(r, "_old"))
            continue

        # 2) Else if new is in the window -> use new (we want the first/any snapshot inside window)
        if new_in_window:
            chosen_rows.append(_pick(r, "_new"))
            continue

        # 3) Neither is in the window -> pick the later odds_time (newer snapshot)
        try:
            odds_old_ts = pd.to_datetime(odds_old) if odds_old is not None else pd.NaT
            odds_new_ts = pd.to_datetime(odds_new) if odds_new is not None else pd.NaT
            if pd.isna(odds_old_ts) and not pd.isna(odds_new_ts):
                chosen_rows.append(_pick(r, "_new"))
            elif pd.isna(odds_new_ts) and not pd.isna(odds_old_ts):
                chosen_rows.append(_pick(r, "_old"))
            else:
                # choose new if it's equal or later
                if odds_new_ts >= odds_old_ts:
                    chosen_rows.append(_pick(r, "_new"))
                else:
                    chosen_rows.append(_pick(r, "_old"))
        except Exception:
            # fallback prefer new
            chosen_rows.append(_pick(r, "_new"))

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
# Mapping diagnostic — flag unmapped/mismatched team names
# -------------------------------------------------------------

def diagnose_team_mappings(league_name, odds_api_events, df_bf_ou_volume, league_bf_map):
    """Compare BF's event list against Odds API events using the current
    mapping, then report any unmapped teams with fuzzy-match suggestions.

    Each upcoming BF event SHOULD match an Odds API event — if it doesn't,
    either a team mapping is missing or wrong.
    """
    if df_bf_ou_volume.empty:
        return  # nothing to compare against

    from difflib import get_close_matches

    # Build sets of unique team names from each side
    oa_home_teams = set(e.get("home", "") for e in odds_api_events if e.get("home"))
    oa_away_teams = set(e.get("away", "") for e in odds_api_events if e.get("away"))
    oa_all_teams = oa_home_teams | oa_away_teams

    # BF event keys are "Home v Away" — split them out
    bf_team_set = set()
    for evt in df_bf_ou_volume["event"].unique():
        if " v " in evt:
            h, a = evt.split(" v ", 1)
            bf_team_set.add(h.strip())
            bf_team_set.add(a.strip())

    # Build set of all currently-mapped BF names (from the OA → BF mapping values)
    mapped_bf_names = set(v for v in league_bf_map.values() if v)

    # Find BF teams that are NOT in any current mapping value
    unmapped_bf_teams = bf_team_set - mapped_bf_names

    if not unmapped_bf_teams:
        return

    # For each unmapped BF team, suggest the best Odds API team match
    suggestions = []
    for bf_team in sorted(unmapped_bf_teams):
        candidates = get_close_matches(bf_team, oa_all_teams, n=2, cutoff=0.5)
        suggestions.append((bf_team, candidates))

    print(f"  [MAPPING-DIAG] {len(unmapped_bf_teams)} BF team(s) not in mapping for {league_name}:")
    for bf_team, candidates in suggestions:
        suggestion = candidates[0] if candidates else "(no close match)"
        print(f'    "{suggestion}": "{bf_team}",   <- BF team "{bf_team}" — closest OA: {candidates}')

    # Also flag any mapping values pointing to non-existent BF names
    bad_mappings = []
    for oa_name, bf_name in league_bf_map.items():
        if not bf_name:
            bad_mappings.append((oa_name, "EMPTY"))
        elif bf_name not in bf_team_set:
            # Only flag if this OA team is actually upcoming — otherwise BF
            # just doesn't list distant fixtures yet (false positive)
            if oa_name in oa_all_teams:
                # Check if any other mapping for this OA team points to a known BF team
                # (some mappings have multiple OA names → same BF name; ignore good ones)
                # Suggest closest BF match
                close = get_close_matches(bf_name, bf_team_set, n=1, cutoff=0.5)
                bad_mappings.append((oa_name, f"{bf_name!r} not in BF (close: {close})"))
    if bad_mappings:
        print(f"  [MAPPING-DIAG] {len(bad_mappings)} mapping(s) point to wrong/missing BF names:")
        for oa, issue in bad_mappings[:10]:
            print(f'    "{oa}" -> {issue}')


# -------------------------------------------------------------
# Atomic per-event bundle fetcher with cross-run attempt tracking
# -------------------------------------------------------------

# Each scheduled run is one attempt. After MAX_ATTEMPTS_PER_EVENT failures
# (across runs, ~30min apart), the event is given up on. State is persisted
# to disk so attempts survive across runs.
MAX_ATTEMPTS_PER_EVENT = 3
BUNDLE_TIME_BUDGET_SEC = 120  # all data must be collected within 2 minutes
ATTEMPT_STATE_FILE = "data/state/fetch_attempts.json"
ATTEMPT_STATE_TTL_HOURS = 48  # prune stale entries after this long


def _load_attempt_state():
    if not os.path.exists(ATTEMPT_STATE_FILE):
        return {}
    try:
        with open(ATTEMPT_STATE_FILE) as f:
            return json.load(f)
    except Exception as e:
        print(f"[WARN] Could not load attempt state: {e}")
        return {}


def _save_attempt_state(state):
    os.makedirs(os.path.dirname(ATTEMPT_STATE_FILE), exist_ok=True)
    with open(ATTEMPT_STATE_FILE, "w") as f:
        json.dump(state, f, indent=2, default=str)


def _prune_stale_attempts(state):
    """Remove entries older than ATTEMPT_STATE_TTL_HOURS so the file
    doesn't grow forever (events that have already played etc.)."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=ATTEMPT_STATE_TTL_HOURS)
    pruned = {}
    for eid, info in state.items():
        try:
            last = parser.isoparse(info.get("last_attempt", info.get("first_attempt", "")))
            if last.tzinfo is None:
                last = last.replace(tzinfo=timezone.utc)
            if last >= cutoff:
                pruned[eid] = info
        except Exception:
            # Bad entry — drop it
            pass
    return pruned


def _bundle_is_complete(totals, btts, has_bf_volume, bf_has_btts_for_event):
    """A bundle is complete when:
      - Bet365 has at least one of our target HDPs (1.5/2.5/3.5)
      - Betfair Exchange has at least one of our target HDPs
      - Bet365 has BTTS data
      - Betfair Exchange has BTTS data (ONLY if BF lists BTTS for this event)
      - Betfair total volume is present (ONLY if BF lists volume for this event)
    """
    bm_hdps_totals = {}
    for t in totals:
        bm_hdps_totals.setdefault(t["bookmaker"], set()).add(t["hdp"])
    bm_btts = set(b["bookmaker"] for b in btts)

    if not bm_hdps_totals.get("Bet365"):
        return False, "missing Bet365 totals"
    if not bm_hdps_totals.get("Betfair Exchange"):
        return False, "missing Betfair Exchange totals"
    if "Bet365" not in bm_btts:
        return False, "missing Bet365 BTTS"
    # Only require BF BTTS if BF actually lists a BTTS market for this event
    if bf_has_btts_for_event and "Betfair Exchange" not in bm_btts:
        return False, "missing Betfair Exchange BTTS"
    if not has_bf_volume:
        return False, "missing Betfair volume"
    return True, "complete"


IN_RUN_ATTEMPTS = 3  # tries per scheduled run before giving up for this run
IN_RUN_BACKOFF_SEC = [10, 20]  # wait between in-run attempts


def _fetch_event_bundle_once(
    api_key,
    event_row,
    league_bf_map,
    bf_session_token,
    league_name,
):
    """Atomic fetch of all data for one event within a single scheduled run.

    ALL sources (Odds API odds, BF Exchange odds, BF volume) are fetched
    together within each attempt so everything is from the same point in time.
    Tries up to IN_RUN_ATTEMPTS times. Each attempt must complete within
    BUNDLE_TIME_BUDGET_SEC (2 minutes). Returns
    (bundle_dict_or_None, reason_string).
    """
    import time

    bf_home = league_bf_map.get(event_row["home_team"])
    bf_away = league_bf_map.get(event_row["away_team"])
    bf_event_key = f"{bf_home} v {bf_away}" if bf_home and bf_away else None

    last_reason = "no attempt"

    for attempt in range(1, IN_RUN_ATTEMPTS + 1):
        start = time.time()

        # 1. Odds API call (returns Bet365 + Betfair Exchange together)
        odds_data = get_event_odds(api_key, event_row["event_id"])
        totals = extract_totals(odds_data) if odds_data else []
        btts = extract_btts(odds_data) if odds_data else []

        # 2. BF volume + catalogue (fetched fresh each attempt for synchronisation)
        df_bf_ou_volume = pd.DataFrame()
        bf_market_catalogue = []
        if bf_session_token:
            try:
                df_bf_ou_volume, bf_market_catalogue = get_ou_volume(
                    bf_session_token, league_name, max_attempts=1)
            except Exception as e:
                print(f"  [BF] Volume fetch failed: {e}")

        # 3. BF direct fallback for any missing Betfair Exchange odds
        bf_in_totals = any(t["bookmaker"] == "Betfair Exchange" for t in totals)
        bf_in_btts = any(b["bookmaker"] == "Betfair Exchange" for b in btts)
        if (not bf_in_totals or not bf_in_btts) and bf_event_key and bf_session_token:
            try:
                fb_totals, fb_btts = fetch_bf_odds_for_event(
                    bf_session_token, bf_event_key, bf_market_catalogue)
                if not bf_in_totals and fb_totals:
                    totals.extend(fb_totals)
                    print(f"  [BF FALLBACK] Injected {len(fb_totals)} totals lines")
                if not bf_in_btts and fb_btts:
                    btts.extend(fb_btts)
                    print(f"  [BF FALLBACK] Injected {len(fb_btts)} BTTS lines")
            except Exception as e:
                print(f"  [BF FALLBACK] Failed: {e}")

        # 4. Check which BF markets exist for this event
        bf_volume_available_for_event = False
        bf_has_btts_for_event = False
        if bf_event_key and not df_bf_ou_volume.empty:
            event_markets = df_bf_ou_volume[df_bf_ou_volume["event"] == bf_event_key]
            bf_volume_available_for_event = not event_markets.empty
            bf_has_btts_for_event = (event_markets["line"] == "Both teams to Score?").any()

        elapsed = time.time() - start

        # 5. Wall-clock budget check — all data must arrive within 2 minutes
        if elapsed > BUNDLE_TIME_BUDGET_SEC:
            last_reason = f"took {elapsed:.1f}s (>{BUNDLE_TIME_BUDGET_SEC}s budget)"
            print(f"  [ATOMIC] Attempt {attempt}/{IN_RUN_ATTEMPTS}: {last_reason}")
        else:
            # 6. Completeness check
            complete, reason = _bundle_is_complete(
                totals, btts, has_bf_volume=bf_volume_available_for_event,
                bf_has_btts_for_event=bf_has_btts_for_event
            )
            if complete:
                if attempt > 1:
                    print(f"  [ATOMIC] Bundle complete on in-run attempt {attempt} ({elapsed:.1f}s)")
                return {"totals": totals, "btts": btts,
                        "df_bf_ou_volume": df_bf_ou_volume}, f"complete in {elapsed:.1f}s"
            last_reason = reason
            print(f"  [ATOMIC] Attempt {attempt}/{IN_RUN_ATTEMPTS}: incomplete ({reason}, {elapsed:.1f}s)")

        if attempt < IN_RUN_ATTEMPTS:
            time.sleep(IN_RUN_BACKOFF_SEC[min(attempt - 1, len(IN_RUN_BACKOFF_SEC) - 1)])

    return None, last_reason


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
    # Get BF session + initial volume snapshot (used for mapping diagnostic only;
    # actual volume for each event is fetched atomically inside the bundle fetcher)
    try:
        session_token = get_session_token()
        df_bf_diag, _ = get_ou_volume(session_token, league_name)
    except Exception as e:
        print(f"Betfair API failed for {league_name}: {e}")
        session_token = None
        df_bf_diag = pd.DataFrame()

    events = get_league_events(api_key, league_key, limit=limit)
    if not events:
        print(f"No events fetched for {league_name}")
        return pd.DataFrame(), pd.DataFrame()
    df_events = extract_events_to_df(events)
    if df_events.empty:
        print(f"No event rows for {league_name}")
        return pd.DataFrame(), pd.DataFrame()

    # Diagnostic: report any BF teams that aren't in the mapping, with suggestions
    diagnose_team_mappings(league_name, events, df_bf_diag, league_bf_map)

    totals_rows = []
    btts_rows = []
    bundle_volumes = []  # collect per-event BF volume DataFrames (fetched atomically with odds)

    # Load existing CSVs to find events already recorded in-window (locked)
    locked_event_ids = set()
    totals_folder = os.path.join(EXPORT_ROOT, slug, "totals")
    btts_folder = os.path.join(EXPORT_ROOT, slug, "btts")
    for hdp in TARGET_HDPS:
        existing_path = todays_filename(totals_folder, f"totals_hdp_{hdp}")
        existing_df = load_existing_csv(existing_path)
        if not existing_df.empty and "hours_until_KO" in existing_df.columns:
            in_window = existing_df[existing_df["hours_until_KO"] <= target_hours]
            locked_event_ids.update(in_window["event_id"].astype(str).tolist())
    existing_btts_path = todays_filename(btts_folder, "btts")
    existing_btts = load_existing_csv(existing_btts_path)
    if not existing_btts.empty and "hours_until_KO" in existing_btts.columns:
        in_window = existing_btts[existing_btts["hours_until_KO"] <= target_hours]
        locked_event_ids.update(in_window["event_id"].astype(str).tolist())
    if locked_event_ids:
        print(f"  {len(locked_event_ids)} events already locked in-window — will not re-fetch")

    # Load cross-run attempt state — events that have failed across runs
    attempt_state = _load_attempt_state()
    attempt_state = _prune_stale_attempts(attempt_state)

    # Iterate events and fetch odds atomically
    for _, row in df_events.iterrows():
        event_id = str(row["event_id"])  # JSON keys are strings

        # Skip events outside the collection window
        match_time = row["match_time"]
        now = datetime.now(PERTH)
        hours_until_ko = float("inf")
        if match_time is not None:
            if match_time < now:
                continue  # already started
            hours_until_ko = (match_time - now).total_seconds() / 3600.0
            if hours_until_ko > target_hours:
                continue  # too far away

        # Skip events already locked (in-window odds already recorded)
        if event_id in locked_event_ids:
            continue

        print(f"Fetching odds for {row['home_team']} vs {row['away_team']} "
              f"(event_id={event_id}, KO in {hours_until_ko:.1f}h)")

        # Cross-run check: skip events that have already exhausted their run attempts
        prior_runs = attempt_state.get(event_id, {}).get("run_attempts", 0)
        if prior_runs >= MAX_ATTEMPTS_PER_EVENT:
            print(f"  [SKIP-EXHAUSTED] Event {event_id} has failed {prior_runs} runs — giving up permanently")
            continue

        bundle, reason = _fetch_event_bundle_once(
            api_key=api_key,
            event_row=row,
            league_bf_map=league_bf_map,
            bf_session_token=session_token,
            league_name=league_name,
        )

        now_iso = datetime.now(timezone.utc).isoformat()
        if bundle is None:
            # Record this run as a failed attempt; will retry next scheduled run
            entry = attempt_state.setdefault(event_id, {
                "first_attempt": now_iso,
                "run_attempts": 0,
                "home_team": row["home_team"],
                "away_team": row["away_team"],
            })
            entry["run_attempts"] = entry.get("run_attempts", 0) + 1
            entry["last_attempt"] = now_iso
            entry["last_reason"] = reason
            remaining = MAX_ATTEMPTS_PER_EVENT - entry["run_attempts"]
            if remaining > 0:
                print(f"  [SKIP] Bundle incomplete ({reason}). Run {entry['run_attempts']}/{MAX_ATTEMPTS_PER_EVENT} — will retry next run")
            else:
                print(f"  [GIVE-UP] Bundle incomplete ({reason}). All {MAX_ATTEMPTS_PER_EVENT} run attempts exhausted")
            _save_attempt_state(attempt_state)
            continue

        # Success — clear any prior failure tracking for this event
        if event_id in attempt_state:
            del attempt_state[event_id]
            _save_attempt_state(attempt_state)

        totals = bundle["totals"]
        btts = bundle["btts"]
        if not bundle["df_bf_ou_volume"].empty:
            bundle_volumes.append(bundle["df_bf_ou_volume"])

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

    # Combine per-event BF volumes (fetched atomically with odds)
    df_bf_ou_volume = pd.concat(bundle_volumes, ignore_index=True).drop_duplicates(
        subset=["marketId"]) if bundle_volumes else pd.DataFrame()

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
        idbc = ['event_id', 'home_team', 'away_team', 'competition', 'match_time', 'odds_time']
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
        bf_btts_vol = df_bf_ou_volume[df_bf_ou_volume.line == BF_BTTS_MARKET_NAME] if not df_bf_ou_volume.empty else pd.DataFrame()
        if not bf_btts_vol.empty:
            for k in bf_btts_vol.event.unique():
                if k not in df_bf_btts['bf_merge_key'].unique():
                    print(f"[WARN] Betfair BTTS volume event key '{k}' has no matching event in BTTS data")
            # Left merge so we keep events without BF volume (would otherwise lose Bet365 odds)
            df_bf_btts_tv = df_bf_btts.merge(bf_btts_vol, how='left', left_on='bf_merge_key', right_on='event')
        else:
            df_bf_btts_tv = df_bf_btts

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
            bf_total_vol = df_bf_ou_volume[df_bf_ou_volume.line == bf_market_name] if not df_bf_ou_volume.empty else pd.DataFrame()
            if not bf_total_vol.empty:
                for k in bf_total_vol.event.unique():
                    if k not in df_bf_total['bf_merge_key'].unique():
                        print(f"[WARN] Betfair volume event key '{k}' has no matching event in Total {hdp} data")
                # Left merge so we keep events without BF volume (would otherwise lose Bet365 odds)
                df_bf_total_tv = df_bf_total.merge(bf_total_vol, how='left', left_on='bf_merge_key', right_on='event')
            else:
                df_bf_total_tv = df_bf_total

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

def send_failure_alert(subject, body):
    """Send a failure alert email so the user knows something broke."""
    try:
        import smtplib
        from email.message import EmailMessage

        email_user = os.environ.get("EMAIL_USER")
        email_pass = os.environ.get("EMAIL_PASS")
        email_to = os.environ.get("EMAIL_TO", "")
        recipients = [e.strip() for e in email_to.split(",") if e.strip()]

        if not email_user or not email_pass or not recipients:
            return

        msg = EmailMessage()
        msg["From"] = email_user
        msg["To"] = ", ".join(recipients)
        msg["Subject"] = subject
        msg.set_content(body)

        with smtplib.SMTP("smtp.mail.me.com", 587, timeout=15) as server:
            server.starttls()
            server.login(email_user, email_pass)
            server.send_message(msg)
    except Exception:
        pass  # can't send alert about failure to send alerts


def main():
    api_key = load_env()
    cfg = load_config()

    all_totals = []
    all_btts = []
    total_leagues = len(cfg["leagues"])
    failed_leagues = []
    succeeded = 0

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
                        sent_dir = Path("/Users/Joel/REPOS/BookieGrabber/data/ready/sent")
                        sent_dir.mkdir(parents=True, exist_ok=True)

                        dest = sent_dir / Path(ready_games_workbook_path).name
                        shutil.move(ready_games_workbook_path, dest)
                        print(f"Workbook emailed and moved to: {dest}")
                else:
                    print(f"Ready games workbook does not exist: {ready_games_workbook_path}")

            succeeded += 1

        except Exception as exc:
            print(f"League {league.get('name')} failed: {exc}")
            failed_leagues.append((league.get("name"), str(exc)))

    # Send failure alert if too many leagues failed
    if failed_leagues:
        fail_rate = len(failed_leagues) / total_leagues
        # Alert if all leagues failed, or more than half failed
        if fail_rate >= 0.5:
            lines = [f"{name}: {err}" for name, err in failed_leagues]
            body = (
                f"Pipeline run completed with {len(failed_leagues)}/{total_leagues} leagues failing "
                f"({succeeded} succeeded).\n\n"
                f"Failed leagues:\n" + "\n".join(lines) + "\n\n"
                f"Check logs at ~/REPOS/BookieGrabber/logs/ for details."
            )
            send_failure_alert(
                f"BookieGrabber ALERT — {len(failed_leagues)}/{total_leagues} leagues failed",
                body,
            )

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
    try:
        main()
    except Exception as exc:
        import traceback
        tb = traceback.format_exc()
        print(f"FATAL: Pipeline crashed: {exc}\n{tb}")
        send_failure_alert(
            "BookieGrabber ALERT — Pipeline crashed",
            f"The pipeline crashed and did not complete.\n\n"
            f"Error: {exc}\n\n{tb}\n\n"
            f"The hourly launchd job will retry next hour, but you may need to check the machine.",
        )