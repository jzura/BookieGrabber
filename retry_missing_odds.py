"""
Retry fetching missing Bet365 odds for matches that couldn't be inserted
into the master spreadsheet during the main pipeline run.

Runs every 15 minutes via launchd. Processes pending_retries.json:
1. For each entry, re-query the Odds API for that event
2. If Bet365 now quotes the missing line, build a minimal ready-games
   DataFrame and feed it through update_master_from_dataframes() — which
   handles insertion, formulas, formatting, stake alerts, etc.
3. Remove successfully-inserted entries from the pending file
4. Discard entries whose match has already kicked off (stale)

This is a targeted retry — only events that were missing odds get re-fetched.
Safe to run concurrently with the hourly bookie_grabber pipeline.
"""

import json
import logging
import os
import sys
from datetime import datetime, date, timedelta
from pathlib import Path

import pandas as pd
import pytz
from dotenv import load_dotenv

from constants import PROJECT_ROOT
sys.path.insert(0, str(PROJECT_ROOT))

# Reuse the existing API fetchers
from bookie_grabber import (
    get_event_odds,
    extract_totals,
    extract_btts,
    PERTH,
    BF_BTTS_MARKET_NAME,
    BF_TOTALS_MARKET_NAME,
    load_team_map,
)
from betfair_api import get_session_token, get_ou_volume, fetch_bf_odds_for_event
from bet_tracker_updater import update_master_from_dataframes, PENDING_RETRIES_PATH

load_dotenv()

LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_PATH = LOG_DIR / f"retry_odds_{datetime.now().strftime('%Y-%m-%d')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(str(LOG_PATH)), logging.StreamHandler()],
    force=True,  # override any prior basicConfig from transitive imports
)
logger = logging.getLogger("retry_missing_odds")

MAX_ATTEMPTS = 4  # give up after 4 × 15-minute retries (1 hour of trying)
STALE_AFTER_HOURS = 1

TARGET_HDPS = [1.5, 2.5, 3.5]
HDP_TO_BET_TYPE = {1.5: "1.5G", 2.5: "2.5G", 3.5: "3.5G"}

READY_DIR = PROJECT_ROOT / "data" / "ready"
READY_SENT_DIR = PROJECT_ROOT / "data" / "ready" / "sent"


def lookup_volume_from_ready_files(event_id, bet_type):
    """Look up total_volume for a given event + bet type from the most recent
    ready_games file that contains it. Returns None if not found."""
    import glob
    paths = sorted(
        list(glob.glob(str(READY_SENT_DIR / "ready_games_*.xlsx"))) +
        list(glob.glob(str(READY_DIR / "ready_games_*.xlsx"))),
        key=os.path.getmtime,
        reverse=True,
    )
    target_line = BF_BTTS_MARKET_NAME if bet_type == "BTTS" else BF_TOTALS_MARKET_NAME.get(
        float(bet_type.replace("G", ""))
    )
    sheet_name = "btts_ready" if bet_type == "BTTS" else "totals_ready"

    for p in paths[:30]:  # only check 30 most recent files
        try:
            df = pd.read_excel(p, sheet_name=sheet_name)
            if df.empty or "event_id" not in df.columns:
                continue
            mask = (df["event_id"] == event_id)
            if "line" in df.columns and bet_type != "BTTS":
                mask = mask & (df["line"] == target_line)
            hits = df[mask]
            if hits.empty:
                continue
            vol = hits.iloc[0].get("total_volume")
            if vol is not None and not pd.isna(vol):
                return float(vol)
        except Exception:
            continue
    return None


# -------------------------------------------------------------
# Pending file I/O
# -------------------------------------------------------------

def load_pending():
    if not PENDING_RETRIES_PATH.exists():
        return {}
    try:
        with open(PENDING_RETRIES_PATH, "r") as f:
            return json.load(f)
    except Exception:
        logger.exception("Failed to read pending_retries.json")
        return {}


def save_pending(pending):
    try:
        with open(PENDING_RETRIES_PATH, "w") as f:
            json.dump(pending, f, indent=2, default=str)
    except Exception:
        logger.exception("Failed to write pending_retries.json")


# -------------------------------------------------------------
# Stale purge
# -------------------------------------------------------------

def is_stale(entry) -> bool:
    try:
        d = date.fromisoformat(entry["date"])
        mt_str = entry.get("match_time") or "00:00"
        h, m = map(int, mt_str.split(":"))
        kickoff = datetime.combine(d, datetime.min.time()).replace(hour=h, minute=m)
        kickoff = PERTH.localize(kickoff)
        now_perth = datetime.now(PERTH)
        return now_perth > kickoff + timedelta(hours=STALE_AFTER_HOURS)
    except Exception:
        return False


# -------------------------------------------------------------
# Build ready-games DataFrames from a single event's fresh odds
# -------------------------------------------------------------

def _parse_match_datetime(entry):
    """Reconstruct a tz-aware Perth datetime from the entry."""
    d = date.fromisoformat(entry["date"])
    mt_str = entry.get("match_time") or "00:00"
    h, m = map(int, mt_str.split(":"))
    dt = datetime.combine(d, datetime.min.time()).replace(hour=h, minute=m)
    return PERTH.localize(dt)


def build_ready_dataframes(entry, odds_data, bf_session=None, bf_catalogue=None):
    """
    Given a pending entry and fresh odds_data from the Odds API, build
    minimal (possibly single-row) totals_ready and btts_ready DataFrames
    matching the shape of the regular pipeline so update_master_from_dataframes
    can process them.

    Returns (totals_df, btts_df), either/both may be empty.
    """
    event_id = entry["event_id"]
    home = entry["home_team"]
    away = entry["away_team"]
    competition = entry.get("competition", "")
    match_dt = _parse_match_datetime(entry)
    odds_time = datetime.now(PERTH)

    missing_bt = set(entry.get("missing_bet_types", []))

    # ----- Totals -----
    totals = extract_totals(odds_data)
    btts_raw = extract_btts(odds_data)

    # BF fallback: if Odds API didn't return BF Exchange, fetch directly
    bf_in_totals = any(t["bookmaker"] == "Betfair Exchange" for t in totals)
    bf_in_btts = any(b["bookmaker"] == "Betfair Exchange" for b in btts_raw)
    if (not bf_in_totals or not bf_in_btts) and bf_session and bf_catalogue:
        # Derive slug from competition via config
        import yaml
        with open(PROJECT_ROOT / "config.yaml") as _f:
            _cfg = yaml.safe_load(_f)
        slug = ""
        for lc in _cfg.get("leagues", []):
            if lc["name"] == competition:
                slug = lc["slug"]
                break
        bf_map = load_team_map(f"mappings/{slug}/team_name_map.json") if slug else {}
        bf_home = bf_map.get(home)
        bf_away = bf_map.get(away)
        if bf_home and bf_away:
            bf_key = f"{bf_home} v {bf_away}"
            try:
                fb_totals, fb_btts = fetch_bf_odds_for_event(
                    bf_session, bf_key, bf_catalogue)
                if not bf_in_totals:
                    totals.extend(fb_totals)
                if not bf_in_btts:
                    btts_raw.extend(fb_btts)
            except Exception:
                pass

    # Consolidate by HDP: need Bet365 over/under and BF over/under
    totals_by_hdp = {}
    for t in totals:
        hdp = t.get("hdp")
        if hdp not in TARGET_HDPS:
            continue
        if hdp not in totals_by_hdp:
            totals_by_hdp[hdp] = {}
        bm = t.get("bookmaker")
        if bm == "Bet365":
            totals_by_hdp[hdp]["Bet365_over_odds"] = t.get("over_odds")
            totals_by_hdp[hdp]["Bet365_under_odds"] = t.get("under_odds")
        elif bm == "Betfair Exchange":
            totals_by_hdp[hdp]["Betfair_Exchange_over_odds"] = t.get("over_odds")
            totals_by_hdp[hdp]["Betfair_Exchange_under_odds"] = t.get("under_odds")

    totals_rows = []
    for hdp in TARGET_HDPS:
        bt = HDP_TO_BET_TYPE[hdp]
        if bt not in missing_bt:
            continue
        d = totals_by_hdp.get(hdp, {})
        bet365_over = d.get("Bet365_over_odds")
        bet365_under = d.get("Bet365_under_odds")
        bf_over = d.get("Betfair_Exchange_over_odds")
        bf_under = d.get("Betfair_Exchange_under_odds")
        if not (bet365_over and bet365_under and bf_over and bf_under):
            # Still missing something — skip this one, it'll be retried next cycle
            continue

        # Compute RPDs
        over_rpd = _compute_rpd(bet365_over, bf_over)
        under_rpd = _compute_rpd(bet365_under, bf_under)

        vol = lookup_volume_from_ready_files(event_id, bt) or 0

        totals_rows.append({
            "event_id": event_id,
            "home_team": home,
            "away_team": away,
            "competition": competition,
            "match_time": match_dt,
            "odds_time": odds_time,
            "Bet365_over_odds": float(bet365_over),
            "Bet365_under_odds": float(bet365_under),
            "Betfair_Exchange_over_odds": float(bf_over),
            "Betfair_Exchange_under_odds": float(bf_under),
            "Over RPD": over_rpd,
            "Under RPD": under_rpd,
            "line": BF_TOTALS_MARKET_NAME[hdp],
            "total_volume": vol,
        })

    totals_df = pd.DataFrame(totals_rows)

    # ----- BTTS -----
    btts_rows = []
    if "BTTS" in missing_bt:
        btts_by_bm = {}
        for b in btts_raw:
            bm = b.get("bookmaker")
            btts_by_bm[bm] = b
        bet365_b = btts_by_bm.get("Bet365", {})
        bf_b = btts_by_bm.get("Betfair Exchange", {})
        y365 = bet365_b.get("yes")
        n365 = bet365_b.get("no")
        ybf = bf_b.get("yes")
        nbf = bf_b.get("no")
        if y365 and n365 and ybf and nbf:
            yes_rpd = _compute_rpd(y365, ybf)
            no_rpd = _compute_rpd(n365, nbf)
            vol = lookup_volume_from_ready_files(event_id, "BTTS") or 0
            btts_rows.append({
                "event_id": event_id,
                "home_team": home,
                "away_team": away,
                "competition": competition,
                "match_time": match_dt,
                "odds_time": odds_time,
                "Bet365_yes_odds": float(y365),
                "Bet365_no_odds": float(n365),
                "Betfair_Exchange_yes_odds": float(ybf),
                "Betfair_Exchange_no_odds": float(nbf),
                "Yes RPD": yes_rpd,
                "No RPD": no_rpd,
                "line": BF_BTTS_MARKET_NAME,
                "total_volume": vol,
            })

    btts_df = pd.DataFrame(btts_rows)
    return totals_df, btts_df


def _compute_rpd(a, b):
    try:
        a, b = float(a), float(b)
        if a > b:
            return 1.0
        pct = abs(a - b) / ((a + b) / 2) * 100
        return 1.0 if pct < 1 else round(pct, 3)
    except (ValueError, TypeError, ZeroDivisionError):
        return None


# -------------------------------------------------------------
# Main retry loop
# -------------------------------------------------------------

def main():
    pending = load_pending()
    if not pending:
        logger.info("No pending retries — exiting")
        return

    logger.info(f"Processing {len(pending)} pending retry entries")

    api_key = os.getenv("ODDS_API_KEY")
    if not api_key:
        logger.error("ODDS_API_KEY missing — cannot retry")
        return

    # Get Betfair session + catalogue for fallback (one session for entire run)
    bf_session = None
    bf_catalogues = {}  # keyed by competition name
    try:
        bf_session = get_session_token()
    except Exception as e:
        logger.warning(f"BF session login failed (fallback unavailable): {e}")

    still_pending = {}

    for key, entry in pending.items():
        label = f"{entry.get('home_team')} vs {entry.get('away_team')}"

        if is_stale(entry):
            logger.info(f"[STALE] {label} — match has kicked off, purging")
            continue

        if entry.get("attempts", 0) >= MAX_ATTEMPTS:
            logger.warning(f"[GIVE UP] {label} — max attempts reached, purging")
            continue

        entry["attempts"] = entry.get("attempts", 0) + 1
        entry["last_attempt"] = datetime.now().isoformat()

        try:
            odds_data = get_event_odds(api_key, entry["event_id"])
        except Exception as e:
            logger.warning(f"[ERROR] {label} — fetch failed: {e}")
            still_pending[key] = entry
            continue

        if not odds_data:
            logger.info(f"[NO DATA] {label} — API returned nothing (attempt {entry['attempts']})")
            still_pending[key] = entry
            continue

        # Lazily fetch BF catalogue per competition for fallback
        bf_cat = None
        comp = entry.get("competition", "")
        if bf_session and comp:
            if comp not in bf_catalogues:
                try:
                    _, cat = get_ou_volume(bf_session, comp)
                    bf_catalogues[comp] = cat
                except Exception:
                    bf_catalogues[comp] = []
            bf_cat = bf_catalogues[comp]

        try:
            totals_df, btts_df = build_ready_dataframes(
                entry, odds_data, bf_session=bf_session, bf_catalogue=bf_cat)
        except Exception:
            logger.exception(f"[BUILD FAIL] {label}")
            still_pending[key] = entry
            continue

        resolved_types = set()
        if not totals_df.empty:
            for _, row in totals_df.iterrows():
                line_name = row.get("line", "")
                for hdp, mkt in BF_TOTALS_MARKET_NAME.items():
                    if mkt == line_name:
                        resolved_types.add(HDP_TO_BET_TYPE[hdp])
        if not btts_df.empty:
            resolved_types.add("BTTS")

        if not resolved_types:
            logger.info(f"[STILL MISSING] {label} — Bet365 still not quoting {entry.get('missing_bet_types')}")
            still_pending[key] = entry
            continue

        # Feed into the main insertion pipeline
        try:
            appended = update_master_from_dataframes(totals_df, btts_df)
            logger.info(f"[OK] {label} — resolved {sorted(resolved_types)}, {appended} row(s) appended")
        except Exception:
            logger.exception(f"[INSERT FAIL] {label}")
            still_pending[key] = entry
            continue

        # Mark this entry as resolved (remove completed bet types, keep any still missing)
        remaining = [bt for bt in entry.get("missing_bet_types", []) if bt not in resolved_types]
        if remaining:
            entry["missing_bet_types"] = remaining
            still_pending[key] = entry

    save_pending(still_pending)
    logger.info(f"Done: {len(pending) - len(still_pending)} resolved/purged, {len(still_pending)} still pending")


if __name__ == "__main__":
    main()