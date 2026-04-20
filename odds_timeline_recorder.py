"""
Odds Timeline Recorder — captures odds snapshots at multiple intervals before kickoff.

Runs every 30 minutes via launchd. For each upcoming match across all leagues,
records Bet365 odds, Betfair Exchange odds, and Betfair volume at target
hour marks before kickoff.

Data stored in flat CSVs: data/odds_timeline/{league_slug}/{date}.csv
Each row = one snapshot of one event at one point in time.

Designed to run independently from the main bookie_grabber pipeline.
"""

import os
import sys
import json
import yaml
import logging
import requests
import pandas as pd
import pytz
from pathlib import Path
from datetime import datetime, timedelta
from dateutil import parser as dtparser
from dotenv import load_dotenv

load_dotenv()

# ─── Config ───
PROJECT_ROOT = Path(__file__).resolve().parent
TIMELINE_DIR = PROJECT_ROOT / "data" / "odds_timeline"
CONFIG_PATH = PROJECT_ROOT / "config.yaml"
LOG_DIR = PROJECT_ROOT / "logs"

API_BASE = "https://api2.odds-api.io/v3"
BOOKMAKERS = "Bet365,Betfair Exchange"
PERTH = pytz.timezone("Australia/Perth")

# Target hours before kickoff to record (±15 min tolerance window)
TARGET_HOURS = [48, 24, 20, 16, 14, 12, 10, 8, 7, 6, 5]
TOLERANCE_MIN = 15  # ±15 minutes from the target hour

# ─── Logging ───
log_date = datetime.now(PERTH).strftime("%Y-%m-%d")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / f"odds_timeline_{log_date}.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("odds_timeline")

# ─── Helpers ───

def load_config():
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def get_events(api_key, sport_key, limit=30):
    """Fetch upcoming events from Odds API."""
    try:
        r = requests.get(f"{API_BASE}/events", params={
            "apiKey": api_key, "sport": "football", "league": sport_key, "limit": limit
        }, timeout=20)
        r.raise_for_status()
        data = r.json()
        return data if isinstance(data, list) else data.get("data", [])
    except Exception as e:
        logger.error(f"Failed to fetch events for {sport_key}: {e}")
        return []


def get_odds(api_key, event_id):
    """Fetch odds for a single event."""
    try:
        r = requests.get(f"{API_BASE}/odds", params={
            "apiKey": api_key, "eventId": event_id, "bookmakers": BOOKMAKERS
        }, timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.warning(f"Failed to fetch odds for event {event_id}: {e}")
        return {}


def parse_odds(odds_data):
    """Extract Bet365 and BF odds for all markets from API response."""
    result = {}
    bookmakers = odds_data.get("bookmakers", {})
    if not isinstance(bookmakers, dict):
        return result

    # Market name mappings
    TOTALS_MARKETS = {"Goals Over/Under", "Alternative Goal Line", "Totals"}
    BTTS_MARKETS = {"Both Teams to Score", "Both Teams To Score"}

    for bm_name, markets in bookmakers.items():
        if bm_name not in ("Bet365", "Betfair Exchange"):
            continue
        prefix = "b365" if bm_name == "Bet365" else "bf"

        if not isinstance(markets, list):
            continue
        for market in markets:
            mname = market.get("name", "")

            if mname in TOTALS_MARKETS:
                for o in market.get("odds", []):
                    hdp = o.get("hdp")
                    if hdp in (1.5, 2.5, 3.5):
                        tag = str(hdp).replace(".", "_")
                        result[f"{prefix}_over_{tag}"] = o.get("over")
                        result[f"{prefix}_under_{tag}"] = o.get("under")

            elif mname in BTTS_MARKETS:
                for o in market.get("odds", []):
                    result[f"{prefix}_btts_yes"] = o.get("yes")
                    result[f"{prefix}_btts_no"] = o.get("no")

    return result


def get_bf_volume(bf_session, league_name):
    """Fetch Betfair volume for a league. Returns dict of {event_key: {line: volume}}."""
    try:
        from betfair_api import get_ou_volume
        df, _ = get_ou_volume(bf_session, league_name, max_attempts=1)
        if df.empty:
            return {}
        vol = {}
        for _, row in df.iterrows():
            evt = row["event"]
            line = row["line"]
            v = row["total_volume"]
            vol.setdefault(evt, {})[line] = v
        return vol
    except Exception as e:
        logger.warning(f"BF volume failed for {league_name}: {e}")
        return {}


def load_team_map(slug):
    path = PROJECT_ROOT / "mappings" / slug / "team_name_map.json"
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return {}


def already_recorded(csv_path, event_id, target_hour):
    """Check if we've already recorded this event at this target hour."""
    if not csv_path.exists():
        return False
    try:
        df = pd.read_csv(csv_path, usecols=["event_id", "target_hours_before"])
        return ((df["event_id"].astype(str) == str(event_id)) &
                (df["target_hours_before"] == target_hour)).any()
    except Exception:
        return False


# ─── Main recording logic ───

def record_league(api_key, bf_session, league_cfg):
    """Record odds snapshots for all in-window events in a league."""
    name = league_cfg["name"]
    slug = league_cfg["slug"]
    sport_key = league_cfg["sport_key"]

    events = get_events(api_key, sport_key)
    if not events:
        return 0

    now = datetime.now(PERTH)
    team_map = load_team_map(slug)
    bf_volumes = None  # lazy-load only if needed
    recorded = 0

    # Output directory
    league_dir = TIMELINE_DIR / slug
    league_dir.mkdir(parents=True, exist_ok=True)
    csv_path = league_dir / f"{now.strftime('%Y-%m-%d')}.csv"

    for event in events:
        event_id = str(event.get("id", ""))
        home = event.get("home", "")
        away = event.get("away", "")
        raw_date = event.get("date", "")

        if not raw_date:
            continue

        # Parse match time
        try:
            match_time = dtparser.isoparse(raw_date)
            if match_time.tzinfo is None:
                match_time = match_time.replace(tzinfo=pytz.UTC)
            match_time = match_time.astimezone(PERTH)
        except Exception:
            continue

        hours_until_ko = (match_time - now).total_seconds() / 3600.0
        if hours_until_ko < 0:
            continue  # already started

        # Check if this event is at any target hour (±tolerance)
        matching_targets = []
        for target in TARGET_HOURS:
            diff_min = abs(hours_until_ko - target) * 60
            if diff_min <= TOLERANCE_MIN:
                # Check if already recorded
                if not already_recorded(csv_path, event_id, target):
                    matching_targets.append(target)

        if not matching_targets:
            continue

        # Fetch odds
        odds_data = get_odds(api_key, event_id)
        parsed = parse_odds(odds_data)

        if not parsed:
            logger.warning(f"No odds data for {home} vs {away}")
            continue

        # Fetch BF volume (lazy — one call per league)
        if bf_volumes is None:
            bf_volumes = get_bf_volume(bf_session, name)

        # Match BF volume to this event
        bf_home = team_map.get(home, "")
        bf_away = team_map.get(away, "")
        bf_key = f"{bf_home} v {bf_away}" if bf_home and bf_away else ""
        event_vol = bf_volumes.get(bf_key, {})

        vol_data = {
            "vol_1_5": event_vol.get("Over/Under 1.5 Goals"),
            "vol_2_5": event_vol.get("Over/Under 2.5 Goals"),
            "vol_3_5": event_vol.get("Over/Under 3.5 Goals"),
            "vol_btts": event_vol.get("Both teams to Score?"),
        }

        # Write one row per matching target hour
        snapshot_time = now.isoformat()
        for target in matching_targets:
            row = {
                "event_id": event_id,
                "home_team": home,
                "away_team": away,
                "competition": name,
                "match_time": match_time.isoformat(),
                "snapshot_time": snapshot_time,
                "hours_before_ko": round(hours_until_ko, 2),
                "target_hours_before": target,
                **parsed,
                **vol_data,
            }

            # Append to CSV
            row_df = pd.DataFrame([row])
            write_header = not csv_path.exists()
            row_df.to_csv(csv_path, mode="a", header=write_header, index=False)
            recorded += 1

            logger.info(f"  Recorded {home} vs {away} at {target}h before KO "
                       f"({hours_until_ko:.1f}h actual)")

    return recorded


def main():
    logger.info("=== Odds Timeline Recorder ===")
    api_key = os.environ.get("ODDS_API_KEY")
    if not api_key:
        logger.error("ODDS_API_KEY not set")
        return

    config = load_config()

    # Get BF session
    try:
        from betfair_api import get_session_token
        bf_session = get_session_token()
    except Exception as e:
        logger.warning(f"BF session failed: {e} — recording without BF volume")
        bf_session = None

    total_recorded = 0
    for league in config.get("leagues", []):
        try:
            n = record_league(api_key, bf_session, league)
            if n > 0:
                logger.info(f"{league['name']}: recorded {n} snapshots")
            total_recorded += n
        except Exception as e:
            logger.error(f"{league['name']}: {e}")

    logger.info(f"Done — {total_recorded} total snapshots recorded")


if __name__ == "__main__":
    main()
