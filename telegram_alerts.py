"""
Telegram bet alert system for Euro Football Bets.
Sends stake alerts via Telegram with inline confirm/skip buttons.
"""

import os
import logging
import requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("telegram_alerts")

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# Fallback: read from .env file directly if dotenv fails
if not TOKEN:
    env_path = Path(__file__).resolve().parent / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if line.startswith("TELEGRAM_BOT_TOKEN="):
                TOKEN = line.split("=", 1)[1].strip()
            elif line.startswith("TELEGRAM_CHAT_ID="):
                CHAT_ID = line.split("=", 1)[1].strip()

API_URL = f"https://api.telegram.org/bot{TOKEN}"


def send_message(text, parse_mode="HTML"):
    """Send a plain text message."""
    if not TOKEN or not CHAT_ID:
        logger.warning("Telegram not configured — skipping alert")
        return False
    try:
        r = requests.post(f"{API_URL}/sendMessage", json={
            "chat_id": CHAT_ID,
            "text": text,
            "parse_mode": parse_mode,
        }, timeout=10)
        return r.json().get("ok", False)
    except Exception as e:
        logger.error(f"Telegram send failed: {e}")
        return False


def send_bet_alert(bet, with_button=True):
    """Send a bet alert with match details and optional Place Bet button.

    bet: dict with keys: bet_type, home_team, away_team, competition,
         bf, rpd, volume, stake, match_time, prediction, event_id
    """
    bt = bet.get("bet_type", "")
    home = bet.get("home_team", "")
    away = bet.get("away_team", "")
    comp = bet.get("competition", "")
    bf = bet.get("bf", 0)
    rpd = bet.get("rpd", 0)
    vol = bet.get("volume", 0)
    stake = bet.get("stake", 1)
    match_time = bet.get("match_time", "")
    desc = bet.get("description", "")
    event_id = bet.get("event_id", "")

    if hasattr(match_time, "strftime"):
        time_str = match_time.strftime("%H:%M AWST")
    else:
        time_str = str(match_time)

    stake_emoji = "2️⃣" if stake == 2 else "1️⃣"

    # Calculate EUR amount for display
    from strategy_config import get_stake_per_unit
    match_date = bet.get("date")
    if match_date and isinstance(match_date, str):
        from datetime import datetime as _dt
        try:
            match_date = _dt.strptime(match_date, "%Y-%m-%d").date()
        except Exception:
            match_date = None
    eur_per_unit = get_stake_per_unit(match_date)
    total_eur = stake * eur_per_unit

    text = (
        f"🎯 <b>NEW BET ALERT</b>\n"
        f"\n"
        f"<b>{desc}</b>\n"
        f"{home} vs {away}\n"
        f"{comp}\n"
        f"\n"
        f"BF: {bf:.2f} | RPD: {rpd:.1f} | Vol: {vol:.0f}\n"
        f"Stake: {stake_emoji} {stake} unit{'s' if stake > 1 else ''} (€{total_eur:.0f})\n"
        f"\n"
        f"KO: {time_str}"
    )

    if not TOKEN or not CHAT_ID:
        logger.warning("Telegram not configured")
        return False

    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
    }

    if with_button and event_id:
        # Save bet details for callback
        _save_pending_bet(event_id, bet)
        payload["reply_markup"] = {
            "inline_keyboard": [[
                {"text": "✅ Place Bet", "callback_data": f"place:{event_id}:{bt}:{stake}"},
                {"text": "❌ Skip", "callback_data": f"skip:{event_id}"},
            ]]
        }

    try:
        r = requests.post(f"{API_URL}/sendMessage", json=payload, timeout=10)
        return r.json().get("ok", False)
    except Exception as e:
        logger.error(f"Telegram send failed: {e}")
        return False


def send_bet_alerts(bets):
    """Send multiple bet alerts."""
    if not bets:
        return

    # Summary header
    n = len(bets)
    total_stake = sum(b.get("stake", 1) for b in bets)
    send_message(f"📋 <b>{n} bet{'s' if n > 1 else ''} to place</b> (total stake: {total_stake})")

    # Individual alerts
    for bet in bets:
        send_bet_alert(bet)


def send_results_summary(wins, losses, profit):
    """Send end-of-day results summary."""
    emoji = "📈" if profit >= 0 else "📉"
    text = (
        f"{emoji} <b>Daily Results</b>\n"
        f"\n"
        f"Wins: {wins} | Losses: {losses}\n"
        f"Profit: {profit:+.2f} units\n"
    )
    return send_message(text)


# ─── Pending bet storage ───
import json

PENDING_BETS_PATH = Path(__file__).resolve().parent / "telegram_pending_bets.json"

def _save_pending_bet(event_id, bet):
    """Save bet details so we can place it when the button is pressed."""
    pending = {}
    if PENDING_BETS_PATH.exists():
        try:
            pending = json.loads(PENDING_BETS_PATH.read_text())
        except Exception:
            pass
    # Serialize datetime
    serialized = {}
    for k, v in bet.items():
        if hasattr(v, "isoformat"):
            serialized[k] = v.isoformat()
        else:
            serialized[k] = v
    key = f"{event_id}:{bet.get('bet_type', '')}"
    pending[key] = serialized
    PENDING_BETS_PATH.write_text(json.dumps(pending, indent=2, default=str))


def _load_pending_bet(key):
    if not PENDING_BETS_PATH.exists():
        return None
    try:
        pending = json.loads(PENDING_BETS_PATH.read_text())
        return pending.get(key)
    except Exception:
        return None


def _remove_pending_bet(key):
    if not PENDING_BETS_PATH.exists():
        return
    try:
        pending = json.loads(PENDING_BETS_PATH.read_text())
        pending.pop(key, None)
        PENDING_BETS_PATH.write_text(json.dumps(pending, indent=2, default=str))
    except Exception:
        pass


# ─── SM event lookup ───

def _find_sm_event_id(token, bet):
    """Search SM for the matching event and return their event_id format."""
    from sportsmarket_api import SM_BASE
    from difflib import SequenceMatcher
    from datetime import datetime as dt

    home = bet.get("home_team", "")
    away = bet.get("away_team", "")
    match_time = bet.get("match_time", "")

    # Parse match date
    match_date = None
    if hasattr(match_time, "date"):
        match_date = match_time.date()
    elif hasattr(match_time, "strftime"):
        match_date = match_time
    elif isinstance(match_time, str):
        try:
            match_date = dt.fromisoformat(match_time).date()
        except Exception:
            pass

    if not match_date:
        return None

    # Search SM events via their WebSocket-based event feed isn't practical,
    # but we can check recent orders for the same teams to get the event_id format
    # Or we can use the betfair catalogue which has SM-compatible event IDs

    # Best approach: query SM's orders endpoint filtered by date
    # SM event_ids follow: "YYYY-MM-DD,home_id,away_id"
    # We can search by looking at the event list in recent orders
    try:
        r = requests.get(f"{SM_BASE}/orders/",
            params={"placer": "joelbrown95", "page_size": 100},
            headers={"Accept": "application/json", "session": token,
                     "x-molly-client-name": "sonic"},
            timeout=15)
        if r.status_code != 200:
            return None

        orders = r.json().get("data", [])
        # Look through recent orders for events matching these teams
        # to learn the team IDs, then construct the event_id

        # Collect known team_id mappings
        team_ids = {}
        for o in orders:
            ev = o.get("event_info", {})
            eid = ev.get("event_id", "")
            if "," not in eid:
                continue
            parts = eid.split(",")
            if len(parts) == 3:
                h_id, a_id = parts[1], parts[2]
                team_ids[ev.get("home_team", "").lower()] = h_id
                team_ids[ev.get("away_team", "").lower()] = a_id

        # Try to find team IDs for our teams
        def find_id(name):
            name_l = name.lower()
            # Exact
            if name_l in team_ids:
                return team_ids[name_l]
            # Fuzzy
            best_score = 0
            best_id = None
            for known, tid in team_ids.items():
                score = SequenceMatcher(None, name_l, known).ratio()
                if name_l in known or known in name_l:
                    score = max(score, 0.85)
                if score > best_score and score > 0.6:
                    best_score = score
                    best_id = tid
            return best_id

        h_id = find_id(home)
        a_id = find_id(away)

        if h_id and a_id:
            date_str = match_date.strftime("%Y-%m-%d") if hasattr(match_date, "strftime") else str(match_date)
            sm_eid = f"{date_str},{h_id},{a_id}"
            logger.info(f"Resolved SM event_id: {sm_eid} for {home} vs {away}")
            return sm_eid

        logger.warning(f"Could not resolve SM team IDs: home={home}({h_id}) away={away}({a_id})")
        return None
    except Exception as e:
        logger.error(f"SM event lookup failed: {e}")
        return None


# ─── SM bet type mapping ───
SM_BET_TYPES = {
    # (market, prediction) -> SM bet_type string
    ("1.5G", 0): "for,ahunder,6",    # Under 1.5 Asian
    ("1.5G", 1): "for,ahover,6",     # Over 1.5 Asian
    ("2.5G", 0): "for,ahunder,10",   # Under 2.5 Asian
    ("2.5G", 1): "for,ahover,10",    # Over 2.5 Asian
    ("3.5G", 0): "for,ahunder,14",   # Under 3.5 Asian
    ("3.5G", 1): "for,ahover,14",    # Over 3.5 Asian
    ("BTTS", 0): "for,nbtts,0",      # No BTTS (At least one team to not score)
    ("BTTS", 1): "for,btts,0",       # Both teams to score
}


SM_EVENT_MAP_FILE = Path(__file__).resolve().parent / "data" / "state" / "sm_event_map.json"


def _lookup_preflight_sm_id(bet):
    """Look up SM event ID from the pre-resolved mapping file (written by preflight_check.py)."""
    if not SM_EVENT_MAP_FILE.exists():
        return None
    try:
        mapping = json.loads(SM_EVENT_MAP_FILE.read_text())
        key = f"{bet.get('home_team', '')}|{bet.get('away_team', '')}"
        entry = mapping.get(key)
        if entry:
            logger.info(f"Preflight SM event ID found: {key} → {entry['sm_event_id']}")
            return entry["sm_event_id"]
    except Exception as e:
        logger.warning(f"Preflight SM map lookup failed: {e}")
    return None


def place_sm_order(bet, stake_eur=250.0):
    """Place an order on SportsMarket via Playwright browser automation.

    Returns (success, message) tuple.
    """
    from sportsmarket_api import get_session, place_order_playwright

    bt = bet.get("bet_type", "")
    # Use actual_prediction if set (fade already resolved), else fall back to prediction
    pred = bet.get("actual_prediction", bet.get("prediction", 0))

    # Resolve SM event ID (date,home_id,away_id)
    # If event_id is already in SM format (date,id,id), use it directly
    event_id = bet.get("event_id", "")
    if event_id and "," in event_id and len(event_id.split(",")) == 3:
        sm_event_id = event_id
    else:
        # Check pre-resolved mapping from preflight_check first
        sm_event_id = _lookup_preflight_sm_id(bet)
        if not sm_event_id:
            token = get_session()
            if not token:
                return False, "SM session expired. Cannot place bet."
            sm_event_id = _find_sm_event_id(token, bet)
        if not sm_event_id:
            return False, (f"Could not find event on SportsMarket for "
                           f"{bet.get('home_team')} vs {bet.get('away_team')}")

    # Place via Playwright
    return place_order_playwright(
        sm_event_id=sm_event_id,
        bet_type=bt,
        prediction=pred,
        stake_eur=stake_eur,
        headless=True,
        league_slug=bet.get("league_slug"),
    )



# ─── Callback handler (runs as long-polling bot) ───

def run_bot():
    """Long-polling bot that listens for Place/Skip button presses."""
    logging.basicConfig(level=logging.INFO,
                       format="%(asctime)s [%(levelname)s] %(message)s")
    logger.info("Telegram bot started - listening for callbacks...")

    # Flush all pending updates on startup so we don't reprocess old callbacks
    try:
        r = requests.get(f"{API_URL}/getUpdates", params={"offset": -1}, timeout=10)
        updates = r.json().get("result", [])
        if updates:
            last_update_id = updates[-1]["update_id"]
            logger.info(f"Flushed {len(updates)} pending updates, starting from {last_update_id}")
        else:
            last_update_id = 0
    except Exception:
        last_update_id = 0

    while True:
        try:
            r = requests.get(f"{API_URL}/getUpdates", params={
                "offset": last_update_id + 1,
                "timeout": 30,  # long poll
            }, timeout=35)
            updates = r.json().get("result", [])

            for update in updates:
                last_update_id = update["update_id"]
                callback = update.get("callback_query")
                if not callback:
                    continue

                data = callback.get("data", "")
                callback_id = callback.get("id")
                message_id = callback["message"]["message_id"]

                if data.startswith("place:"):
                    parts = data.split(":")
                    event_id = parts[1]
                    bt = parts[2]
                    stake = int(parts[3])
                    key = f"{event_id}:{bt}"

                    bet = _load_pending_bet(key)
                    if not bet:
                        _answer_callback(callback_id, "Already processed")
                        continue  # Silently skip — don't update old messages

                    # Remove pending bet IMMEDIATELY to prevent duplicate placements
                    _remove_pending_bet(key)

                    # Calculate EUR stake based on day of week
                    from strategy_config import get_stake_per_unit
                    match_date = bet.get("date")
                    if match_date and isinstance(match_date, str):
                        from datetime import datetime as _dt
                        try:
                            match_date = _dt.strptime(match_date, "%Y-%m-%d").date()
                        except Exception:
                            match_date = None
                    stake_eur = stake * get_stake_per_unit(match_date)

                    _answer_callback(callback_id, "Placing bet...")
                    _update_message(message_id,
                        f"⏳ <b>PLACING BET...</b>\n\n"
                        f"{bet.get('description', bt)}\n"
                        f"{bet.get('home_team')} vs {bet.get('away_team')}\n"
                        f"Stake: €{stake_eur:.0f}")

                    success, msg = place_sm_order(bet, stake_eur=stake_eur)

                    if success:
                        _update_message(message_id,
                            f"✅ <b>BET PLACED</b>\n\n"
                            f"{bet.get('description', bt)}\n"
                            f"{bet.get('home_team')} vs {bet.get('away_team')}\n"
                            f"Stake: €{stake_eur:.0f}\n\n"
                            f"{msg}")
                    else:
                        _update_message(message_id,
                            f"❌ <b>PLACEMENT FAILED</b>\n\n{msg}\n\n"
                            f"Place manually on SportsMarket.")

                elif data.startswith("skip:"):
                    event_id = data.split(":")[1]
                    _answer_callback(callback_id, "Bet skipped")
                    _update_message(message_id, "⏭️ Bet skipped")
                    # Remove all pending bets for this event
                    if PENDING_BETS_PATH.exists():
                        try:
                            pending = json.loads(PENDING_BETS_PATH.read_text())
                            to_remove = [k for k in pending if k.startswith(event_id)]
                            for k in to_remove:
                                pending.pop(k)
                            PENDING_BETS_PATH.write_text(json.dumps(pending, indent=2, default=str))
                        except Exception:
                            pass

        except requests.exceptions.ReadTimeout:
            continue
        except Exception as e:
            logger.error(f"Bot error: {e}")
            import time
            time.sleep(5)


def _answer_callback(callback_id, text):
    try:
        requests.post(f"{API_URL}/answerCallbackQuery", json={
            "callback_query_id": callback_id,
            "text": text,
        }, timeout=5)
    except Exception:
        pass


def _update_message(message_id, text):
    try:
        requests.post(f"{API_URL}/editMessageText", json={
            "chat_id": CHAT_ID,
            "message_id": message_id,
            "text": text,
            "parse_mode": "HTML",
        }, timeout=5)
    except Exception:
        pass


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "bot":
        run_bot()
    else:
        # Test alert with buttons
        send_bet_alert({
            "bet_type": "3.5G",
            "home_team": "Arsenal",
            "away_team": "Chelsea",
            "competition": "English Premier League",
            "bf": 1.85,
            "rpd": 2.1,
            "volume": 450,
            "stake": 1,
            "match_time": "21:00 AWST",
            "description": "Bet 3.5G Under",
            "prediction": 0,
            "event_id": "test_event_123",
        })
        print("Test alert sent with buttons")