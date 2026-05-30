"""Place the 4 remaining KO=22:00 bets (€400 total)."""
import sys, logging, json
from pathlib import Path

sys.path.insert(0, "/Users/Joel/REPOS/BookieGrabber")
from telegram_alerts import place_sm_order, send_message

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("place4")

sm_map = json.loads(Path("/Users/Joel/REPOS/BookieGrabber/data/state/sm_event_map.json").read_text())

BETS = [
    {"home_team": "Brentford FC", "away_team": "Crystal Palace",
     "bet_type": "3.5G", "prediction": 0, "actual_prediction": 0,
     "league_slug": "english_premier_league", "stake": 1,
     "description": "Bet 3.5G Under", "match_time": "22:00", "date": "2026-05-17"},
    {"home_team": "Leeds United", "away_team": "Brighton & Hove Albion",
     "bet_type": "3.5G", "prediction": 0, "actual_prediction": 0,
     "league_slug": "english_premier_league", "stake": 1,
     "description": "Bet 3.5G Under", "match_time": "22:00", "date": "2026-05-17"},
    {"home_team": "Everton FC", "away_team": "Sunderland AFC",
     "bet_type": "BTTS", "prediction": 0, "actual_prediction": 1,  # BTTS fade → Yes
     "league_slug": "english_premier_league", "stake": 1,
     "description": "Bet BTTS Yes (No fade)", "match_time": "22:00", "date": "2026-05-17"},
    {"home_team": "Kayserispor", "away_team": "Konyaspor",
     "bet_type": "3.5G", "prediction": 0, "actual_prediction": 0,
     "league_slug": "turkish_super_league", "stake": 1,
     "description": "Bet 3.5G Under", "match_time": "22:00", "date": "2026-05-17"},
]

results = []
for i, b in enumerate(BETS, 1):
    key = f"{b['home_team']}|{b['away_team']}"
    entry = sm_map.get(key)
    if entry:
        b["event_id"] = entry["sm_event_id"]
    else:
        log.warning(f"No SM map for {key}")
        b["event_id"] = ""
    eur = b["stake"] * 100
    log.info(f"[{i}/4] PLACING €{eur} {b['description']} | {b['home_team']} vs {b['away_team']} | event={b.get('event_id')}")
    try:
        ok, msg = place_sm_order(b, stake_eur=eur)
    except Exception as e:
        ok, msg = False, f"Exception: {e}"
    log.info(f"[{i}/4] {'OK  ' if ok else 'FAIL'} — {msg}")
    results.append((b, eur, ok, msg))

placed = [r for r in results if r[2]]
failed = [r for r in results if not r[2]]
log.info("=" * 50)
log.info(f"PLACED: {len(placed)}, €{sum(r[1] for r in placed)}")
log.info(f"FAILED: {len(failed)}")

lines = [f"🎯 <b>Final 4 placement (€400 plan)</b>",
         f"Placed: {len(placed)} (€{sum(r[1] for r in placed)})",
         f"Failed: {len(failed)}"]
for b, eur, ok, msg in failed:
    lines.append(f"• €{eur} {b['description']} {b['home_team']} vs {b['away_team']} — {msg}")
send_message("\n".join(lines))
