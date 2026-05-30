"""Place all pending Telegram bets at €100/unit via SM Playwright automation.

Iterates telegram_pending_bets.json, calls place_sm_order with
stake_eur = stake_units * 100, removes from pending on success.
"""

import sys
import json
import logging
from pathlib import Path

sys.path.insert(0, "/Users/Joel/REPOS/BookieGrabber")

from telegram_alerts import place_sm_order, send_message

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("placer")

PENDING = Path("/Users/Joel/REPOS/BookieGrabber/telegram_pending_bets.json")
EUR_PER_UNIT = 100.0


def main():
    d = json.loads(PENDING.read_text())
    keys = list(d.keys())
    log.info(f"Placing {len(keys)} pending bets at €{EUR_PER_UNIT}/unit")

    results = []
    for i, key in enumerate(keys, 1):
        bet = d[key]
        stake_units = bet.get("stake", 1)
        stake_eur = stake_units * EUR_PER_UNIT
        desc = bet.get("description", bet.get("bet_type", "?"))
        match = f"{bet.get('home_team')} vs {bet.get('away_team')}"
        log.info(f"[{i}/{len(keys)}] PLACING €{stake_eur:.0f} | {desc} | {match} | key={key}")

        try:
            success, msg = place_sm_order(bet, stake_eur=stake_eur)
        except Exception as e:
            success, msg = False, f"Exception: {e}"

        log.info(f"[{i}/{len(keys)}] {'OK ' if success else 'FAIL'} — {msg}")
        results.append((key, bet, stake_eur, success, msg))

        # Re-read & update pending file each iteration to be resilient to crashes
        if success:
            cur = json.loads(PENDING.read_text())
            cur.pop(key, None)
            PENDING.write_text(json.dumps(cur, indent=2, default=str))

    # Summary
    placed = [r for r in results if r[3]]
    failed = [r for r in results if not r[3]]
    total_placed = sum(r[2] for r in placed)
    log.info("=" * 60)
    log.info(f"PLACED: {len(placed)} bets, €{total_placed:.0f} total")
    log.info(f"FAILED: {len(failed)} bets")
    for key, bet, eur, _, msg in failed:
        log.info(f"  FAIL €{eur:.0f} {bet.get('home_team')} vs {bet.get('away_team')} {bet.get('bet_type')} — {msg}")

    # Send Telegram summary
    summary_lines = [f"🎯 <b>Resend placement complete</b>",
                     f"Placed: {len(placed)} (€{total_placed:.0f})",
                     f"Failed: {len(failed)}"]
    if failed:
        summary_lines.append("\n<b>Failed bets (place manually):</b>")
        for key, bet, eur, _, msg in failed:
            summary_lines.append(
                f"• €{eur:.0f} {bet.get('description','?')} — "
                f"{bet.get('home_team')} vs {bet.get('away_team')}"
            )
    send_message("\n".join(summary_lines))


if __name__ == "__main__":
    main()
