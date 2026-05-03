"""
Weekly team-name mapping audit.

For every league in config.yaml, fetches both Odds API events and Betfair's
upcoming event list, then runs diagnose_team_mappings() to flag:
  - BF teams not present in our mapping (with closest OA team suggestion)
  - Mappings that point to wrong/missing BF names

Designed to run weekly via launchd. Sends a Telegram summary.
"""

import os
import sys
import json
import yaml
import io
import contextlib
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from bookie_grabber import diagnose_team_mappings, API_BASE
from betfair_api import get_session_token, get_ou_volume
from telegram_alerts import send_message


def audit_all_leagues():
    """Run diagnostic for every league. Returns (text_report, leagues_with_issues)."""
    api_key = os.environ.get("ODDS_API_KEY")
    if not api_key:
        return "ODDS_API_KEY not set in environment", 0

    with open("config.yaml") as f:
        config = yaml.safe_load(f)

    bf_token = get_session_token()

    report_lines = []
    leagues_with_issues = 0
    leagues_skipped = 0

    for league in config["leagues"]:
        name = league["name"]
        slug = league["slug"]
        sport_key = league["sport_key"]

        map_path = f"mappings/{slug}/team_name_map.json"
        if not os.path.exists(map_path):
            report_lines.append(f"\n--- {name} ---\n  [SKIP] No mapping file at {map_path}")
            leagues_skipped += 1
            continue

        with open(map_path) as f:
            mapping = json.load(f)

        # Fetch Odds API events
        try:
            r = requests.get(
                f"{API_BASE}/events",
                params={"apiKey": api_key, "sport": "football",
                        "league": sport_key, "limit": 30},
                timeout=20,
            )
            r.raise_for_status()
            events = r.json()
        except Exception as e:
            report_lines.append(f"\n--- {name} ---\n  [ERROR] OA fetch failed: {e}")
            continue

        # Fetch BF events
        try:
            df_bf, _ = get_ou_volume(bf_token, name)
        except Exception as e:
            report_lines.append(f"\n--- {name} ---\n  [ERROR] BF fetch failed: {e}")
            continue

        if df_bf.empty:
            # Off-season or league not currently active on BF — skip silently
            leagues_skipped += 1
            continue

        # Capture diagnostic output
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            diagnose_team_mappings(name, events, df_bf, mapping)
        diag_output = buf.getvalue().strip()

        if diag_output:
            leagues_with_issues += 1
            report_lines.append(f"\n--- {name} ---\n{diag_output}")

    summary = (f"\n{'='*60}\n"
               f"Audited {len(config['leagues'])} leagues\n"
               f"  Issues found in: {leagues_with_issues}\n"
               f"  Skipped (no mapping / off-season): {leagues_skipped}\n"
               f"  Clean: {len(config['leagues']) - leagues_with_issues - leagues_skipped}\n")

    return "".join(report_lines) + summary, leagues_with_issues


def send_telegram_summary(report_text, n_issues):
    """Send a concise Telegram summary linking to the full log."""
    if n_issues == 0:
        msg = (f"✅ <b>Weekly mapping audit</b>\n\n"
               f"All league team-name mappings look clean.")
    else:
        # Telegram has a 4096 char limit — truncate aggressively if needed
        body = report_text
        if len(body) > 3500:
            body = body[:3400] + "\n... (truncated, see logs/check_team_mappings.out)"
        msg = (f"⚠️ <b>Weekly mapping audit — {n_issues} league(s) need attention</b>\n\n"
               f"<pre>{body}</pre>")
    send_message(msg)


if __name__ == "__main__":
    print(f"=== Team mapping audit started: {datetime.now().isoformat()} ===")
    report, n_issues = audit_all_leagues()
    print(report)
    try:
        send_telegram_summary(report, n_issues)
        print(f"\nSent Telegram summary ({n_issues} leagues with issues)")
    except Exception as e:
        print(f"Failed to send Telegram summary: {e}")
    print(f"=== Done: {datetime.now().isoformat()} ===")
