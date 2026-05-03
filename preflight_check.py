"""
Daily pre-flight check for SportsMarket bet placement.

Runs at 6:00 AWST (22:00 UTC previous day) before alerts start.
Validates every component in the placement chain and sends a
Telegram summary so issues are caught before bets come through.

Checks:
  1. Playwright chromium binary exists
  2. SM_PASSWORD is set
  3. SM login works (Playwright headless)
  4. Telegram bot process is running
  5. SM DOM selectors still work (asian total goals, BTTS sections)
  6. SM league prefixes resolve for leagues with matches today
  7. SM event IDs resolvable for today's matches
"""

import logging
import os
import subprocess
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests
import yaml

from constants import PROJECT_ROOT
sys.path.insert(0, str(PROJECT_ROOT))

LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_PATH = LOG_DIR / f"preflight_{datetime.now().strftime('%Y-%m-%d')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(str(LOG_PATH)), logging.StreamHandler()],
    force=True,
)
logger = logging.getLogger("preflight")

# Load .env
from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env", override=True)


def check_playwright_binary():
    """Check that the Playwright Chromium binary exists."""
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            path = p.chromium.executable_path
            if Path(path).exists():
                return True, f"Chromium found: {path}"
            return False, f"Chromium binary missing: {path}"
    except Exception as e:
        return False, f"Playwright error: {e}"


def check_sm_password():
    """Check SM_PASSWORD is set in environment."""
    pw = os.getenv("SM_PASSWORD", "")
    if pw:
        return True, "SM_PASSWORD is set"
    return False, "SM_PASSWORD not set in .env"


def check_telegram_bot():
    """Check the Telegram bot process is running via launchd."""
    try:
        result = subprocess.run(
            ["launchctl", "list"], capture_output=True, text=True, timeout=5
        )
        for line in result.stdout.splitlines():
            if "com.john.telegrambot" in line:
                parts = line.split()
                pid = parts[0]
                exit_code = parts[1]
                if pid != "-" and pid.isdigit():
                    return True, f"Telegram bot running (PID {pid})"
                return False, f"Telegram bot not running (exit code {exit_code})"
        return False, "Telegram bot launchd job not found"
    except Exception as e:
        return False, f"Could not check launchd: {e}"


def check_sm_login():
    """Attempt SM login via Playwright and verify we land on sportsbook."""
    from playwright.sync_api import sync_playwright

    username = os.getenv("SM_USERNAME", "joelbrown95")
    password = os.getenv("SM_PASSWORD", "")

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()

            page.goto("https://pro.sportmarket.com/login", timeout=30000)
            page.wait_for_selector('input[type="text"]', timeout=10000)
            page.fill('input[type="text"]', username)
            page.fill('input[type="password"]', password)
            page.get_by_role("button", name="log In").click()
            page.wait_for_url(
                lambda url: "/sportsbook" in url or "/trade" in url,
                timeout=30000,
            )
            url = page.url
            browser.close()
            return True, f"SM login OK — landed on {url}"
    except Exception as e:
        return False, f"SM login failed: {e}"


def _get_todays_matches():
    """Get today's matches using the same API as bookie_grabber.py."""
    api_key = os.getenv("ODDS_API_KEY", "")
    if not api_key:
        return []

    cfg = yaml.safe_load((PROJECT_ROOT / "config.yaml").read_text())
    leagues = cfg.get("leagues", [])

    API_BASE = "https://api2.odds-api.io/v3"
    now = datetime.now(timezone.utc)
    end_dt = now.date() + timedelta(days=1)
    end_rfc = datetime(end_dt.year, end_dt.month, end_dt.day,
                       23, 59, 59, tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")

    matches = []
    for league in leagues:
        sport_key = league["sport_key"]
        slug = league["slug"]
        try:
            r = requests.get(
                f"{API_BASE}/events",
                params={"apiKey": api_key, "sport": "football",
                        "league": sport_key, "limit": 50, "to": end_rfc},
                timeout=15,
            )
            if r.status_code != 200:
                continue
            data = r.json()
            events = data.get("data", data) if isinstance(data, dict) else data
            if not isinstance(events, list):
                continue
            for ev in events:
                ko_str = ev.get("date", "")
                if not ko_str:
                    continue
                try:
                    ko_dt = datetime.fromisoformat(
                        ko_str.replace("Z", "+00:00")
                    )
                except (ValueError, TypeError):
                    continue
                if now <= ko_dt <= now + timedelta(hours=24):
                    matches.append({
                        "home": ev.get("home", ""),
                        "away": ev.get("away", ""),
                        "ko": ko_dt,
                        "slug": slug,
                        "sport_key": sport_key,
                    })
        except Exception:
            continue

    return matches


def check_sm_league_prefixes(page, matches):
    """Verify SM league prefix URLs load for leagues with matches today."""
    from sportsmarket_api import SM_LEAGUE_PREFIXES, SM_SPORTSBOOK_BASE

    slugs_today = set(m["slug"] for m in matches)
    results = []

    for slug in slugs_today:
        prefix = SM_LEAGUE_PREFIXES.get(slug)
        if not prefix:
            results.append((False, f"{slug}: no SM prefix configured"))
            continue

        url = f"{SM_SPORTSBOOK_BASE}/{prefix}"
        try:
            page.goto(url, timeout=15000)
            page.wait_for_timeout(2000)
            # Check page isn't an error / empty
            text = page.text_content("body") or ""
            if "not found" in text.lower() or "error" in text.lower()[:200]:
                results.append((False, f"{slug}: prefix {prefix} — page error"))
            else:
                # Count events listed
                event_links = page.evaluate('''() => {
                    return document.querySelectorAll("a[href*='origin=sportsbook']").length;
                }''')
                results.append(
                    (True, f"{slug}: prefix {prefix} OK ({event_links} events)")
                )
        except Exception as e:
            results.append((False, f"{slug}: prefix {prefix} — {e}"))

    return results


def check_sm_dom_selectors(page, matches):
    """Navigate to a real event page and verify DOM selectors still work.

    Tests that:
    - 'asian total goals' section is findable
    - 'both teams to score' section is findable
    - Over/Under buttons exist with expected structure
    - Yes/No buttons exist with expected structure
    """
    from sportsmarket_api import SM_LEAGUE_PREFIXES, SM_SPORTSBOOK_BASE

    if not matches:
        return True, "No matches today — DOM check skipped"

    # Pick the first league with a known prefix and find an event
    for m in matches:
        prefix = SM_LEAGUE_PREFIXES.get(m["slug"])
        if not prefix:
            continue

        league_url = f"{SM_SPORTSBOOK_BASE}/{prefix}"
        try:
            page.goto(league_url, timeout=15000)
            page.wait_for_timeout(2000)

            # Find first event link
            event_href = page.evaluate('''() => {
                const links = document.querySelectorAll("a[href*='origin=sportsbook']");
                for (const link of links) {
                    const href = link.href;
                    // Must contain a date-based event ID
                    if (/\\d{4}-\\d{2}-\\d{2}/.test(href)) return href;
                }
                return null;
            }''')

            if not event_href:
                continue

            page.goto(event_href, timeout=15000)
            page.wait_for_timeout(4000)

            # Check for market sections
            dom_check = page.evaluate('''() => {
                const results = {
                    asian_goals: false, btts: false,
                    over_under_btns: false, yes_no_btns: false,
                    section_class: null, header_class: null,
                    btn_class: null, label_class: null,
                    line_class: null, odds_class: null,
                    all_sections: [],
                };

                // Find all sections — look for market header spans
                const allSpans = document.querySelectorAll("span");
                for (const span of allSpans) {
                    const t = (span.textContent || "").trim().toLowerCase();
                    if (t === "asian total goals") {
                        results.asian_goals = true;
                        // Walk up to find section container
                        let sec = span.closest("div[class]");
                        for (let i = 0; i < 5 && sec; i++) {
                            if (sec.children.length > 2) {
                                results.section_class = sec.className;
                                break;
                            }
                            sec = sec.parentElement;
                        }
                        results.header_class = span.className;
                    }
                    if (t === "both teams to score") {
                        results.btts = true;
                    }
                }

                // Find over/under and yes/no buttons
                for (const span of allSpans) {
                    const t = (span.textContent || "").trim().toLowerCase();
                    if (t === "over" || t === "under") {
                        results.over_under_btns = true;
                        results.label_class = span.className;
                        const btn = span.parentElement;
                        if (btn) results.btn_class = btn.className;
                        // Find odds element sibling
                        const odds = btn ? btn.querySelector(
                            "span:not([class='" + span.className + "'])"
                        ) : null;
                        if (odds) results.odds_class = odds.className;
                    }
                    if (t === "yes" || t === "no") {
                        results.yes_no_btns = true;
                    }
                }

                // Find line label class (e.g., "1.5", "2.5", "3.5")
                for (const el of document.querySelectorAll("div")) {
                    const t = (el.textContent || "").trim();
                    if (t === "1.5" || t === "2.5" || t === "3.5") {
                        if (el.children.length === 0) {
                            results.line_class = el.className;
                            break;
                        }
                    }
                }

                // Collect all section header texts for debugging
                const headers = document.querySelectorAll("span");
                const seen = new Set();
                for (const h of headers) {
                    const t = (h.textContent || "").trim().toLowerCase();
                    if (t.length > 3 && t.length < 40 && !seen.has(t)) {
                        seen.add(t);
                        if (["asian", "goal", "score", "corner", "card",
                             "handicap", "match", "1x2", "total", "btts",
                             "both"].some(k => t.includes(k))) {
                            results.all_sections.push(t);
                        }
                    }
                }

                return results;
            }''')

            issues = []
            if not dom_check["asian_goals"]:
                issues.append("'asian total goals' section NOT FOUND")
            if not dom_check["btts"]:
                issues.append("'both teams to score' section NOT FOUND")
            if not dom_check["over_under_btns"]:
                issues.append("Over/Under buttons NOT FOUND")
            if not dom_check["yes_no_btns"]:
                issues.append("Yes/No buttons NOT FOUND")

            # Check if CSS classes match what's hardcoded
            from sportsmarket_api import _click_odds_button
            import inspect
            source = inspect.getsource(_click_odds_button)

            stale_classes = []
            for cls_name in ["_db547d4", "_53364e30", "_5efe57c4", "_2b5878f5",
                             "_6319cb42", "_67b4d261", "_430d5ce1", "_4aa3c57e",
                             "_3dcf8bbe"]:
                if cls_name in source:
                    # Check if this class still exists in the page
                    exists = page.evaluate(
                        f'() => document.querySelector(".{cls_name}") !== null'
                    )
                    if not exists:
                        stale_classes.append(cls_name)

            if stale_classes:
                issues.append(
                    f"Stale CSS classes in _click_odds_button: {', '.join(stale_classes)}"
                )

                # Report what the NEW classes are
                new_classes = {
                    "section": dom_check.get("section_class", "?"),
                    "header_span": dom_check.get("header_class", "?"),
                    "button": dom_check.get("btn_class", "?"),
                    "label_span": dom_check.get("label_class", "?"),
                    "odds_span": dom_check.get("odds_class", "?"),
                    "line_div": dom_check.get("line_class", "?"),
                }
                issues.append(f"New DOM classes: {new_classes}")

            if issues:
                detail = "; ".join(issues)
                sections = ", ".join(dom_check.get("all_sections", []))
                return False, f"DOM issues on {event_href}: {detail}. Sections found: [{sections}]"

            return True, (
                f"DOM OK — asian goals: ✓, BTTS: ✓, buttons: ✓, "
                f"CSS classes: ✓ (checked on {event_href})"
            )

        except Exception as e:
            logger.warning(f"DOM check failed on {m['slug']}: {e}")
            continue

    return False, "Could not find any event page to test DOM selectors"


SM_EVENT_MAP_FILE = PROJECT_ROOT / "data" / "state" / "sm_event_map.json"


def check_sm_events_resolvable(page, matches):
    """For today's matches, resolve SM event IDs and save mapping.

    Uses the team name registry for matching (no fuzzy matching needed once
    teams are registered). Collects SM team names into the registry.
    Falls back to fuzzy match for new/unregistered teams.
    """
    import json, re
    from sportsmarket_api import SM_LEAGUE_PREFIXES, SM_SPORTSBOOK_BASE
    from team_name_registry import register_names, lookup, collect_from_odds_api

    if not matches:
        return []

    results = []
    sm_map = {}
    checked_slugs = set()

    # Register Odds API names for today's matches
    from collections import defaultdict
    matches_by_slug = defaultdict(list)
    for m in matches:
        matches_by_slug[m["slug"]].append(m)
    for slug, ms in matches_by_slug.items():
        odds_api_names = set()
        for m in ms:
            odds_api_names.add(m["home"])
            odds_api_names.add(m["away"])
        collect_from_odds_api(slug, [{"home": n, "away": ""} for n in odds_api_names]
                              + [{"home": "", "away": n} for n in odds_api_names])

    checked_prefixes = set()  # avoid visiting same SM page for multiple leagues

    for m in matches:
        slug = m["slug"]
        if slug in checked_slugs:
            continue
        checked_slugs.add(slug)

        prefix = SM_LEAGUE_PREFIXES.get(slug)
        if not prefix:
            continue
        if prefix in checked_prefixes:
            continue  # another league already checked this SM page (e.g. XE/1 = EPL + UEFA)
        checked_prefixes.add(prefix)

        league_url = f"{SM_SPORTSBOOK_BASE}/{prefix}"
        try:
            page.goto(league_url, timeout=15000)
            page.wait_for_timeout(2000)

            # Extract SM event links with team names from DOM divs
            sm_events = page.evaluate('''() => {
                const results = [];
                const links = document.querySelectorAll("a[href*='origin=sportsbook']");
                for (const link of links) {
                    const href = link.href;
                    const match = href.match(/(\\d{4}-\\d{2}-\\d{2},\\d+,\\d+)/);
                    if (!match) continue;

                    // Team names are in leaf divs (no child divs)
                    // Pattern: first non-date/time leaf div = home, second = away
                    const leafDivs = link.querySelectorAll("div");
                    const teamNames = [];
                    for (const d of leafDivs) {
                        if (d.querySelectorAll("div").length > 0) continue;
                        const t = d.textContent.trim();
                        if (!t || t === "today" || t === "tomorrow") continue;
                        if (/^\\d+[.:,]\\d+$/.test(t)) continue;
                        if (/^\\d{2}\\/\\d{2}$/.test(t)) continue;
                        if (/^-\\/-$/.test(t)) continue;
                        if (t.length <= 1) continue;
                        // Filter live match artifacts (scores, minutes, HT)
                        if (t === "HT" || t === "FT" || t === "ET") continue;
                        if (/^\\d+'/.test(t)) continue;
                        if (/^\\d+$/.test(t)) continue;
                        teamNames.push(t);
                    }

                    results.push({
                        eid: match[1],
                        text: link.textContent.toLowerCase().replace(/\\s+/g, " "),
                        teams: teamNames.slice(0, 2),
                        href: href,
                    });
                }
                return results;
            }''')

            # Collect SM team names into registry
            sm_team_names = set()
            for ev in sm_events:
                for t in ev.get("teams", []):
                    sm_team_names.add(t)
            if sm_team_names:
                unmatched_sm = register_names(slug, "sm", list(sm_team_names))
                if unmatched_sm:
                    logger.warning(f"[{slug}] Unmatched SM teams: {unmatched_sm}")

            # Match each of today's matches to SM events
            from difflib import SequenceMatcher
            league_matches = [m2 for m2 in matches if m2["slug"] == slug]
            for lm in league_matches:
                # Try registry lookup first
                home_sm = lookup(slug, "odds_api", "sm", lm["home"])
                away_sm = lookup(slug, "odds_api", "sm", lm["away"])

                matched_ev = None
                if home_sm and away_sm:
                    # Exact match via registry
                    for ev in sm_events:
                        txt = ev["text"]
                        if home_sm.lower() in txt and away_sm.lower() in txt:
                            matched_ev = ev
                            break

                # Fuzzy fallback for unregistered teams
                # Match against individual team names, requiring BOTH to match
                if not matched_ev:
                    home = lm["home"].lower()
                    away = lm["away"].lower()
                    best_score = 0
                    for ev in sm_events:
                        teams = ev.get("teams", [])
                        if len(teams) < 2:
                            continue
                        sm_home = teams[0].lower()
                        sm_away = teams[1].lower()
                        # Try both orderings (home/away might be swapped)
                        h1 = SequenceMatcher(None, home, sm_home).ratio()
                        a1 = SequenceMatcher(None, away, sm_away).ratio()
                        h2 = SequenceMatcher(None, home, sm_away).ratio()
                        a2 = SequenceMatcher(None, away, sm_home).ratio()
                        score1 = (h1 + a1) / 2 if min(h1, a1) > 0.4 else 0
                        score2 = (h2 + a2) / 2 if min(h2, a2) > 0.4 else 0
                        score = max(score1, score2)
                        if score > best_score:
                            best_score = score
                            matched_ev = ev if score > 0.55 else None

                if matched_ev:
                    sm_eid = matched_ev["eid"]
                    key = f"{lm['home']}|{lm['away']}"
                    sm_map[key] = {
                        "sm_event_id": sm_eid,
                        "slug": slug,
                        "prefix": prefix,
                    }
                    method = "registry" if (home_sm and away_sm) else "fuzzy"
                    results.append(
                        (True, f"{lm['home']} vs {lm['away']}: {sm_eid} ({method})")
                    )
                else:
                    results.append(
                        (False, f"{lm['home']} vs {lm['away']} [{slug}]: NOT on SM")
                    )
        except Exception as e:
            results.append((False, f"{slug}: page load failed — {e}"))

    # Save event mapping file
    SM_EVENT_MAP_FILE.parent.mkdir(parents=True, exist_ok=True)
    existing = {}
    if SM_EVENT_MAP_FILE.exists():
        try:
            existing = json.loads(SM_EVENT_MAP_FILE.read_text())
        except Exception:
            pass
    existing.update(sm_map)
    SM_EVENT_MAP_FILE.write_text(json.dumps(existing, indent=2))
    logger.info(f"SM event map: {len(sm_map)} new, {len(existing)} total")

    return results


def run_preflight():
    """Run all pre-flight checks and send Telegram summary."""
    logger.info("Starting pre-flight check...")
    all_results = []
    critical_fail = False

    # --- 1. Playwright binary ---
    ok, msg = check_playwright_binary()
    all_results.append(("Playwright", ok, msg))
    if not ok:
        critical_fail = True
        # Try auto-fix
        logger.info("Attempting to install Playwright Chromium...")
        try:
            subprocess.run(
                [str(PROJECT_ROOT / "venv" / "bin" / "playwright"), "install", "chromium"],
                capture_output=True, timeout=120,
            )
            ok2, msg2 = check_playwright_binary()
            if ok2:
                all_results.append(("Playwright (auto-fix)", True, "Reinstalled successfully"))
                critical_fail = False
            else:
                all_results.append(("Playwright (auto-fix)", False, msg2))
        except Exception as e:
            all_results.append(("Playwright (auto-fix)", False, str(e)))

    # --- 2. SM_PASSWORD ---
    ok, msg = check_sm_password()
    all_results.append(("SM Password", ok, msg))
    if not ok:
        critical_fail = True

    # --- 3. Telegram bot ---
    ok, msg = check_telegram_bot()
    all_results.append(("Telegram Bot", ok, msg))
    if not ok:
        # Try auto-fix: restart via launchctl
        logger.info("Attempting to restart Telegram bot...")
        try:
            uid = subprocess.run(
                ["id", "-u"], capture_output=True, text=True
            ).stdout.strip()
            subprocess.run(
                ["launchctl", "kickstart", "-k", f"gui/{uid}/com.john.telegrambot"],
                capture_output=True, timeout=10,
            )
            import time
            time.sleep(3)
            ok2, msg2 = check_telegram_bot()
            all_results.append(("Telegram Bot (auto-fix)", ok2, msg2))
        except Exception as e:
            all_results.append(("Telegram Bot (auto-fix)", False, str(e)))

    # --- 4-7: SM checks (need Playwright working) ---
    if not critical_fail:
        try:
            from playwright.sync_api import sync_playwright

            # Get today's matches (don't count against checks if API fails)
            matches = _get_todays_matches()
            if matches:
                all_results.append(
                    ("Matches Today", True,
                     f"{len(matches)} matches in next 24h across "
                     f"{len(set(m['slug'] for m in matches))} leagues")
                )
            else:
                all_results.append(("Matches Today", True, "No matches found (API may be down or no games today)"))

            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(viewport={"width": 1400, "height": 900})
                page = context.new_page()

                # --- 4. SM login ---
                try:
                    page.goto("https://pro.sportmarket.com/login", timeout=30000)
                    page.wait_for_selector('input[type="text"]', timeout=10000)
                    page.fill('input[type="text"]', os.getenv("SM_USERNAME", "joelbrown95"))
                    page.fill('input[type="password"]', os.getenv("SM_PASSWORD", ""))
                    page.get_by_role("button", name="log In").click()
                    page.wait_for_url(
                        lambda url: "/sportsbook" in url or "/trade" in url,
                        timeout=30000,
                    )
                    all_results.append(("SM Login", True, "Logged in OK"))
                except Exception as e:
                    all_results.append(("SM Login", False, f"Failed: {e}"))
                    critical_fail = True

                if not critical_fail and matches:
                    # --- 5. League prefixes ---
                    prefix_results = check_sm_league_prefixes(page, matches)
                    for ok, msg in prefix_results:
                        all_results.append(("SM Prefix", ok, msg))

                    # --- 6. DOM selectors ---
                    ok, msg = check_sm_dom_selectors(page, matches)
                    all_results.append(("SM DOM Selectors", ok, msg))

                    # --- 7. Resolve SM event IDs for today's matches ---
                    event_results = check_sm_events_resolvable(page, matches)
                    missing_count = sum(1 for ok, _ in event_results if not ok)
                    found_count = sum(1 for ok, _ in event_results if ok)
                    if event_results:
                        all_results.append(
                            ("SM Events", missing_count == 0,
                             f"{found_count}/{len(event_results)} events resolved"
                             + (f" — {missing_count} MISSING" if missing_count else ""))
                        )
                        for ok, msg in event_results:
                            if not ok:
                                all_results.append(("SM Event", False, msg))

                browser.close()

            # --- 8. Team registry gaps ---
            from team_name_registry import get_unmatched_summary
            gaps = get_unmatched_summary()
            sm_gaps = [g for g in gaps if "sm" in g["missing"]]
            if sm_gaps:
                # Only report teams that are in leagues with matches today
                today_slugs = set(m["slug"] for m in matches)
                relevant_gaps = [g for g in sm_gaps if g["league"] in today_slugs]
                if relevant_gaps:
                    gap_lines = []
                    for g in relevant_gaps[:10]:
                        known = ", ".join(f"{k}={v}" for k, v in g["sources"].items() if v)
                        gap_lines.append(f"{g['canonical']} [{g['league']}] ({known})")
                    all_results.append(
                        ("Team Registry", False,
                         f"{len(relevant_gaps)} teams missing SM names: "
                         + "; ".join(gap_lines))
                    )
                else:
                    all_results.append(
                        ("Team Registry", True,
                         f"All teams for today's leagues have SM mappings")
                    )
            else:
                all_results.append(
                    ("Team Registry", True, "All teams have complete mappings")
                )

        except Exception as e:
            all_results.append(("SM Checks", False, f"Playwright session failed: {e}"))

    # --- Build summary ---
    failures = [(name, msg) for name, ok, msg in all_results if not ok]
    passes = [(name, msg) for name, ok, msg in all_results if ok]

    lines = []
    if failures:
        lines.append("❌ <b>PRE-FLIGHT ISSUES</b>\n")
        for name, msg in failures:
            lines.append(f"  ✗ <b>{name}</b>: {msg}")
        lines.append("")

    # Show key checks concisely
    lines.append(f"✅ <b>{len(passes)} checks passed</b>")
    # Only show important pass details, not every prefix
    key_checks = ["Playwright", "SM Password", "Telegram Bot", "SM Login",
                  "Matches Today", "SM DOM Selectors", "SM Events", "Team Registry"]
    for name, msg in passes:
        if name in key_checks or "(auto-fix)" in name:
            lines.append(f"  ✓ {name}: {msg}")

    summary = "\n".join(lines)
    logger.info(f"Pre-flight complete: {len(passes)} pass, {len(failures)} fail")

    # Send via Telegram
    from telegram_alerts import send_message
    # Only flag as failed for critical issues (not prefix/event availability)
    critical_names = {"Playwright", "SM Password", "Telegram Bot", "SM Login",
                      "SM DOM Selectors", "Playwright (auto-fix)", "SM Events"}
    critical_failures = [f for f in failures if f[0] in critical_names]
    title = "🔴 PRE-FLIGHT FAILED" if critical_failures else "🟢 PRE-FLIGHT OK"
    send_message(f"{title}\n\n{summary}")

    return len(failures) == 0


if __name__ == "__main__":
    success = run_preflight()
    sys.exit(0 if success else 1)
