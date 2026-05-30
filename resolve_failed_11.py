"""Resolve SM event IDs for the 11 failed bets and update sm_event_map.json.

Logs into SM via Playwright, scrapes each relevant league page,
matches by fuzzy team-name overlap, and updates the map.
"""

import os, sys, json, re, logging
from pathlib import Path
from difflib import SequenceMatcher

sys.path.insert(0, "/Users/Joel/REPOS/BookieGrabber")

from playwright.sync_api import sync_playwright
from sportsmarket_api import SM_LEAGUE_PREFIXES, SM_SPORTSBOOK_BASE
from dotenv import load_dotenv

load_dotenv("/Users/Joel/REPOS/BookieGrabber/.env", override=True)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("resolver")

SM_MAP = Path("/Users/Joel/REPOS/BookieGrabber/data/state/sm_event_map.json")
PENDING = Path("/Users/Joel/REPOS/BookieGrabber/telegram_pending_bets.json")

# (home_team, away_team, slug)
FAILED_BETS = [
    ("Real Oviedo", "Deportivo Alaves", "spanish_la_liga"),
    ("Atletico Madrid", "Girona FC", "spanish_la_liga"),
    ("Sevilla FC", "Real Madrid", "spanish_la_liga"),
    ("Athletic Bilbao", "RC Celta de Vigo", "spanish_la_liga"),
    ("Real Sociedad San Sebastian", "Valencia CF", "spanish_la_liga"),
    ("Cagliari Calcio", "Torino FC", "italian_serie_a"),
    ("Sassuolo Calcio", "US Lecce", "italian_serie_a"),
    ("FC Lugano", "FC Basel 1893", "swiss_super_league"),
    ("FC Midtjylland", "Broendby IF", "danish_superliga"),
    ("AEK Athens", "Olympiacos Piraeus", "greek_super_league"),
    ("FK Radnik Surdulica", "FK Crvena Zvezda Belgrade", "serbian_super_league"),
]


def norm(s):
    s = re.sub(r'\b(FC|CF|SC|FK|GNK|HNK|RC|NK|AC|AS|AFC|US|RCD|RKS|KS|GKS|SK|OFK)\b', '', s, flags=re.IGNORECASE)
    s = re.sub(r'[^\w\s]', ' ', s)
    return ' '.join(s.lower().split())


def best_match(target_home, target_away, events):
    """Find event whose two team strings best overlap with target."""
    th, ta = norm(target_home), norm(target_away)
    best, best_score = None, 0
    for ev in events:
        teams = ev.get("teams") or []
        if len(teams) < 2:
            # Fallback: use full text
            txt = norm(ev.get("text", ""))
            score = 0
            for w in th.split():
                if w and w in txt:
                    score += 1
            for w in ta.split():
                if w and w in txt:
                    score += 1
            if score > best_score:
                best, best_score = ev, score
            continue
        eh, ea = norm(teams[0]), norm(teams[1])
        s = (SequenceMatcher(None, th, eh).ratio() + SequenceMatcher(None, ta, ea).ratio()) / 2
        s_swap = (SequenceMatcher(None, th, ea).ratio() + SequenceMatcher(None, ta, eh).ratio()) / 2
        score = max(s, s_swap)
        if score > best_score:
            best, best_score = ev, score
    return best, best_score


def extract_events(page):
    return page.evaluate(r'''() => {
        const results = [];
        const links = document.querySelectorAll("a[href*='origin=sportsbook']");
        for (const link of links) {
            const href = link.href;
            const m = href.match(/(\d{4}-\d{2}-\d{2},\d+,\d+)/);
            if (!m) continue;
            const leafDivs = link.querySelectorAll("div");
            const teamNames = [];
            for (const d of leafDivs) {
                if (d.querySelectorAll("div").length > 0) continue;
                const t = d.textContent.trim();
                if (!t || t === "today" || t === "tomorrow") continue;
                if (/^\d+[.:,]\d+$/.test(t)) continue;
                if (/^\d{2}\/\d{2}$/.test(t)) continue;
                if (/^-\/-$/.test(t)) continue;
                if (t.length <= 1) continue;
                if (t === "HT" || t === "FT" || t === "ET") continue;
                if (/^\d+'/.test(t)) continue;
                if (/^\d+$/.test(t)) continue;
                teamNames.push(t);
            }
            results.push({eid: m[1], text: link.textContent.toLowerCase().replace(/\s+/g," "), teams: teamNames.slice(0,2)});
        }
        return results;
    }''')


def main():
    sm_map = json.loads(SM_MAP.read_text())
    log.info(f"Starting map: {len(sm_map)} entries")

    slugs_needed = sorted(set(b[2] for b in FAILED_BETS))
    log.info(f"Need to scrape leagues: {slugs_needed}")

    resolved = {}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width":1400,"height":900})
        page = ctx.new_page()

        # Login
        log.info("Logging into SM...")
        page.goto("https://pro.sportmarket.com/login", timeout=30000)
        page.wait_for_selector('input[type="text"]', timeout=10000)
        page.fill('input[type="text"]', os.getenv("SM_USERNAME", "joelbrown95"))
        page.fill('input[type="password"]', os.getenv("SM_PASSWORD", ""))
        page.get_by_role("button", name="log In").click()
        page.wait_for_url(lambda u: "/sportsbook" in u or "/trade" in u, timeout=30000)
        log.info("Logged in OK")

        seen_prefixes = set()
        events_by_slug = {}
        for slug in slugs_needed:
            prefix = SM_LEAGUE_PREFIXES.get(slug)
            if not prefix:
                log.warning(f"No SM prefix for {slug}, skipping")
                continue
            if prefix in seen_prefixes:
                continue
            seen_prefixes.add(prefix)

            url = f"{SM_SPORTSBOOK_BASE}/{prefix}"
            log.info(f"Visiting {slug} → {url}")
            try:
                page.goto(url, timeout=20000)
                page.wait_for_timeout(2500)
                evs = extract_events(page)
                log.info(f"  extracted {len(evs)} event links")
                events_by_slug[slug] = evs
            except Exception as e:
                log.error(f"  failed: {e}")
                events_by_slug[slug] = []

        browser.close()

    # Match each failed bet
    for home, away, slug in FAILED_BETS:
        evs = events_by_slug.get(slug, [])
        if not evs:
            log.warning(f"NO EVENTS for {slug}: {home} vs {away}")
            continue
        match, score = best_match(home, away, evs)
        if match and score >= 0.55:
            eid = match["eid"]
            key = f"{home}|{away}"
            sm_map[key] = {
                "sm_event_id": eid,
                "slug": slug,
                "prefix": SM_LEAGUE_PREFIXES.get(slug, ""),
            }
            resolved[(home, away)] = eid
            log.info(f"OK   {home} vs {away} → {eid} (score={score:.2f}, sm_teams={match.get('teams')})")
        else:
            log.warning(f"MISS {home} vs {away} (best score={score:.2f}, best_teams={match.get('teams') if match else None})")

    SM_MAP.write_text(json.dumps(sm_map, indent=2))
    log.info(f"Updated map written. Resolved {len(resolved)}/{len(FAILED_BETS)}.")

    # Update pending bets file: replace synthetic event_ids with real ones
    pending = json.loads(PENDING.read_text())
    new_pending = {}
    replaced = 0
    for old_key, bet in pending.items():
        h, a = bet.get("home_team"), bet.get("away_team")
        new_eid = resolved.get((h, a))
        if new_eid:
            bet["event_id"] = new_eid
            new_key = f"{new_eid}:{bet['bet_type']}"
            new_pending[new_key] = bet
            replaced += 1
        else:
            new_pending[old_key] = bet
    PENDING.write_text(json.dumps(new_pending, indent=2, default=str))
    log.info(f"Pending bets file updated: {replaced} keys re-keyed with real SM IDs")


if __name__ == "__main__":
    main()
