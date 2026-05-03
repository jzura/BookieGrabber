"""
Cross-platform team name registry.

Collects team names from all three sources (Odds API/Bet365, Betfair, SportsMarket)
and maintains a persistent mapping so fuzzy matching only happens once per team.

Registry file: data/state/team_registry.json
Format:
{
  "english_premier_league": {
    "Arsenal": {
      "odds_api": "Arsenal FC",
      "betfair": "Arsenal",
      "sm": "Arsenal"
    },
    ...
  }
}

When a new team name appears that can't be matched to any existing canonical
entry, a Telegram message is sent asking the user to confirm the mapping.
"""

import json
import logging
from pathlib import Path
from difflib import SequenceMatcher

from constants import PROJECT_ROOT
REGISTRY_FILE = PROJECT_ROOT / "data" / "state" / "team_registry.json"
PENDING_FILE = PROJECT_ROOT / "data" / "state" / "team_registry_pending.json"

logger = logging.getLogger("team_registry")


def _load_registry():
    if REGISTRY_FILE.exists():
        try:
            return json.loads(REGISTRY_FILE.read_text())
        except Exception:
            pass
    return {}


def _save_registry(registry):
    REGISTRY_FILE.parent.mkdir(parents=True, exist_ok=True)
    REGISTRY_FILE.write_text(json.dumps(registry, indent=2, ensure_ascii=False))


def _load_pending():
    if PENDING_FILE.exists():
        try:
            return json.loads(PENDING_FILE.read_text())
        except Exception:
            pass
    return {}


def _save_pending(pending):
    PENDING_FILE.parent.mkdir(parents=True, exist_ok=True)
    PENDING_FILE.write_text(json.dumps(pending, indent=2, ensure_ascii=False))


def _best_match(name, candidates, threshold=0.6):
    """Find the best fuzzy match for name among candidates."""
    name_lower = name.lower()
    best_score = 0
    best_match = None
    for c in candidates:
        score = SequenceMatcher(None, name_lower, c.lower()).ratio()
        if score > best_score:
            best_score = score
            best_match = c
    if best_score >= threshold:
        return best_match, best_score
    return None, best_score


def register_names(league_slug, source, team_names):
    """Register a list of team names from a specific source.

    source: "odds_api", "betfair", or "sm"
    team_names: list of team name strings

    Auto-matches to existing canonical entries where possible.
    Returns list of unmatched names that need user resolution.
    """
    registry = _load_registry()
    if league_slug not in registry:
        registry[league_slug] = {}

    league = registry[league_slug]
    unmatched = []

    for name in team_names:
        if not name or not name.strip():
            continue
        name = name.strip()

        # Check if this exact name is already registered for this source
        already_registered = False
        for canonical, sources in league.items():
            if sources.get(source) == name:
                already_registered = True
                break
        if already_registered:
            continue

        # Try to match to existing canonical entry
        # First: exact match on any existing source name
        matched_canonical = None
        for canonical, sources in league.items():
            for src, src_name in sources.items():
                if src_name and src_name.lower() == name.lower():
                    matched_canonical = canonical
                    break
            if matched_canonical:
                break

        # Second: fuzzy match on canonical names and all source names
        if not matched_canonical:
            all_names = {}
            for canonical, sources in league.items():
                all_names[canonical] = canonical
                for src, src_name in sources.items():
                    if src_name:
                        all_names[src_name] = canonical

            if all_names:
                best, score = _best_match(name, all_names.keys(), threshold=0.7)
                if best:
                    matched_canonical = all_names[best]

        if matched_canonical:
            league[matched_canonical][source] = name
        else:
            # New team — create a canonical entry if this is odds_api (primary source)
            if source == "odds_api":
                league[name] = {"odds_api": name}
            else:
                unmatched.append(name)

    _save_registry(registry)
    return unmatched


def lookup(league_slug, source_from, source_to, team_name):
    """Look up a team name across platforms.

    e.g. lookup("english_premier_league", "odds_api", "sm", "Arsenal FC")
    returns the SM name for Arsenal, or None if not mapped.
    """
    registry = _load_registry()
    league = registry.get(league_slug, {})

    for canonical, sources in league.items():
        if sources.get(source_from) == team_name:
            return sources.get(source_to)

    # Fuzzy fallback
    for canonical, sources in league.items():
        src_name = sources.get(source_from, "")
        if src_name and SequenceMatcher(None, src_name.lower(), team_name.lower()).ratio() > 0.8:
            return sources.get(source_to)

    return None


def lookup_sm_event_id(league_slug, home_odds_api, away_odds_api, sm_events):
    """Given Odds API team names, find the matching SM event from a list.

    sm_events: list of dicts with 'text' (team names from SM page) and 'href'
    Returns SM event ID string or None.
    """
    import re
    registry = _load_registry()
    league = registry.get(league_slug, {})

    # Get SM names for home and away
    home_sm = None
    away_sm = None
    for canonical, sources in league.items():
        if sources.get("odds_api") == home_odds_api:
            home_sm = sources.get("sm", "").lower()
        if sources.get("odds_api") == away_odds_api:
            away_sm = sources.get("sm", "").lower()

    if not home_sm or not away_sm:
        return None

    for ev in sm_events:
        txt = ev["text"].lower()
        if home_sm in txt and away_sm in txt:
            match = re.search(r'(\d{4}-\d{2}-\d{2},\d+,\d+)', ev.get("href", ""))
            if match:
                return match.group(1)

    return None


def collect_from_odds_api(league_slug, events):
    """Record team names from Odds API events list."""
    names = set()
    for ev in events:
        if ev.get("home"):
            names.add(ev["home"])
        if ev.get("away"):
            names.add(ev["away"])
    if names:
        register_names(league_slug, "odds_api", list(names))


def collect_from_betfair(league_slug, bf_teams):
    """Record team names from Betfair exchange data."""
    if bf_teams:
        unmatched = register_names(league_slug, "betfair", list(bf_teams))
        if unmatched:
            logger.info(f"[{league_slug}] Unmatched BF teams: {unmatched}")


def collect_from_sm(league_slug, sm_team_names):
    """Record team names from SportsMarket sportsbook page."""
    if sm_team_names:
        unmatched = register_names(league_slug, "sm", list(sm_team_names))
        return unmatched
    return []


def get_unmatched_summary():
    """Return a summary of all teams that have incomplete cross-platform mappings."""
    registry = _load_registry()
    issues = []

    for league_slug, teams in registry.items():
        for canonical, sources in teams.items():
            missing = []
            if not sources.get("odds_api"):
                missing.append("odds_api")
            if not sources.get("betfair"):
                missing.append("betfair")
            if not sources.get("sm"):
                missing.append("sm")
            if missing:
                issues.append({
                    "league": league_slug,
                    "canonical": canonical,
                    "sources": sources,
                    "missing": missing,
                })

    return issues


def build_initial_registry():
    """Bootstrap the registry from existing team_name_map.json files and SM pages.

    Reads all mappings/{league}/team_name_map.json files to populate
    odds_api → betfair mappings as a starting point.
    """
    import yaml
    cfg = yaml.safe_load((PROJECT_ROOT / "config.yaml").read_text())
    registry = _load_registry()

    for league in cfg.get("leagues", []):
        slug = league["slug"]
        map_file = PROJECT_ROOT / "mappings" / slug / "team_name_map.json"
        if not map_file.exists():
            continue

        team_map = json.loads(map_file.read_text())
        if slug not in registry:
            registry[slug] = {}

        for odds_api_name, bf_name in team_map.items():
            # Use odds_api name as canonical
            if odds_api_name not in registry[slug]:
                registry[slug][odds_api_name] = {}
            registry[slug][odds_api_name]["odds_api"] = odds_api_name
            registry[slug][odds_api_name]["betfair"] = bf_name

    _save_registry(registry)
    total_teams = sum(len(teams) for teams in registry.values())
    logger.info(f"Registry bootstrapped: {total_teams} teams across {len(registry)} leagues")
    return registry
