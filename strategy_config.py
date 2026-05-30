"""
Centralised strategy parameters for the betting pipeline.

All qualification rules, thresholds, and constants are defined here.
Every module (bet_tracker_updater, export_dashboard_data, results_updater,
bookie_postproc, etc.) imports from this file instead of hardcoding values.

To change a parameter, update it here — it propagates everywhere automatically.
"""

# ─── Volume bounds ───
VOL_MIN = 40.0
VOL_MAX = 1100.0

# ─── BF odds minimum ───
BF_MIN = 1.45

# ─── RPD tier schedule ───
# Core contrarian: pred=0, within volume/BF bounds, RPD within tier threshold
# BF <= BF_TIER1 → RPD <= RPD_TIER1
# BF > BF_TIER1  → RPD <= RPD_TIER2
# Previous values (for easy rollback):
# BF_TIER1 = 2.7, RPD_TIER1 = 2.8, RPD_TIER2 = 3.5
# Changed to 3.0 on 2026-04-20, reverted to 2.7 same day
BF_TIER1 = 2.7
RPD_TIER1 = 4.0   # max RPD when BF <= BF_TIER1
RPD_TIER2 = 5.0   # max RPD when BF > BF_TIER1  (was 6.0, tightened 2026-05-30)

# ─── Fade thresholds ───
BTTS_FADE_RPD = 5.0    # BTTS pred=0 with RPD >= this → fade to Yes
G15_FADE_RPD = 4.5     # 1.5G pred=1 with RPD >= this → fade to Under

# ─── Fade odds haircut ───
# Theoretical opposite odds = 1/(1-1/BF). Real SM fills are lower due to
# bookmaker margin on the less popular side. Based on 4 actual fade fills,
# actual odds average 3.6% below theoretical. Using 4% haircut for safety.
FADE_ODDS_HAIRCUT = 0.04

# ─── BF-to-SM odds discount tiers ───
# When SM_Odds are not available, we estimate actual SM fill price by
# discounting BF odds. Based on 148 actual BF vs SM comparisons.
# Higher BF odds = wider spread = bigger discount.
BF_SM_DISCOUNT_TIERS = [
    (1.50, 0.002),   # BF <= 1.50: 0.2% discount
    (1.80, 0.007),   # 1.50 < BF <= 1.80: 0.7% discount
    (2.50, 0.011),   # 1.80 < BF <= 2.50: 1.1% discount
    (3.50, 0.032),   # 2.50 < BF <= 3.50: 3.2% discount
    (999,  0.042),   # BF > 3.50: 4.2% discount
]


def compute_rpd(o365, bf):
    """Compute Relative Price Difference between Bet365 and Betfair odds.
    Returns 1.0 when B365 > BF, percentage otherwise, or None on error."""
    try:
        a, b = float(o365), float(bf)
        if a > b:
            return 1.0
        pct = abs(a - b) / ((a + b) / 2) * 100
        return 1.0 if pct < 1 else round(pct, 3)
    except Exception:
        return None


def estimate_sm_odds(bf_odds):
    """Estimate SM fill price from BF odds using tiered discount."""
    for threshold, discount in BF_SM_DISCOUNT_TIERS:
        if bf_odds <= threshold:
            return bf_odds * (1 - discount)
    return bf_odds * (1 - BF_SM_DISCOUNT_TIERS[-1][1])

# ─── Double stake ───
# Previous: 1.0, then 1.2
DOUBLE_STAKE_RPD = 3.5       # RPD must be <= this to qualify for 2x
DOUBLE_STAKE_MIN_COUNT = 2   # min core bets on same match for 2x

# ─── Core bet types ───
CORE_BET_TYPES = ("1.5G", "3.5G", "BTTS")

# ─── Stake per unit (EUR) ───
STAKE_PER_UNIT_WEEKEND = 250.0   # Saturday, Sunday
STAKE_PER_UNIT_MIDWEEK = 500.0   # Monday–Friday


def get_stake_per_unit(match_date=None, match_time=None):
    """Return stake per unit based on day of week in match-local (European) time.

    `match_date` is the Perth-local kickoff date (date or "YYYY-MM-DD" string),
    and `match_time` is the Perth-local kickoff time ("HH:MM" or "HH:MM AWST").
    Perth is UTC+8 and European kickoffs are 6–8h behind Perth, so a Perth-Monday
    03:00 kickoff is still a Sunday match in Europe and must qualify as weekend.
    When `match_time` is given we shift Perth → UTC to recover the match-local
    weekday; otherwise we fall back to the supplied date or today.
    """
    from datetime import date, datetime, timedelta

    if match_date is None:
        match_date = date.today()

    if isinstance(match_date, str):
        try:
            match_date = datetime.strptime(match_date, "%Y-%m-%d").date()
        except Exception:
            return STAKE_PER_UNIT_MIDWEEK

    weekday = None
    if match_time and hasattr(match_date, "year") and not isinstance(match_date, datetime):
        try:
            hhmm = str(match_time).strip().split()[0]
            hh, mm = hhmm.split(":")[:2]
            perth_dt = datetime(match_date.year, match_date.month, match_date.day,
                                int(hh), int(mm))
            weekday = (perth_dt - timedelta(hours=8)).weekday()
        except Exception:
            weekday = None

    if weekday is None:
        if isinstance(match_date, datetime) and match_date.tzinfo is not None:
            import pytz
            weekday = match_date.astimezone(pytz.UTC).weekday()
        elif hasattr(match_date, "weekday"):
            weekday = match_date.weekday()

    if weekday in (5, 6):  # Saturday=5, Sunday=6
        return STAKE_PER_UNIT_WEEKEND
    return STAKE_PER_UNIT_MIDWEEK


def is_core_qualifying(bet_type, prediction, bf, vol, rpd, vol_max=VOL_MAX):
    """Check if a bet qualifies under core contrarian rules.
    Returns True if it qualifies. `vol_max` defaults to the global cap but can
    be overridden per-league (e.g. World Cup markets carry far higher volume)."""
    if bet_type not in CORE_BET_TYPES:
        return False
    if prediction != 0:
        return False
    if vol is None or not (VOL_MIN <= vol <= vol_max):
        return False
    if bf is None or bf <= BF_MIN:
        return False
    if rpd is None:
        return False
    if bf <= BF_TIER1 and rpd > RPD_TIER1:
        return False
    if bf > BF_TIER1 and rpd > RPD_TIER2:
        return False
    return True


def is_btts_fade(bet_type, prediction, rpd, vol, vol_max=VOL_MAX):
    """Check if a BTTS bet qualifies as a fade (bet Yes instead of No)."""
    if bet_type != "BTTS":
        return False
    if prediction != 0:
        return False
    if rpd is None or rpd < BTTS_FADE_RPD:
        return False
    if vol is None or not (VOL_MIN <= vol <= vol_max):
        return False
    return True


def is_15g_fade(bet_type, prediction, rpd, vol, vol_max=VOL_MAX):
    """Check if a 1.5G bet qualifies as a fade (bet Under instead of Over)."""
    if bet_type != "1.5G":
        return False
    if prediction != 1:
        return False
    if rpd is None or rpd < G15_FADE_RPD:
        return False
    if vol is None or not (VOL_MIN <= vol <= vol_max):
        return False
    return True


def is_25g_piggyback(bet_type, prediction, bf, core_15g_match):
    """2.5G piggyback — DISABLED. Minimal profit vs other markets."""
    return False


def core_conditions_excel(r):
    """Excel formula string for core contrarian conditions."""
    return (
        f'AND(OR(A{r}="1.5G",A{r}="3.5G",A{r}="BTTS"),H{r}=0,'
        f'K{r}>={VOL_MIN},K{r}<={VOL_MAX},J{r}>{BF_MIN},'
        f'OR(AND(J{r}<={BF_TIER1},L{r}<={RPD_TIER1}),'
        f'AND(J{r}>{BF_TIER1},L{r}<={RPD_TIER2})))'
    )


def fade_conditions_excel(r):
    """Excel formula string for fade conditions."""
    return (
        f'AND(A{r}="BTTS",H{r}=0,L{r}>={BTTS_FADE_RPD},'
        f'K{r}>={VOL_MIN},K{r}<={VOL_MAX},P{r}<>""),'
        f'AND(A{r}="1.5G",H{r}=1,L{r}>={G15_FADE_RPD},'
        f'K{r}>={VOL_MIN},K{r}<={VOL_MAX})'
    )


def piggyback_conditions_excel(r):
    """Excel formula string for 2.5G piggyback (pre-qualified in Python)."""
    return f'AND(A{r}="2.5G",H{r}=0,J{r}>{BF_MIN},P{r}<>"")'
