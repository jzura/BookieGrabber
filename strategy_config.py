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
BF_TIER1 = 3.0
RPD_TIER1 = 4.0   # max RPD when BF <= BF_TIER1
RPD_TIER2 = 6.0   # max RPD when BF > BF_TIER1

# ─── Fade thresholds ───
BTTS_FADE_RPD = 5.0    # BTTS pred=0 with RPD >= this → fade to Yes
G15_FADE_RPD = 4.6     # 1.5G pred=1 with RPD >= this → fade to Under

# ─── Double stake ───
DOUBLE_STAKE_RPD = 1.0       # RPD must equal this to qualify for 2x
DOUBLE_STAKE_MIN_COUNT = 2   # min core bets on same match for 2x

# ─── Core bet types ───
CORE_BET_TYPES = ("1.5G", "3.5G", "BTTS")

# ─── Stake per unit (EUR) ───
STAKE_PER_UNIT = 250.0


def is_core_qualifying(bet_type, prediction, bf, vol, rpd):
    """Check if a bet qualifies under core contrarian rules.
    Returns True if it qualifies."""
    if bet_type not in CORE_BET_TYPES:
        return False
    if prediction != 0:
        return False
    if vol is None or not (VOL_MIN <= vol <= VOL_MAX):
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


def is_btts_fade(bet_type, prediction, rpd, vol):
    """Check if a BTTS bet qualifies as a fade (bet Yes instead of No)."""
    if bet_type != "BTTS":
        return False
    if prediction != 0:
        return False
    if rpd is None or rpd < BTTS_FADE_RPD:
        return False
    if vol is None or not (VOL_MIN <= vol <= VOL_MAX):
        return False
    return True


def is_15g_fade(bet_type, prediction, rpd, vol):
    """Check if a 1.5G bet qualifies as a fade (bet Under instead of Over)."""
    if bet_type != "1.5G":
        return False
    if prediction != 1:
        return False
    if rpd is None or rpd < G15_FADE_RPD:
        return False
    if vol is None or not (VOL_MIN <= vol <= VOL_MAX):
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
