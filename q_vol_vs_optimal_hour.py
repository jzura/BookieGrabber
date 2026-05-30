"""Per-league analysis: at which hour-before-KO does each league's ROI peak,
and is that hour correlated with league volume?
"""
import sys
from pathlib import Path
sys.path.insert(0, "/Users/Joel/REPOS/BookieGrabber")
import pandas as pd
import numpy as np
from strategy_config import (
    compute_rpd, is_core_qualifying, is_btts_fade,
    estimate_sm_odds, FADE_ODDS_HAIRCUT,
)

ROOT = Path("/Users/Joel/REPOS/BookieGrabber")
tl = pd.read_csv(ROOT / "dashboard_data" / "odds_timeline_summary.csv")
df = pd.read_csv(ROOT / "dashboard_data" / "bets.csv")
tl["target_hours_before"] = pd.to_numeric(tl["target_hours_before"], errors="coerce")

df["Result"] = pd.to_numeric(df["Result"], errors="coerce")
df["Date"] = pd.to_datetime(df["Date"]).dt.date
tl["match_date"] = pd.to_datetime(tl["match_time"]).dt.date

merged = tl.merge(
    df[["Date", "Home", "Away", "Market", "Result"]].drop_duplicates(),
    left_on=["match_date", "home_team", "away_team"],
    right_on=["Date", "Home", "Away"], how="inner"
)
merged = merged[merged["Result"].notna()]

market_cols = {
    "1.5G": {"bf": "bf_under_1_5", "b365": "b365_under_1_5", "vol": "vol_1_5"},
    "3.5G": {"bf": "bf_under_3_5", "b365": "b365_under_3_5", "vol": "vol_3_5"},
    "BTTS": {"bf": "bf_btts_no",   "b365": "b365_btts_no",   "vol": "vol_btts"},
}

cells = []
for (league, target_h), subset in merged.groupby(["league", "target_hours_before"]):
    n_bets = 0; total_stake = 0.0; total_return = 0.0; vols = []
    for _, row in subset.iterrows():
        mkt = row.get("Market")
        if mkt not in market_cols: continue
        mc = market_cols[mkt]
        try:
            bf = float(row[mc["bf"]]); b365 = float(row[mc["b365"]]); vol = float(row[mc["vol"]])
        except Exception:
            continue
        if not (bf > 1 and b365 > 0 and vol >= 0): continue
        rpd = compute_rpd(b365, bf)
        result = int(row["Result"])
        is_core = is_core_qualifying(mkt, 0, bf, vol, rpd)
        is_fade = is_btts_fade(mkt, 0, rpd, vol) if mkt == "BTTS" else False
        if not is_core and not is_fade: continue
        n_bets += 1; total_stake += 1; vols.append(vol)
        if is_fade:
            if result == 0:
                opp = 1/(1-1/bf)*(1-FADE_ODDS_HAIRCUT)
                c = 0.01 if opp<=1.5 else 0.02 if opp<=2.8 else 0.03 if opp<=3.5 else 0.04
                total_return += 1*(1+(opp-1)*(1-c))
        elif result == 1:
            odds = estimate_sm_odds(bf)
            c = 0.01 if odds<=1.5 else 0.02 if odds<=2.8 else 0.03 if odds<=3.5 else 0.04
            total_return += 1*(1+(odds-1)*(1-c))
    if n_bets >= 5:  # need at least 5 bets per cell to be meaningful
        cells.append({"league": league, "hours": int(target_h), "bets": n_bets,
                      "roi": (total_return-total_stake)/total_stake*100,
                      "avg_vol": float(np.mean(vols))})

cells_df = pd.DataFrame(cells)

# Per league: avg_vol across all timeslices, best timeslice
rows = []
for league, g in cells_df.groupby("league"):
    avg_vol = g["avg_vol"].mean()
    best = g.loc[g["roi"].idxmax()]
    total_bets = int(g["bets"].sum())
    rows.append({
        "league": league,
        "avg_vol": round(avg_vol, 0),
        "best_hour": int(best["hours"]),
        "best_roi": round(best["roi"], 2),
        "n_at_best": int(best["bets"]),
        "total_bets": total_bets,
        "timeslices_with_data": int(len(g)),
    })

result = pd.DataFrame(rows).sort_values("avg_vol", ascending=False)
print(result.to_string(index=False))
print()

# Correlation
from scipy.stats import spearmanr, pearsonr
sp_r, sp_p = spearmanr(result["avg_vol"], result["best_hour"])
ps_r, ps_p = pearsonr(np.log(result["avg_vol"]), result["best_hour"])  # log because volume spans orders of magnitude
print(f"Spearman corr (rank-based) between avg_vol and best_hour: r = {sp_r:+.3f}, p = {sp_p:.3f}")
print(f"Pearson corr (log-vol vs best_hour):                       r = {ps_r:+.3f}, p = {ps_p:.3f}")
print()
print("Interpretation: negative correlation => higher volume leagues peak EARLIER (closer to KO).")
print("                positive correlation => higher volume leagues peak LATER (further from KO).")
