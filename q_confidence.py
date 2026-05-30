"""Statistical confidence on the two headline cells:
  - High-volume leagues at 5h before KO (+19% claim)
  - Low-volume leagues at 12h before KO (+34% claim)
Computes per-bet P/L, mean ROI, standard error, t-stat, and a bootstrap 95% CI.
"""
import sys
from pathlib import Path
sys.path.insert(0, "/Users/Joel/REPOS/BookieGrabber")
import pandas as pd
import numpy as np
from strategy_config import (compute_rpd, is_core_qualifying, is_btts_fade,
                             estimate_sm_odds, FADE_ODDS_HAIRCUT)

ROOT = Path("/Users/Joel/REPOS/BookieGrabber")
tl = pd.read_csv(ROOT / "dashboard_data" / "odds_timeline_summary.csv")
df = pd.read_csv(ROOT / "dashboard_data" / "bets.csv")
tl["target_hours_before"] = pd.to_numeric(tl["target_hours_before"], errors="coerce")
df["Result"] = pd.to_numeric(df["Result"], errors="coerce")
df["Date"] = pd.to_datetime(df["Date"]).dt.date
tl["match_date"] = pd.to_datetime(tl["match_time"]).dt.date

merged = tl.merge(df[["Date","Home","Away","Market","Result"]].drop_duplicates(),
                  left_on=["match_date","home_team","away_team"],
                  right_on=["Date","Home","Away"], how="inner")
merged = merged[merged["Result"].notna()]

market_cols = {
    "1.5G": {"bf":"bf_under_1_5","b365":"b365_under_1_5","vol":"vol_1_5"},
    "3.5G": {"bf":"bf_under_3_5","b365":"b365_under_3_5","vol":"vol_3_5"},
    "BTTS": {"bf":"bf_btts_no","b365":"b365_btts_no","vol":"vol_btts"},
}

def per_bet_pl(subset):
    """Return list of per-bet profit (units) for qualifying bets in subset."""
    pls = []
    for _, row in subset.iterrows():
        mkt = row.get("Market")
        if mkt not in market_cols: continue
        mc = market_cols[mkt]
        try:
            bf=float(row[mc["bf"]]); b365=float(row[mc["b365"]]); vol=float(row[mc["vol"]])
        except Exception:
            continue
        if not (bf>1 and b365>0 and vol>=0): continue
        rpd=compute_rpd(b365,bf); result=int(row["Result"])
        is_core=is_core_qualifying(mkt,0,bf,vol,rpd)
        is_fade=is_btts_fade(mkt,0,rpd,vol) if mkt=="BTTS" else False
        if not is_core and not is_fade: continue
        if is_fade:
            if result==0:
                opp=1/(1-1/bf)*(1-FADE_ODDS_HAIRCUT)
                c=0.01 if opp<=1.5 else 0.02 if opp<=2.8 else 0.03 if opp<=3.5 else 0.04
                pls.append((opp-1)*(1-c))
            else:
                pls.append(-1.0)
        else:
            if result==1:
                odds=estimate_sm_odds(bf)
                c=0.01 if odds<=1.5 else 0.02 if odds<=2.8 else 0.03 if odds<=3.5 else 0.04
                pls.append((odds-1)*(1-c))
            else:
                pls.append(-1.0)
    return np.array(pls)

# Build per-league avg vol to split tiers (same as dashboard)
cells=[]
for (league,h),sub in merged.groupby(["league","target_hours_before"]):
    pls=per_bet_pl(sub)
    if len(pls)>=5:
        vols=[]
        for _,row in sub.iterrows():
            mkt=row.get("Market")
            if mkt in market_cols:
                try: vols.append(float(row[market_cols[mkt]["vol"]]))
                except: pass
        cells.append({"league":league,"h":int(h),"n":len(pls),"avg_vol":np.mean(vols)})
cdf=pd.DataFrame(cells)
lav=cdf.groupby("league")["avg_vol"].mean()
med=lav.median()
high=set(lav[lav>=med].index); low=set(lav[lav<med].index)

def analyse(label, leagues, hour):
    sub = merged[(merged["league"].isin(leagues)) & (merged["target_hours_before"]==hour)]
    pls = per_bet_pl(sub)
    n=len(pls)
    if n==0:
        print(f"{label}: no bets"); return
    mean=pls.mean(); sd=pls.std(ddof=1); se=sd/np.sqrt(n)
    roi=mean*100
    t=mean/se if se>0 else 0
    # bootstrap 95% CI on ROI
    rng=np.random.default_rng(42)
    boots=[rng.choice(pls,size=n,replace=True).mean()*100 for _ in range(10000)]
    lo,hi=np.percentile(boots,[2.5,97.5])
    # rough two-sided p-value from t (normal approx)
    from math import erf, sqrt
    p=2*(1-0.5*(1+erf(abs(t)/sqrt(2))))
    print(f"{label}")
    print(f"  n bets           : {n}")
    print(f"  ROI (mean P/L)   : {roi:+.2f}% per unit")
    print(f"  std dev per bet  : {sd:.2f} units  (huge — odds-based payouts)")
    print(f"  std error        : {se*100:.2f}%")
    print(f"  t-stat vs 0      : {t:+.2f}   (p ~ {p:.3f})")
    print(f"  bootstrap 95% CI : [{lo:+.1f}%, {hi:+.1f}%]")
    print(f"  >>> {'CI excludes 0 (signal)' if lo>0 else 'CI INCLUDES 0 — not significant'}")
    print()

analyse("HIGH-VOLUME leagues @ 5h before KO", high, 5)
analyse("LOW-VOLUME leagues @ 12h before KO", low, 12)
# Also the overall full-sample baseline for reference
allpls = per_bet_pl(merged.drop_duplicates(subset=["home_team","away_team","match_date","Market","target_hours_before"]))
print(f"(For reference) every qualifying snapshot, all hours/leagues pooled: "
      f"n={len(allpls)}, ROI={allpls.mean()*100:+.2f}%")
