"""
Export master spreadsheet data to CSV for the Streamlit Cloud dashboard.
Runs every 12 hours via launchd, then pushes to GitHub.
"""

import logging
import subprocess
from pathlib import Path
from datetime import datetime

import openpyxl
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent
MASTER_PATH = Path.home() / "Desktop" / "EFB_Master_Bet_Tracker_VS Code.xlsx"
DASHBOARD_DIR = PROJECT_ROOT / "dashboard_data"
CSV_PATH = DASHBOARD_DIR / "bets.csv"

LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_PATH = LOG_DIR / f"dashboard_export_{datetime.now().strftime('%Y-%m-%d')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(str(LOG_PATH)), logging.StreamHandler()],
    force=True,
)
logger = logging.getLogger("dashboard_export")


def _rpd(o365, bf):
    try:
        a, b = float(o365), float(bf)
        if a > b: return 1.0
        pct = abs(a - b) / ((a + b) / 2) * 100
        return 1.0 if pct < 1 else round(pct, 3)
    except: return None


def _compute_stake_and_return(df):
    """Compute Stake, Return, Profit from raw data (since Excel formulas aren't cached)."""
    from collections import defaultdict

    df = df.copy()
    df['Stake'] = None
    df['Return'] = None
    df['Profit'] = None

    # Core qualifying check
    def is_core(row):
        if row['Market'] not in ('1.5G', '3.5G', 'BTTS'): return False
        if row['Prediction'] != 0: return False
        try:
            bf = float(row['BF']); vol = float(row['Volume'])
        except: return False
        if not (40 <= vol <= 1100): return False
        if bf <= 1.45: return False
        rpd = _rpd(row['Bet365'], bf)
        if rpd is None: return False
        if bf <= 2.7 and rpd > 2.8: return False
        if bf > 2.7 and rpd > 3.5: return False
        return True

    # Fade check
    def is_btts_fade(row):
        if row['Market'] != 'BTTS': return False
        if row['Prediction'] != 0: return False
        try:
            vol = float(row['Volume'])
        except: return False
        if not (40 <= vol <= 1100): return False
        rpd = _rpd(row['Bet365'], row['BF'])
        if rpd is None or rpd < 5: return False
        return True

    def is_15g_fade(row):
        if row['Market'] != '1.5G': return False
        if row['Prediction'] != 1: return False
        try:
            vol = float(row['Volume'])
        except: return False
        if not (40 <= vol <= 1100): return False
        rpd = _rpd(row['Bet365'], row['BF'])
        if rpd is None or rpd < 4.6: return False
        return True

    # Build core set and double-stake
    core_idx = set()
    match_count = defaultdict(int)
    for idx, row in df.iterrows():
        if is_core(row):
            core_idx.add(idx)
            mk = (str(row['Date']), str(row['Home']), str(row['Away']))
            match_count[mk] += 1

    # Double-stake: highest BF per match with RPD=1.0 and ≥2 core bets
    match_dbl = defaultdict(list)
    for idx in core_idx:
        row = df.loc[idx]
        rpd = _rpd(row['Bet365'], row['BF'])
        mk = (str(row['Date']), str(row['Home']), str(row['Away']))
        if rpd == 1.0 and match_count[mk] >= 2:
            try:
                match_dbl[mk].append((idx, float(row['BF'])))
            except: pass
    dbl_idx = set()
    for mk, candidates in match_dbl.items():
        best = max(candidates, key=lambda x: x[1])[0]
        dbl_idx.add(best)

    # Assign stakes
    for idx, row in df.iterrows():
        stake = 0
        fade = False
        if idx in core_idx:
            stake = 2 if idx in dbl_idx else 1
        elif is_btts_fade(row):
            stake = 1; fade = True
        elif is_15g_fade(row):
            stake = 1; fade = True

        if stake == 0: continue

        result = row.get('Result')
        if result is None or pd.isna(result):
            df.at[idx, 'Stake'] = stake
            continue

        result = int(result)
        try:
            bf = float(row['BF'])
        except: continue

        if fade:
            if result == 0:
                opp = 1 / (1 - 1 / bf)
                c = 0.01 if opp <= 1.5 else 0.02 if opp <= 2.8 else 0.03 if opp <= 3.5 else 0.04
                ret = stake * (1 + (opp - 1) * (1 - c))
            else:
                ret = 0
        else:
            if result == 1:
                c = 0.01 if bf <= 1.5 else 0.02 if bf <= 2.8 else 0.03 if bf <= 3.5 else 0.04
                ret = stake * (1 + (bf - 1) * (1 - c))
            else:
                ret = 0

        df.at[idx, 'Stake'] = stake
        df.at[idx, 'Return'] = round(ret, 4)
        df.at[idx, 'Profit'] = round(ret - stake, 4)

    return df


def export_csv():
    if not MASTER_PATH.exists():
        logger.error(f"Master not found: {MASTER_PATH}")
        return False

    logger.info("Loading master spreadsheet...")
    try:
        wb = openpyxl.load_workbook(MASTER_PATH, data_only=True)
    except PermissionError:
        logger.warning("File locked — skipping export")
        return False

    ws = wb['Master Bet Tracker']

    rows = []
    for r in range(2, ws.max_row + 1):
        bt = ws.cell(row=r, column=1).value
        if bt is None:
            break
        d = ws.cell(row=r, column=2).value
        if isinstance(d, datetime):
            d = d.date()

        # Read raw values — formulas won't be cached
        result = ws.cell(row=r, column=16).value
        hg = ws.cell(row=r, column=14).value
        ag = ws.cell(row=r, column=15).value
        pred = ws.cell(row=r, column=8).value
        goals = ws.cell(row=r, column=13).value

        # Compute result if it's a formula (None in data_only mode)
        if result is None and hg is not None and ag is not None:
            try:
                hg_i, ag_i = int(hg), int(ag)
                tg = hg_i + ag_i
                if bt == 'BTTS':
                    both = (hg_i > 0 and ag_i > 0)
                    result = (1 if both else 0) if pred == 1 else (0 if both else 1)
                elif bt in ('1.5G', '2.5G', '3.5G'):
                    line = float(bt.replace('G', ''))
                    result = (1 if tg > line else 0) if pred == 1 else (1 if tg < line else 0)
            except: pass
        elif result is None and goals is not None:
            try:
                tg = int(goals)
                if bt in ('1.5G', '2.5G', '3.5G'):
                    line = float(bt.replace('G', ''))
                    result = (1 if tg > line else 0) if pred == 1 else (1 if tg < line else 0)
            except: pass

        # Compute RPD
        o365 = ws.cell(row=r, column=9).value
        bf = ws.cell(row=r, column=10).value
        rpd = _rpd(o365, bf)

        rows.append({
            'Market': bt,
            'Date': d,
            'Home': ws.cell(row=r, column=3).value,
            'Away': ws.cell(row=r, column=4).value,
            'Competition': ws.cell(row=r, column=5).value,
            'Prediction': pred,
            'Bet365': o365,
            'BF': bf,
            'Volume': ws.cell(row=r, column=11).value,
            'RPD': rpd,
            'Goals': goals if isinstance(goals, (int, float)) else (int(hg) + int(ag) if hg is not None and ag is not None else None),
            'HG': hg,
            'AG': ag,
            'Result': result,
            'Stake': None,  # computed below
            'Return': None,
            'Profit': None,
            'SM_Odds': ws.cell(row=r, column=22).value,
        })
    wb.close()

    df = pd.DataFrame(rows)
    df = _compute_stake_and_return(df)

    DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(CSV_PATH, index=False)

    n_staked = df['Stake'].notna().sum()
    n_settled = df['Profit'].notna().sum()
    logger.info(f"Exported {len(df)} rows ({n_staked} staked, {n_settled} settled) to {CSV_PATH}")
    return True


def git_push():
    try:
        subprocess.run(["git", "add", str(CSV_PATH)], cwd=str(PROJECT_ROOT),
                       capture_output=True, timeout=30)
        result = subprocess.run(
            ["git", "commit", "-m", f"Dashboard data update {datetime.now().strftime('%Y-%m-%d %H:%M')}"],
            cwd=str(PROJECT_ROOT), capture_output=True, text=True, timeout=30)
        if "nothing to commit" in result.stdout:
            logger.info("No changes to push")
            return
        subprocess.run(["git", "push"], cwd=str(PROJECT_ROOT),
                       capture_output=True, timeout=60)
        logger.info("Pushed to GitHub")
    except Exception as e:
        logger.error(f"Git push failed: {e}")


def export_fx_rate():
    """Fetch and save EUR/AUD rate."""
    try:
        import json, requests as req
        r = req.get('https://api.exchangerate-api.com/v4/latest/EUR', timeout=10)
        if r.status_code == 200:
            rate = r.json()['rates']['AUD']
            fx_file = DASHBOARD_DIR / "fx_rate.json"
            fx_file.write_text(json.dumps({
                "EUR_AUD": rate,
                "updated": datetime.now().isoformat()
            }))
            logger.info(f"EUR/AUD rate: {rate}")
    except Exception as e:
        logger.warning(f"FX rate fetch failed: {e}")


def export_sm_balance():
    """Fetch and save SM account balance info."""
    try:
        import requests
        from sportsmarket_api import get_session, SM_BASE, SM_USERNAME
        token = get_session()
        if not token:
            logger.warning("No SM session — skipping balance export")
            return
        r = requests.get(f"{SM_BASE}/../api/accounting_info",
                        headers={"Accept": "application/json", "session": token,
                                 "x-molly-client-name": "sonic"}, timeout=15)
        if r.status_code != 200:
            # Try alternate endpoint
            r = requests.get(f"https://pro.sportmarket.com/v1/accounting_info/",
                            headers={"Accept": "application/json", "session": token,
                                     "x-molly-client-name": "sonic"}, timeout=15)
        if r.status_code == 200:
            data = r.json()
            balance_file = DASHBOARD_DIR / "sm_balance.json"
            import json
            # Append timestamped entry
            history = []
            if balance_file.exists():
                try:
                    history = json.loads(balance_file.read_text())
                except: pass
            entry = {"timestamp": datetime.now().isoformat()}
            for item in data.get("data", []):
                entry[item["key"]] = item["value"]
            history.append(entry)
            # Keep last 365 entries
            history = history[-365:]
            balance_file.write_text(json.dumps(history, indent=2))
            logger.info(f"SM balance: {entry.get('current_balance', '?')}")
    except Exception as e:
        logger.warning(f"SM balance export failed: {e}")


def main():
    logger.info("Starting dashboard data export...")
    if export_csv():
        export_fx_rate()
        export_sm_balance()
        git_push()
    logger.info("Done")


if __name__ == "__main__":
    main()