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
        rows.append({
            'Market': bt,
            'Date': d,
            'Home': ws.cell(row=r, column=3).value,
            'Away': ws.cell(row=r, column=4).value,
            'Competition': ws.cell(row=r, column=5).value,
            'Prediction': ws.cell(row=r, column=8).value,
            'Bet365': ws.cell(row=r, column=9).value,
            'BF': ws.cell(row=r, column=10).value,
            'Volume': ws.cell(row=r, column=11).value,
            'RPD': ws.cell(row=r, column=12).value,
            'Goals': ws.cell(row=r, column=13).value,
            'HG': ws.cell(row=r, column=14).value,
            'AG': ws.cell(row=r, column=15).value,
            'Result': ws.cell(row=r, column=16).value,
            'Stake': ws.cell(row=r, column=17).value,
            'Return': ws.cell(row=r, column=18).value,
            'Profit': ws.cell(row=r, column=19).value,
            'SM_Odds': ws.cell(row=r, column=22).value,
        })
    wb.close()

    df = pd.DataFrame(rows)
    DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(CSV_PATH, index=False)
    logger.info(f"Exported {len(df)} rows to {CSV_PATH}")
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


def main():
    logger.info("Starting dashboard data export...")
    if export_csv():
        git_push()
    logger.info("Done")


if __name__ == "__main__":
    main()