"""
Centralised path constants for the BookieGrabber pipeline.

All modules should import from here instead of defining their own
PROJECT_ROOT, MASTER_PATH, etc.
"""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
MASTER_PATH = Path.home() / "Desktop" / "EFB_Master_Bet_Tracker_VS Code.xlsx"
MASTER_SHEET = "Master Bet Tracker"
LOG_DIR = PROJECT_ROOT / "logs"
DATA_DIR = PROJECT_ROOT / "data"
EXPORT_DIR = DATA_DIR / "exports"
DASHBOARD_DIR = PROJECT_ROOT / "dashboard_data"
STATE_DIR = DATA_DIR / "state"
