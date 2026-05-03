"""
Wrapper that runs the optimizer and emails the results.

Designed to be triggered by launchd on the 15th of each month.

Runs both objectives (profit and ROI) so you can compare, and attaches
all four output files (CSV + MD for each).
"""

import logging
import os
import smtplib
import subprocess
import sys
from datetime import datetime
from email.message import EmailMessage
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

PYTHON = PROJECT_ROOT / "venv" / "bin" / "python"
OPTIMIZE_SCRIPT = PROJECT_ROOT / "optimizer" / "optimize.py"
WALKFORWARD_SCRIPT = PROJECT_ROOT / "optimizer" / "walk_forward.py"
RESULTS_DIR = PROJECT_ROOT / "optimizer" / "results"

LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_PATH = LOG_DIR / f"optimizer_{datetime.now().strftime('%Y-%m-%d')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(str(LOG_PATH)), logging.StreamHandler()],
)
logger = logging.getLogger("optimizer_runner")


def run_optimizer(objective: str, trials: int = 1000) -> list[Path]:
    """Run optimize.py for one objective and return paths to the new files."""
    logger.info(f"Running optimizer with --objective {objective} --trials {trials}")
    before = set(RESULTS_DIR.glob("optim_*"))

    result = subprocess.run(
        [str(PYTHON), str(OPTIMIZE_SCRIPT), "--trials", str(trials), "--objective", objective],
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        timeout=600,
    )
    if result.returncode != 0:
        logger.error(f"Optimizer failed: {result.stderr}")
        raise RuntimeError(f"Optimizer exited {result.returncode}")

    logger.info(result.stdout.split("Reports written:")[-1].strip())

    after = set(RESULTS_DIR.glob("optim_*"))
    new_files = sorted(after - before)
    return new_files


def run_walkforward(objective: str, windows: int = 4, window_days: int = 30, trials: int = 500) -> list[Path]:
    """Run walk_forward.py for one objective and return paths to the new files."""
    logger.info(f"Running walk-forward with --objective {objective} --windows {windows} --trials {trials}")
    before = set(RESULTS_DIR.glob("walkforward_*"))

    result = subprocess.run(
        [str(PYTHON), str(WALKFORWARD_SCRIPT),
         "--windows", str(windows),
         "--window-days", str(window_days),
         "--trials", str(trials),
         "--objective", objective],
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        timeout=1800,
    )
    if result.returncode != 0:
        logger.error(f"Walk-forward failed: {result.stderr}")
        raise RuntimeError(f"Walk-forward exited {result.returncode}")

    # Log the aggregate summary at the bottom
    summary_marker = "=== Aggregate Summary ==="
    if summary_marker in result.stdout:
        logger.info(summary_marker + result.stdout.split(summary_marker, 1)[1].strip())

    after = set(RESULTS_DIR.glob("walkforward_*"))
    new_files = sorted(after - before)
    return new_files


def email_results(
    profit_files: list[Path],
    roi_files: list[Path],
    wf_profit_files: list[Path],
    wf_roi_files: list[Path],
) -> None:
    email_user = os.environ.get("EMAIL_USER")
    email_pass = os.environ.get("EMAIL_PASS")
    email_to = [e.strip() for e in os.environ.get("EMAIL_TO", "").split(",") if e.strip()]

    if not email_user or not email_pass or not email_to:
        logger.warning("Email credentials missing — not sending")
        return

    today = datetime.now().strftime("%B %Y")
    msg = EmailMessage()
    msg["From"] = email_user
    msg["To"] = ", ".join(email_to)
    msg["Subject"] = f"Bet Tracker Optimizer Report - {today}"
    msg.set_content(
        f"Monthly optimizer run completed.\n\n"
        f"Four reports are attached:\n\n"
        f"1. optim_profit       - Single train/test, 1000 trials, maximizing total profit\n"
        f"2. optim_roi          - Single train/test, 1000 trials, maximizing ROI per bet\n"
        f"3. walkforward_profit - 4-window walk-forward analysis (profit objective)\n"
        f"4. walkforward_roi    - 4-window walk-forward analysis (ROI objective)\n\n"
        f"Each report includes a CSV (data) and Markdown (human-readable summary).\n\n"
        f"START WITH THE WALK-FORWARD REPORTS — they're the rigorous test.\n"
        f"Look at the aggregate table: which configs beat baseline on multiple windows?\n"
        f"A config that wins on 1 window could be lucky; one that wins on 3+ is more credible.\n\n"
        f"To apply a config (manual step):\n"
        f"  python optimizer/apply_params.py --config <csv_path> --rank <N>\n\n"
        f"To restore baseline:\n"
        f"  python optimizer/apply_params.py --config <csv_path> --rank baseline\n"
    )

    def attach(path: Path, label_prefix: str):
        name = f"{label_prefix}_{path.name}"
        with open(path, "rb") as f:
            data = f.read()
        if path.suffix == ".csv":
            msg.add_attachment(data, maintype="text", subtype="csv", filename=name)
        else:
            msg.add_attachment(data, maintype="text", subtype="markdown", filename=name)

    for p in profit_files:
        attach(p, "optim_profit")
    for p in roi_files:
        attach(p, "optim_roi")
    for p in wf_profit_files:
        attach(p, "walkforward_profit")
    for p in wf_roi_files:
        attach(p, "walkforward_roi")

    try:
        with smtplib.SMTP("smtp.mail.me.com", 587, timeout=15) as server:
            server.starttls()
            server.login(email_user, email_pass)
            server.send_message(msg)
        logger.info(f"Email sent to {email_to}")
    except Exception:
        logger.exception("Failed to send email")


def main():
    logger.info("=== Monthly optimizer run starting ===")
    try:
        profit_files = run_optimizer("profit", trials=1000)
        roi_files = run_optimizer("roi", trials=1000)
        wf_profit_files = run_walkforward("profit", windows=4, window_days=30, trials=500)
        wf_roi_files = run_walkforward("roi", windows=4, window_days=30, trials=500)
        email_results(profit_files, roi_files, wf_profit_files, wf_roi_files)
        logger.info("=== Done ===")
    except Exception:
        logger.exception("Optimizer run failed")
        sys.exit(1)


if __name__ == "__main__":
    main()