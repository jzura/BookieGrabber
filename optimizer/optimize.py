"""
Bayesian optimizer for the bet tracker strategy.

Uses Optuna to search for parameter configurations that maximize profit
on the training set, subject to a minimum bet count constraint.

Holds out the last 30 days as an unseen test set for validation.

Usage:
    python optimizer/optimize.py                    # Default: 500 trials
    python optimizer/optimize.py --trials 1000      # More trials = better search
    python optimizer/optimize.py --holdout-days 60  # Custom holdout window
    python optimizer/optimize.py --min-bets 50      # Lower min bet constraint
"""

import argparse
import csv
import json
import sys
from datetime import datetime, date, timedelta
from pathlib import Path

import optuna
from optuna.trial import Trial

# Make sibling import work whether run from project root or optimizer/
sys.path.insert(0, str(Path(__file__).resolve().parent))
from backtest import (
    StrategyParams,
    BacktestStats,
    load_historical_bets,
    run_backtest,
)


RESULTS_DIR = Path(__file__).resolve().parent / "results"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)


# -------------------------------------------------------------
# Search space — what Optuna can tune
# -------------------------------------------------------------

def suggest_params(trial: Trial) -> StrategyParams:
    """Define the parameter search space."""
    p = StrategyParams()

    # Volume range
    p.vol_min = trial.suggest_float("vol_min", 0, 200, step=5)
    p.vol_max = trial.suggest_float("vol_max", 300, 2000, step=25)

    # BF lower bound
    p.bf_min = trial.suggest_float("bf_min", 1.0, 1.6, step=0.05)

    # BF tier breakpoints
    p.bf_tier1 = trial.suggest_float("bf_tier1", 1.5, 2.2, step=0.05)
    p.bf_tier2 = trial.suggest_float("bf_tier2", 2.0, 3.5, step=0.05)

    # Ensure tier1 < tier2
    if p.bf_tier1 >= p.bf_tier2:
        raise optuna.exceptions.TrialPruned()

    # RPD thresholds per tier
    p.rpd_low = trial.suggest_float("rpd_low", 0.5, 3.0, step=0.1)
    p.rpd_mid = trial.suggest_float("rpd_mid", 1.0, 5.0, step=0.1)
    p.rpd_high = trial.suggest_float("rpd_high", 2.0, 8.0, step=0.1)

    # Per-market fade thresholds
    p.btts_fade_rpd = trial.suggest_float("btts_fade_rpd", 3.0, 8.0, step=0.1)
    p.g15_fade_rpd = trial.suggest_float("g15_fade_rpd", 3.0, 8.0, step=0.1)

    # Double-stake settings
    p.double_stake_rpd = trial.suggest_float("double_stake_rpd", 0.5, 2.0, step=0.1)
    p.double_stake_min_count = trial.suggest_int("double_stake_min_count", 2, 4)

    return p


# -------------------------------------------------------------
# Objective function
# -------------------------------------------------------------

def make_objective(bets, train_end: date, min_bets: int, metric: str):
    """Create the Optuna objective function.
    metric: 'profit' or 'roi'"""

    def objective(trial: Trial) -> float:
        params = suggest_params(trial)

        stats = run_backtest(bets, params, date_to=train_end)

        # Constraint: minimum bet count
        if stats.n_bets < min_bets:
            return -1e9 + stats.n_bets  # still has gradient

        # Track metadata
        trial.set_user_attr("n_bets", stats.n_bets)
        trial.set_user_attr("total_staked", stats.total_staked)
        trial.set_user_attr("total_profit", stats.total_profit)
        trial.set_user_attr("roi_pct", stats.roi)
        trial.set_user_attr("win_rate_pct", stats.win_rate)
        trial.set_user_attr("by_market", stats.by_market)

        if metric == "roi":
            return stats.roi
        return stats.total_profit

    return objective


# -------------------------------------------------------------
# Reporting
# -------------------------------------------------------------

def baseline_stats(bets, train_end: date, test_start: date, test_end: date):
    """Run the current default params on train and test for comparison."""
    default = StrategyParams()
    train = run_backtest(bets, default, date_to=train_end)
    test = run_backtest(bets, default, date_from=test_start, date_to=test_end)
    return default, train, test


def write_csv_report(study: optuna.Study, baseline_train, baseline_test, top_n: int, csv_path: Path):
    """Write top-N trials to CSV."""
    trials = sorted(
        [t for t in study.trials if t.value is not None and t.value > -1e8],
        key=lambda t: t.value,
        reverse=True,
    )[:top_n]

    fieldnames = [
        "rank", "train_profit", "train_roi_pct", "train_n_bets", "train_win_rate_pct",
    ] + list(StrategyParams().to_dict().keys())

    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        # Baseline
        writer.writerow({
            "rank": "baseline",
            "train_profit": round(baseline_train.total_profit, 2),
            "train_roi_pct": round(baseline_train.roi, 2),
            "train_n_bets": baseline_train.n_bets,
            "train_win_rate_pct": round(baseline_train.win_rate, 2),
            **StrategyParams().to_dict(),
        })

        for rank, t in enumerate(trials, 1):
            writer.writerow({
                "rank": rank,
                "train_profit": round(t.user_attrs.get("total_profit", 0), 2),
                "train_roi_pct": round(t.user_attrs.get("roi_pct", 0), 2),
                "train_n_bets": t.user_attrs.get("n_bets", 0),
                "train_win_rate_pct": round(t.user_attrs.get("win_rate_pct", 0), 2),
                **t.params,
            })


def write_markdown_report(
    study: optuna.Study,
    bets,
    baseline_train,
    baseline_test,
    test_start: date,
    test_end: date,
    train_end: date,
    top_n: int,
    md_path: Path,
):
    """Write a human-readable markdown report."""
    trials = sorted(
        [t for t in study.trials if t.value is not None and t.value > -1e8],
        key=lambda t: t.value,
        reverse=True,
    )[:top_n]

    lines = []
    lines.append("# Bet Tracker Optimization Report")
    lines.append("")
    lines.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")
    lines.append(f"- **Training period:** up to {train_end} ({sum(1 for b in bets if b.date <= train_end)} rows)")
    lines.append(f"- **Test period (holdout):** {test_start} to {test_end}")
    lines.append(f"- **Total trials run:** {len(study.trials)}")
    lines.append("")

    # Baseline
    lines.append("## Current parameters (baseline)")
    lines.append("")
    lines.append("| Metric | Training set | Test set |")
    lines.append("|---|---|---|")
    lines.append(f"| Bets | {baseline_train.n_bets} | {baseline_test.n_bets} |")
    lines.append(f"| Total staked | ${baseline_train.total_staked:.2f} | ${baseline_test.total_staked:.2f} |")
    lines.append(f"| Profit | ${baseline_train.total_profit:.2f} | ${baseline_test.total_profit:.2f} |")
    lines.append(f"| ROI | {baseline_train.roi:.2f}% | {baseline_test.roi:.2f}% |")
    lines.append(f"| Win rate | {baseline_train.win_rate:.2f}% | {baseline_test.win_rate:.2f}% |")
    lines.append("")

    if not trials:
        lines.append("**No trials passed the minimum bet count constraint.**")
        md_path.write_text("\n".join(lines))
        return

    # Top configs comparison table
    lines.append(f"## Top {min(top_n, len(trials))} configurations")
    lines.append("")
    lines.append("Each config was optimized on the training set, then validated on the unseen test set.")
    lines.append("")
    lines.append("| Rank | Train Profit | Train ROI | Train Bets | Test Profit | Test ROI | Test Bets |")
    lines.append("|---:|---:|---:|---:|---:|---:|---:|")

    # Baseline row
    lines.append(
        f"| baseline | ${baseline_train.total_profit:.2f} | {baseline_train.roi:.2f}% | "
        f"{baseline_train.n_bets} | ${baseline_test.total_profit:.2f} | "
        f"{baseline_test.roi:.2f}% | {baseline_test.n_bets} |"
    )

    # Test the top trials on the holdout set
    for rank, t in enumerate(trials, 1):
        params = StrategyParams(**t.params)
        test_stats = run_backtest(bets, params, date_from=test_start, date_to=test_end)
        t.set_user_attr("test_profit", test_stats.total_profit)
        t.set_user_attr("test_roi", test_stats.roi)
        t.set_user_attr("test_n_bets", test_stats.n_bets)

        lines.append(
            f"| {rank} | ${t.user_attrs['total_profit']:.2f} | {t.user_attrs['roi_pct']:.2f}% | "
            f"{t.user_attrs['n_bets']} | ${test_stats.total_profit:.2f} | "
            f"{test_stats.roi:.2f}% | {test_stats.n_bets} |"
        )

    lines.append("")
    lines.append("## Top configurations — full parameters")
    lines.append("")

    # Detailed param dump for top 5
    for rank, t in enumerate(trials[:5], 1):
        lines.append(f"### Rank {rank}")
        lines.append("")
        lines.append("| Parameter | Value |")
        lines.append("|---|---|")
        for k, v in t.params.items():
            if isinstance(v, float):
                lines.append(f"| {k} | {v:.3f} |")
            else:
                lines.append(f"| {k} | {v} |")
        lines.append("")
        lines.append(f"**Train:** {t.user_attrs['n_bets']} bets, ${t.user_attrs['total_profit']:.2f} profit, "
                     f"{t.user_attrs['roi_pct']:.2f}% ROI")
        lines.append(f"**Test:**  {t.user_attrs.get('test_n_bets', 0)} bets, ${t.user_attrs.get('test_profit', 0):.2f} profit, "
                     f"{t.user_attrs.get('test_roi', 0):.2f}% ROI")
        lines.append("")

    # Param importances
    try:
        importances = optuna.importance.get_param_importances(study)
        lines.append("## Parameter importance")
        lines.append("")
        lines.append("Which parameters had the biggest impact on the objective?")
        lines.append("")
        lines.append("| Parameter | Importance |")
        lines.append("|---|---:|")
        for k, v in importances.items():
            lines.append(f"| {k} | {v:.3f} |")
        lines.append("")
    except Exception:
        pass

    # How to apply
    lines.append("## How to apply a config")
    lines.append("")
    lines.append("1. Pick a config from the table above. **Read the test column carefully** — high train/low test = overfitting.")
    lines.append("2. Run the apply script with the rank number:")
    lines.append("")
    lines.append("```bash")
    lines.append("python optimizer/apply_params.py --config optimizer/results/<csv_filename> --rank N")
    lines.append("```")
    lines.append("")
    lines.append("3. The script will preview the param changes, ask for confirmation, then rewrite stake formulas in the master sheet.")
    lines.append("")

    md_path.write_text("\n".join(lines))


# -------------------------------------------------------------
# CLI
# -------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Bayesian optimizer for bet tracker strategy")
    parser.add_argument("--trials", type=int, default=500, help="Number of optimization trials (default: 500)")
    parser.add_argument("--holdout-days", type=int, default=30, help="Days to hold out as test set (default: 30)")
    parser.add_argument("--min-bets", type=int, default=100, help="Min bets in train set for valid config (default: 100)")
    parser.add_argument("--top-n", type=int, default=20, help="Top N configs to report (default: 20)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed (default: 42)")
    parser.add_argument("--objective", choices=["profit", "roi"], default="profit",
                        help="What to maximize (default: profit)")
    args = parser.parse_args()

    print("Loading historical bets from master spreadsheet...")
    bets = load_historical_bets()
    print(f"Loaded {len(bets)} rows")

    settled = [b for b in bets if b.result is not None]
    print(f"Settled rows: {len(settled)}")

    if not settled:
        print("ERROR: No settled bets found.")
        return

    # Determine train/test split
    max_date = max(b.date for b in settled)
    test_start = max_date - timedelta(days=args.holdout_days - 1)
    train_end = test_start - timedelta(days=1)

    n_train = sum(1 for b in settled if b.date <= train_end)
    n_test = sum(1 for b in settled if test_start <= b.date <= max_date)

    print(f"\nData split:")
    print(f"  Training: ... {train_end} ({n_train} settled rows)")
    print(f"  Test:     {test_start} ... {max_date} ({n_test} settled rows)")

    # Baseline
    print("\nRunning baseline (current default params)...")
    default_params, baseline_train, baseline_test = baseline_stats(bets, train_end, test_start, max_date)
    print(f"  Train: {baseline_train.n_bets} bets, ${baseline_train.total_profit:.2f} profit, {baseline_train.roi:.2f}% ROI")
    print(f"  Test:  {baseline_test.n_bets} bets, ${baseline_test.total_profit:.2f} profit, {baseline_test.roi:.2f}% ROI")

    # Run optimization
    print(f"\nRunning Optuna with {args.trials} trials (objective: {args.objective}, min_bets: {args.min_bets})...")
    optuna.logging.set_verbosity(optuna.logging.WARNING)
    sampler = optuna.samplers.TPESampler(seed=args.seed)
    study = optuna.create_study(direction="maximize", sampler=sampler)
    objective = make_objective(bets, train_end, args.min_bets, args.objective)
    study.optimize(objective, n_trials=args.trials, show_progress_bar=True)

    if args.objective == "roi":
        print(f"\nBest trial ROI: {study.best_value:.2f}%")
    else:
        print(f"\nBest trial profit: ${study.best_value:.2f}")
    print(f"Best trial bets:   {study.best_trial.user_attrs.get('n_bets', '?')}")

    # Write reports
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = RESULTS_DIR / f"optim_{timestamp}.csv"
    md_path = RESULTS_DIR / f"optim_{timestamp}.md"

    write_csv_report(study, baseline_train, baseline_test, args.top_n, csv_path)
    write_markdown_report(
        study, bets, baseline_train, baseline_test,
        test_start, max_date, train_end, args.top_n, md_path,
    )

    print(f"\nReports written:")
    print(f"  {csv_path}")
    print(f"  {md_path}")


if __name__ == "__main__":
    main()