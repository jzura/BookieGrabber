"""
Walk-forward analysis for the bet tracker strategy.

Instead of one train/test split, this slides a window through history and
runs the optimizer multiple times. For each window we get:
  - Best optimized config on training
  - That config's performance on the held-out test window
  - Baseline performance on the same test window for comparison

This gives multiple independent tests of whether an "optimized" config
genuinely beats baseline, rather than relying on a single 30-day window.

Usage:
    python optimizer/walk_forward.py
    python optimizer/walk_forward.py --windows 6 --window-days 30 --trials 500
    python optimizer/walk_forward.py --objective roi
    python optimizer/walk_forward.py --candidate-trials 5  # how many top configs to evaluate per window
"""

import argparse
import csv
import sys
from datetime import datetime, date, timedelta
from pathlib import Path
from collections import defaultdict
from statistics import mean, stdev

import optuna

sys.path.insert(0, str(Path(__file__).resolve().parent))
from backtest import StrategyParams, load_historical_bets, run_backtest
from optimize import suggest_params  # reuse the same search space

RESULTS_DIR = Path(__file__).resolve().parent / "results"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)


# -------------------------------------------------------------
# One window's optimization
# -------------------------------------------------------------

def optimize_window(bets, train_end: date, trials: int, min_bets: int,
                    objective_metric: str, seed: int = 42) -> optuna.Study:
    """Run Optuna for a single window's training period."""

    def objective(trial):
        params = suggest_params(trial)
        stats = run_backtest(bets, params, date_to=train_end)
        if stats.n_bets < min_bets:
            return -1e9 + stats.n_bets
        trial.set_user_attr("n_bets", stats.n_bets)
        trial.set_user_attr("total_profit", stats.total_profit)
        trial.set_user_attr("roi_pct", stats.roi)
        if objective_metric == "roi":
            return stats.roi
        return stats.total_profit

    optuna.logging.set_verbosity(optuna.logging.WARNING)
    sampler = optuna.samplers.TPESampler(seed=seed)
    study = optuna.create_study(direction="maximize", sampler=sampler)
    study.optimize(objective, n_trials=trials, show_progress_bar=False)
    return study


# -------------------------------------------------------------
# Walk-forward driver
# -------------------------------------------------------------

def walk_forward(bets, n_windows: int, window_days: int, trials: int,
                 min_bets: int, objective_metric: str, candidate_trials: int) -> dict:
    """
    Slide n_windows test windows through the data.
    Each window has window_days of test data, with everything before it as training.

    Returns dict with results per window.
    """
    # Find date range from settled bets
    settled = [b for b in bets if b.result is not None]
    max_date = max(b.date for b in settled)
    min_date = min(b.date for b in settled)

    # Build window list
    windows = []
    test_end = max_date
    for _ in range(n_windows):
        test_start = test_end - timedelta(days=window_days - 1)
        train_end = test_start - timedelta(days=1)
        if train_end - timedelta(days=60) < min_date:
            break  # not enough training data
        windows.append((train_end, test_start, test_end))
        test_end = test_start - timedelta(days=1)

    windows.reverse()  # chronological order

    print(f"\nWalk-forward windows ({len(windows)} windows of {window_days} days each):")
    for i, (te, ts, tend) in enumerate(windows, 1):
        print(f"  Window {i}: train ≤ {te} | test {ts} → {tend}")

    results = {
        "windows": [],
        "objective_metric": objective_metric,
        "n_windows": len(windows),
        "window_days": window_days,
        "trials_per_window": trials,
    }

    baseline_params = StrategyParams()

    for i, (train_end, test_start, test_end) in enumerate(windows, 1):
        print(f"\n=== Window {i}/{len(windows)} ===")
        print(f"  Optimizing on training (up to {train_end})...")

        study = optimize_window(bets, train_end, trials, min_bets, objective_metric)

        # Get top N candidates from training
        valid_trials = sorted(
            [t for t in study.trials if t.value is not None and t.value > -1e8],
            key=lambda t: t.value,
            reverse=True,
        )[:candidate_trials]

        # Baseline on test
        baseline_test = run_backtest(bets, baseline_params, date_from=test_start, date_to=test_end)

        candidates = []
        for rank, t in enumerate(valid_trials, 1):
            params = StrategyParams(**t.params)
            test_stats = run_backtest(bets, params, date_from=test_start, date_to=test_end)
            candidates.append({
                "rank": rank,
                "train_profit": t.user_attrs.get("total_profit", 0),
                "train_roi": t.user_attrs.get("roi_pct", 0),
                "train_bets": t.user_attrs.get("n_bets", 0),
                "test_profit": test_stats.total_profit,
                "test_roi": test_stats.roi,
                "test_bets": test_stats.n_bets,
                "params": t.params,
            })

        window_result = {
            "window_num": i,
            "train_end": str(train_end),
            "test_start": str(test_start),
            "test_end": str(test_end),
            "baseline_test_profit": baseline_test.total_profit,
            "baseline_test_roi": baseline_test.roi,
            "baseline_test_bets": baseline_test.n_bets,
            "candidates": candidates,
        }
        results["windows"].append(window_result)

        # Print summary for this window
        print(f"  Baseline:  {baseline_test.n_bets} bets, ${baseline_test.total_profit:.2f} profit, "
              f"{baseline_test.roi:.2f}% ROI")
        for c in candidates[:3]:
            print(f"  Rank {c['rank']}:    {c['test_bets']} bets, ${c['test_profit']:.2f} profit, "
                  f"{c['test_roi']:.2f}% ROI")

    return results


# -------------------------------------------------------------
# Aggregate analysis
# -------------------------------------------------------------

def aggregate_results(results: dict) -> dict:
    """Compute summary stats across all windows."""
    n_windows = len(results["windows"])
    if n_windows == 0:
        return {}

    # Baseline aggregate
    baseline_profits = [w["baseline_test_profit"] for w in results["windows"]]
    baseline_rois = [w["baseline_test_roi"] for w in results["windows"]]
    baseline_bets = [w["baseline_test_bets"] for w in results["windows"]]

    # Per-rank aggregate (across windows)
    rank_stats = defaultdict(lambda: {"profits": [], "rois": [], "bets": [], "wins": 0})
    for w in results["windows"]:
        for c in w["candidates"]:
            r = c["rank"]
            rank_stats[r]["profits"].append(c["test_profit"])
            rank_stats[r]["rois"].append(c["test_roi"])
            rank_stats[r]["bets"].append(c["test_bets"])
            if c["test_profit"] > w["baseline_test_profit"]:
                rank_stats[r]["wins"] += 1

    aggregated = {
        "baseline": {
            "total_test_profit": sum(baseline_profits),
            "mean_test_profit": mean(baseline_profits),
            "stdev_test_profit": stdev(baseline_profits) if n_windows > 1 else 0.0,
            "mean_test_roi": mean(baseline_rois),
            "total_test_bets": sum(baseline_bets),
            "windows_positive": sum(1 for p in baseline_profits if p > 0),
        },
        "ranks": {},
    }
    for r, stats in sorted(rank_stats.items()):
        aggregated["ranks"][r] = {
            "total_test_profit": sum(stats["profits"]),
            "mean_test_profit": mean(stats["profits"]),
            "stdev_test_profit": stdev(stats["profits"]) if len(stats["profits"]) > 1 else 0.0,
            "mean_test_roi": mean(stats["rois"]),
            "total_test_bets": sum(stats["bets"]),
            "windows_positive": sum(1 for p in stats["profits"] if p > 0),
            "windows_beat_baseline": stats["wins"],
            "n_windows": len(stats["profits"]),
        }
    return aggregated


# -------------------------------------------------------------
# Reporting
# -------------------------------------------------------------

def write_report(results: dict, aggregated: dict, output_path: Path):
    """Write a human-readable markdown report."""
    n_windows = results["n_windows"]
    lines = []
    lines.append("# Walk-Forward Analysis Report")
    lines.append("")
    lines.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")
    lines.append(f"- **Objective:** {results['objective_metric']}")
    lines.append(f"- **Windows:** {n_windows} × {results['window_days']} days")
    lines.append(f"- **Trials per window:** {results['trials_per_window']}")
    lines.append("")

    lines.append("## How to read this report")
    lines.append("")
    lines.append(
        "Each window optimizes on its training period (everything before the test window) "
        "and validates on the unseen test window. We compare top-N optimized configs against "
        "the current baseline parameters across all test windows. The key question: **which "
        "configs win on the test set consistently across multiple time periods?**"
    )
    lines.append("")

    # Per-window detail table
    lines.append("## Per-window results")
    lines.append("")
    lines.append("| Window | Test period | Baseline (bets / profit / ROI) | "
                 "Best optimized (bets / profit / ROI) | Winner |")
    lines.append("|---|---|---|---|---|")
    for w in results["windows"]:
        bn_str = f"{w['baseline_test_bets']} / ${w['baseline_test_profit']:.2f} / {w['baseline_test_roi']:.2f}%"
        if w["candidates"]:
            best = w["candidates"][0]  # rank 1
            opt_str = f"{best['test_bets']} / ${best['test_profit']:.2f} / {best['test_roi']:.2f}%"
            winner = "Optimized" if best["test_profit"] > w["baseline_test_profit"] else "Baseline"
        else:
            opt_str = "—"
            winner = "Baseline"
        lines.append(f"| {w['window_num']} | {w['test_start']} → {w['test_end']} | {bn_str} | {opt_str} | {winner} |")
    lines.append("")

    # Aggregate table
    lines.append("## Aggregate across all windows")
    lines.append("")
    lines.append(
        "Total test profit summed across all windows. **Mean / std dev** show "
        "the per-window distribution. **Windows positive** = how many windows had test profit > 0. "
        "**Beat baseline** = how many windows the rank-N config out-profited baseline."
    )
    lines.append("")
    lines.append("| Config | Total Profit | Mean / window | Std dev | Mean ROI | Total Bets | Pos windows | Beat baseline |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|")

    bn = aggregated["baseline"]
    lines.append(
        f"| **Baseline** | ${bn['total_test_profit']:.2f} | ${bn['mean_test_profit']:.2f} | "
        f"${bn['stdev_test_profit']:.2f} | {bn['mean_test_roi']:.2f}% | {bn['total_test_bets']} | "
        f"{bn['windows_positive']} / {n_windows} | — |"
    )

    for rank, stats in aggregated["ranks"].items():
        lines.append(
            f"| Rank {rank} | ${stats['total_test_profit']:.2f} | ${stats['mean_test_profit']:.2f} | "
            f"${stats['stdev_test_profit']:.2f} | {stats['mean_test_roi']:.2f}% | "
            f"{stats['total_test_bets']} | {stats['windows_positive']} / {n_windows} | "
            f"{stats['windows_beat_baseline']} / {n_windows} |"
        )
    lines.append("")

    # Verdict
    lines.append("## Verdict")
    lines.append("")
    bn_total = bn["total_test_profit"]
    best_rank_profit = max((s["total_test_profit"] for s in aggregated["ranks"].values()), default=0)
    best_rank_n = max(aggregated["ranks"].items(),
                      key=lambda kv: kv[1]["total_test_profit"], default=(None, None))

    if best_rank_profit > bn_total:
        rank_no, rank_stats = best_rank_n
        lines.append(
            f"Rank {rank_no} beats baseline on total test profit "
            f"(${best_rank_profit:.2f} vs ${bn_total:.2f})."
        )
        lines.append("")
        lines.append(
            f"It also beat baseline in **{rank_stats['windows_beat_baseline']}/{n_windows} windows**, "
            f"with positive profit in **{rank_stats['windows_positive']}/{n_windows} windows**."
        )
        if rank_stats["windows_beat_baseline"] >= n_windows * 0.6:
            lines.append("")
            lines.append("**This is reasonably strong evidence** — the config wins on a majority of windows, "
                         "not just one lucky one.")
        else:
            lines.append("")
            lines.append("**Caution:** while the total profit is higher, it doesn't beat baseline consistently. "
                         "Could be carried by a single lucky window.")
    else:
        lines.append(
            f"**Baseline wins.** Total test profit ${bn_total:.2f} beats every optimized config. "
            f"Don't change anything based on this data."
        )
    lines.append("")

    # Best params dump
    lines.append("## Best optimized config from each window")
    lines.append("")
    for w in results["windows"]:
        if not w["candidates"]:
            continue
        c = w["candidates"][0]
        lines.append(f"### Window {w['window_num']}: train ≤ {w['train_end']}")
        lines.append("")
        lines.append("| Parameter | Value |")
        lines.append("|---|---|")
        for k, v in c["params"].items():
            if isinstance(v, float):
                lines.append(f"| {k} | {v:.3f} |")
            else:
                lines.append(f"| {k} | {v} |")
        lines.append("")

    output_path.write_text("\n".join(lines))


def write_csv(results: dict, output_path: Path):
    """Per-window per-rank CSV for further analysis."""
    fieldnames = [
        "window", "train_end", "test_start", "test_end", "config",
        "test_bets", "test_profit", "test_roi",
        "train_bets", "train_profit", "train_roi",
    ] + list(StrategyParams().to_dict().keys())

    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for w in results["windows"]:
            # Baseline row
            writer.writerow({
                "window": w["window_num"],
                "train_end": w["train_end"],
                "test_start": w["test_start"],
                "test_end": w["test_end"],
                "config": "baseline",
                "test_bets": w["baseline_test_bets"],
                "test_profit": round(w["baseline_test_profit"], 2),
                "test_roi": round(w["baseline_test_roi"], 2),
                **{k: "" for k in ["train_bets", "train_profit", "train_roi"]},
                **StrategyParams().to_dict(),
            })
            for c in w["candidates"]:
                row = {
                    "window": w["window_num"],
                    "train_end": w["train_end"],
                    "test_start": w["test_start"],
                    "test_end": w["test_end"],
                    "config": f"rank_{c['rank']}",
                    "test_bets": c["test_bets"],
                    "test_profit": round(c["test_profit"], 2),
                    "test_roi": round(c["test_roi"], 2),
                    "train_bets": c["train_bets"],
                    "train_profit": round(c["train_profit"], 2),
                    "train_roi": round(c["train_roi"], 2),
                }
                row.update(c["params"])
                writer.writerow(row)


# -------------------------------------------------------------
# CLI
# -------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Walk-forward analysis for bet tracker optimizer")
    parser.add_argument("--windows", type=int, default=4, help="Number of test windows (default: 4)")
    parser.add_argument("--window-days", type=int, default=30, help="Days per test window (default: 30)")
    parser.add_argument("--trials", type=int, default=500, help="Trials per window (default: 500)")
    parser.add_argument("--min-bets", type=int, default=100, help="Min bets in train (default: 100)")
    parser.add_argument("--objective", choices=["profit", "roi"], default="profit",
                        help="Objective metric (default: profit)")
    parser.add_argument("--candidate-trials", type=int, default=5,
                        help="Top N configs per window to validate on test (default: 5)")
    args = parser.parse_args()

    print("Loading historical bets from master spreadsheet...")
    bets = load_historical_bets()
    print(f"Loaded {len(bets)} rows ({sum(1 for b in bets if b.result is not None)} settled)")

    results = walk_forward(
        bets,
        n_windows=args.windows,
        window_days=args.window_days,
        trials=args.trials,
        min_bets=args.min_bets,
        objective_metric=args.objective,
        candidate_trials=args.candidate_trials,
    )

    aggregated = aggregate_results(results)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    md_path = RESULTS_DIR / f"walkforward_{args.objective}_{timestamp}.md"
    csv_path = RESULTS_DIR / f"walkforward_{args.objective}_{timestamp}.csv"

    write_report(results, aggregated, md_path)
    write_csv(results, csv_path)

    print(f"\nReports written:")
    print(f"  {md_path}")
    print(f"  {csv_path}")

    # Quick verdict
    print("\n=== Aggregate Summary ===")
    bn = aggregated["baseline"]
    print(f"Baseline: ${bn['total_test_profit']:.2f} total, "
          f"{bn['mean_test_roi']:.2f}% mean ROI, "
          f"{bn['windows_positive']}/{results['n_windows']} positive windows")
    for rank, stats in aggregated["ranks"].items():
        marker = " ★" if stats["total_test_profit"] > bn["total_test_profit"] else ""
        print(f"Rank {rank}:  ${stats['total_test_profit']:.2f} total, "
              f"{stats['mean_test_roi']:.2f}% mean ROI, "
              f"{stats['windows_positive']}/{results['n_windows']} positive, "
              f"{stats['windows_beat_baseline']}/{results['n_windows']} beat baseline{marker}")


if __name__ == "__main__":
    main()