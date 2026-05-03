# Walk-Forward Analysis Report

**Generated:** 2026-04-15 08:01:37

- **Objective:** profit
- **Windows:** 4 × 30 days
- **Trials per window:** 500

## How to read this report

Each window optimizes on its training period (everything before the test window) and validates on the unseen test window. We compare top-N optimized configs against the current baseline parameters across all test windows. The key question: **which configs win on the test set consistently across multiple time periods?**

## Per-window results

| Window | Test period | Baseline (bets / profit / ROI) | Best optimized (bets / profit / ROI) | Winner |
|---|---|---|---|---|
| 1 | 2025-12-16 → 2026-01-14 | 51 / $-16.72 / -29.85% | 157 / $10.54 / 6.71% | Optimized |
| 2 | 2026-01-15 → 2026-02-13 | 92 / $-16.89 / -16.72% | 264 / $-30.51 / -11.56% | Baseline |
| 3 | 2026-02-14 → 2026-03-15 | 87 / $-40.10 / -42.66% | 212 / $-21.48 / -10.13% | Optimized |
| 4 | 2026-03-16 → 2026-04-14 | 41 / $-26.41 / -48.90% | 61 / $-0.67 / -1.10% | Optimized |

## Aggregate across all windows

Total test profit summed across all windows. **Mean / std dev** show the per-window distribution. **Windows positive** = how many windows had test profit > 0. **Beat baseline** = how many windows the rank-N config out-profited baseline.

| Config | Total Profit | Mean / window | Std dev | Mean ROI | Total Bets | Pos windows | Beat baseline |
|---|---:|---:|---:|---:|---:|---:|---:|
| **Baseline** | $-100.11 | $-25.03 | $11.02 | -34.53% | 271 | 0 / 4 | — |
| Rank 1 | $-42.12 | $-10.53 | $18.80 | -4.02% | 694 | 1 / 4 | 3 / 4 |
| Rank 2 | $-39.41 | $-9.85 | $18.58 | -0.86% | 690 | 2 / 4 | 3 / 4 |
| Rank 3 | $-38.96 | $-9.74 | $20.70 | -0.54% | 687 | 2 / 4 | 3 / 4 |
| Rank 4 | $-30.22 | $-7.55 | $15.73 | -1.15% | 673 | 2 / 4 | 3 / 4 |
| Rank 5 | $-43.57 | $-10.89 | $18.89 | -2.26% | 694 | 2 / 4 | 3 / 4 |

## Verdict

Rank 4 beats baseline on total test profit ($-30.22 vs $-100.11).

It also beat baseline in **3/4 windows**, with positive profit in **2/4 windows**.

**This is reasonably strong evidence** — the config wins on a majority of windows, not just one lucky one.

## Best optimized config from each window

### Window 1: train ≤ 2025-12-15

| Parameter | Value |
|---|---|
| vol_min | 0.000 |
| vol_max | 1425.000 |
| bf_min | 1.100 |
| bf_tier1 | 2.000 |
| bf_tier2 | 2.700 |
| rpd_low | 0.600 |
| rpd_mid | 1.200 |
| rpd_high | 7.900 |
| btts_fade_rpd | 7.300 |
| g15_fade_rpd | 3.000 |
| double_stake_rpd | 1.100 |
| double_stake_min_count | 3 |

### Window 2: train ≤ 2026-01-14

| Parameter | Value |
|---|---|
| vol_min | 0.000 |
| vol_max | 1450.000 |
| bf_min | 1.350 |
| bf_tier1 | 1.900 |
| bf_tier2 | 2.600 |
| rpd_low | 0.700 |
| rpd_mid | 1.000 |
| rpd_high | 5.400 |
| btts_fade_rpd | 6.200 |
| g15_fade_rpd | 3.200 |
| double_stake_rpd | 0.700 |
| double_stake_min_count | 2 |

### Window 3: train ≤ 2026-02-13

| Parameter | Value |
|---|---|
| vol_min | 5.000 |
| vol_max | 1750.000 |
| bf_min | 1.050 |
| bf_tier1 | 2.200 |
| bf_tier2 | 2.500 |
| rpd_low | 0.500 |
| rpd_mid | 1.800 |
| rpd_high | 5.500 |
| btts_fade_rpd | 6.500 |
| g15_fade_rpd | 3.700 |
| double_stake_rpd | 1.800 |
| double_stake_min_count | 3 |

### Window 4: train ≤ 2026-03-15

| Parameter | Value |
|---|---|
| vol_min | 0.000 |
| vol_max | 1975.000 |
| bf_min | 1.050 |
| bf_tier1 | 2.200 |
| bf_tier2 | 2.450 |
| rpd_low | 0.500 |
| rpd_mid | 1.900 |
| rpd_high | 6.500 |
| btts_fade_rpd | 7.600 |
| g15_fade_rpd | 4.300 |
| double_stake_rpd | 1.800 |
| double_stake_min_count | 2 |
