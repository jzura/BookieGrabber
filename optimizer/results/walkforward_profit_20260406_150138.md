# Walk-Forward Analysis Report

**Generated:** 2026-04-06 15:01:38

- **Objective:** profit
- **Windows:** 4 × 30 days
- **Trials per window:** 500

## How to read this report

Each window optimizes on its training period (everything before the test window) and validates on the unseen test window. We compare top-N optimized configs against the current baseline parameters across all test windows. The key question: **which configs win on the test set consistently across multiple time periods?**

## Per-window results

| Window | Test period | Baseline (bets / profit / ROI) | Best optimized (bets / profit / ROI) | Winner |
|---|---|---|---|---|
| 1 | 2025-12-06 → 2026-01-04 | 125 / $65.62 / 47.90% | 474 / $39.16 / 8.12% | Baseline |
| 2 | 2026-01-05 → 2026-02-03 | 98 / $-17.08 / -15.82% | 361 / $-17.76 / -4.55% | Baseline |
| 3 | 2026-02-04 → 2026-03-05 | 108 / $15.83 / 13.89% | 277 / $-8.23 / -2.97% | Baseline |
| 4 | 2026-03-06 → 2026-04-04 | 73 / $20.08 / 24.79% | 326 / $-1.87 / -0.54% | Baseline |

## Aggregate across all windows

Total test profit summed across all windows. **Mean / std dev** show the per-window distribution. **Windows positive** = how many windows had test profit > 0. **Beat baseline** = how many windows the rank-N config out-profited baseline.

| Config | Total Profit | Mean / window | Std dev | Mean ROI | Total Bets | Pos windows | Beat baseline |
|---|---:|---:|---:|---:|---:|---:|---:|
| **Baseline** | $84.45 | $21.11 | $34.00 | 17.69% | 404 | 3 / 4 | — |
| Rank 1 | $11.29 | $2.82 | $25.09 | 0.01% | 1438 | 1 / 4 | 0 / 4 |
| Rank 2 | $28.78 | $7.20 | $43.00 | 1.03% | 1438 | 1 / 4 | 1 / 4 |
| Rank 3 | $28.83 | $7.21 | $43.72 | 0.68% | 1470 | 1 / 4 | 1 / 4 |
| Rank 4 | $26.76 | $6.69 | $23.53 | 1.15% | 1409 | 2 / 4 | 1 / 4 |
| Rank 5 | $-12.18 | $-3.04 | $29.72 | -1.28% | 1490 | 1 / 4 | 0 / 4 |

## Verdict

**Baseline wins.** Total test profit $84.45 beats every optimized config. Don't change anything based on this data.

## Best optimized config from each window

### Window 1: train ≤ 2025-12-05

| Parameter | Value |
|---|---|
| vol_min | 50.000 |
| vol_max | 1725.000 |
| bf_min | 1.300 |
| bf_tier1 | 1.600 |
| bf_tier2 | 2.450 |
| rpd_low | 2.600 |
| rpd_mid | 4.200 |
| rpd_high | 6.400 |
| btts_fade_rpd | 3.600 |
| g15_fade_rpd | 4.100 |
| double_stake_rpd | 1.000 |
| double_stake_min_count | 3 |

### Window 2: train ≤ 2026-01-04

| Parameter | Value |
|---|---|
| vol_min | 55.000 |
| vol_max | 1600.000 |
| bf_min | 1.250 |
| bf_tier1 | 1.750 |
| bf_tier2 | 2.450 |
| rpd_low | 2.900 |
| rpd_mid | 3.500 |
| rpd_high | 7.600 |
| btts_fade_rpd | 4.200 |
| g15_fade_rpd | 6.800 |
| double_stake_rpd | 1.000 |
| double_stake_min_count | 2 |

### Window 3: train ≤ 2026-02-03

| Parameter | Value |
|---|---|
| vol_min | 45.000 |
| vol_max | 775.000 |
| bf_min | 1.300 |
| bf_tier1 | 1.650 |
| bf_tier2 | 2.400 |
| rpd_low | 2.800 |
| rpd_mid | 4.100 |
| rpd_high | 3.300 |
| btts_fade_rpd | 4.200 |
| g15_fade_rpd | 4.200 |
| double_stake_rpd | 1.300 |
| double_stake_min_count | 3 |

### Window 4: train ≤ 2026-03-05

| Parameter | Value |
|---|---|
| vol_min | 40.000 |
| vol_max | 1275.000 |
| bf_min | 1.000 |
| bf_tier1 | 1.700 |
| bf_tier2 | 3.150 |
| rpd_low | 3.000 |
| rpd_mid | 1.100 |
| rpd_high | 3.300 |
| btts_fade_rpd | 3.100 |
| g15_fade_rpd | 6.000 |
| double_stake_rpd | 1.000 |
| double_stake_min_count | 2 |
