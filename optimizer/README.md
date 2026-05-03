# Bet Tracker Optimizer

Bayesian optimization (Optuna) for the bet tracker strategy parameters.

**Self-contained:** none of these scripts modify the main pipeline. The only file they touch is the master spreadsheet, and only via `apply_params.py` after you confirm.

## Files

- `backtest.py` — pure simulation engine. Reads master sheet, computes P&L for a given parameter config.
- `optimize.py` — Optuna search. Optimizes profit on a training set, validates on a 30-day holdout, writes a CSV and Markdown report.
- `apply_params.py` — takes a chosen config from a report and rewrites the stake formulas in the master sheet.
- `results/` — generated optimization reports (timestamped).
- `active_params.json` — record of the most recently applied config (created on first apply).

## Workflow

### 1. Run an optimization

```bash
python optimizer/optimize.py
```

Defaults: 500 trials, 30-day holdout, min 100 bets per config, top 20 reported.

Options:
```bash
python optimizer/optimize.py --trials 1000 --holdout-days 60 --min-bets 50 --top-n 30
```

### 2. Review the report

Open `optimizer/results/optim_YYYYMMDD_HHMMSS.md` and look at the **test column**. Configs with high train profit but poor test performance are overfit — skip them.

A good config has:
- Reasonable train profit (not the maximum, just solid)
- **Positive test profit** with similar ROI to train
- **Bet count similar to baseline** or modestly larger
- Parameters that aren't extreme outliers

### 3. (Optional) Dry-run apply

Preview what would change without writing:

```bash
python optimizer/apply_params.py --config optimizer/results/optim_20260406_143348.csv --rank 3 --dry-run
```

### 4. Apply the chosen config

```bash
python optimizer/apply_params.py --config optimizer/results/optim_20260406_143348.csv --rank 3
```

You'll be prompted to confirm before any writes. The chosen config is recorded in `optimizer/active_params.json` for reference.

### 5. Restore baseline

The CSV always includes a row with rank `baseline` (the current default params):

```bash
python optimizer/apply_params.py --config optimizer/results/optim_20260406_143348.csv --rank baseline
```

## Notes on overfitting

The optimizer can produce parameter sets that look great on training data but flop on real bets. **Always check the test column** before applying.

Common signs of overfitting:
- Train ROI very high, test ROI negative or near zero
- Test bet count is tiny (small sample = noise)
- Parameter values at the boundary of the search space (e.g. vol_max = 2000)

If every top config fails on the test set, **don't apply anything** — the strategy may already be optimal or the parameters need re-thinking.

## What gets tuned

| Parameter | Description | Default |
|---|---|---|
| `vol_min` / `vol_max` | Volume range filter | 40 / 700 |
| `bf_min` | BF must exceed this | 1.30 |
| `bf_tier1` / `bf_tier2` | Tier breakpoints for RPD schedule | 1.80 / 2.50 |
| `rpd_low` / `rpd_mid` / `rpd_high` | RPD ceiling per tier | 1.0 / 2.0 / 3.5 |
| `btts_fade_rpd` | BTTS fade RPD threshold | 5.0 |
| `g15_fade_rpd` | 1.5G fade RPD threshold | 4.6 |
| `double_stake_rpd` | RPD value triggering 2x check | 1.0 |
| `double_stake_min_count` | Min core bets per match for 2x | 2 |