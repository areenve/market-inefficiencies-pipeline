# Market Inefficiencies Pipeline (Cross-Venue Price Dislocations)

Python pipeline that collects cross-venue mid-prices (ticks), detects short-lived price dislocations between venues, and runs a simple friction-aware backtest.

This repo is a cleaned-up, script-first version of my earlier notebook/Colab workflow.

---

## What this project does

**End-to-end workflow:**
1) **Collect ticks** into SQLite  
2) **Detect dislocation events** (spread in bps between venues)  
3) **Backtest** event-driven trades with rough trading frictions  
4) Save **CSV summaries** + **figures** for reporting / posters

**Outputs you can show quickly:**
- `data/processed/metrics_last{N}min.csv` (spread time series / metrics)
- `data/processed/events_last{N}min.csv` (detected dislocation events)
- `data/processed/trades_last{N}min.csv` (trade-level results incl. `pnl_net_bps`)
- `outputs/figures/` (plots for poster / README)

---

## Repo structure

- `scripts/` — “run this” entrypoints (collection, detection, backtest, plots)
- `src/market_inefficiencies/` — reusable modules (I/O, features, analysis, plotting)
- `data/raw/` — local SQLite DB (ignored by git)
- `data/processed/` — small CSV outputs (safe to commit)
- `outputs/figures/` — small PNG plots (safe to commit)

---

## Quickstart (local)

### 1) Setup environment
From repo root:

```bash
python -m pip install -r requirements.txt

