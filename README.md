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
**From repo root:**
python -m pip install -r requirements.txt

### 2) Configure

**Edit `config.yaml` to control:**
- `db_path`: SQLite location (default: `data/raw/dislocations.sqlite3`)
- `costs_bps`: trading frictions (`fee_bps`, `half_spread_bps`, `slippage_bps`)
- `latency_ms`: execution delay assumption
- `persistence_ms`: event persistence filter

### 3) Run the pipeline

**A) Collect ticks (writes to SQLite)**
python scripts/collect_ticks.py --minutes 60

**B) Detect events (writes metrics + events CSV)**
python scripts/detect_events.py --lookback-min 720 --threshold-bps 6 --persistence-ms 600

**C) Backtest (writes trades CSV + prints summary)**
python scripts/backtest.py --lookback-min 720

**D) Make plots for poster / README (writes PNGs)**
python scripts/make_plots.py --lookback-min 720

### Interpreting results
**pnl_bps:** gross event payoff (basis points), before costs

**pnl_net_bps:** payoff after estimated frictions
(fees + spread crossing + slippage, plus latency assumptions)

Note: Many small dislocations disappear after realistic frictions — that’s the point of the pipeline.

### 4) Reproducible demo run (copy/paste)
python scripts/collect_ticks.py --minutes 60

python scripts/detect_events.py --lookback-min 180 --threshold-bps 6 --persistence-ms 600

python scripts/backtest.py --lookback-min 180

python scripts/make_plots.py --lookback-min 180

## Notes
Raw SQLite DB is ignored by git (data/raw/).
data/processed/ CSV summaries and outputs/figures/ PNGs are small and safe to commit.
During low-activity windows you may see fewer detected events.

### Save, then commit + push
git add README.md
git commit -m "Improve README with pipeline usage and outputs"
git push

### Author
**Arina Veprikova** — BSc Data Science (Finance minor), SFU
**Interests:** quant research, market microstructure, data pipelines, evaluation workflows
