# scripts/make_plots.py
"""
Make 2 poster-friendly plots:
1) Distribution of spread_bps (from metrics CSV)
2) Distribution of pnl_net_bps (from trades CSV)

Outputs:
- outputs/figures/spread_hist.png
- outputs/figures/pnl_net_hist.png

Run:
python scripts/make_plots.py --lookback-min 720
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt


def _require_columns(df: pd.DataFrame, cols: list[str], name: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"{name} is missing columns: {missing}. Has: {list(df.columns)}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lookback-min", type=int, default=720)
    ap.add_argument("--processed-dir", type=str, default="data/processed")
    ap.add_argument("--figures-dir", type=str, default="outputs/figures")
    args = ap.parse_args()

    root = Path(__file__).resolve().parents[1]
    processed_dir = (root / args.processed_dir).resolve()
    figures_dir = (root / args.figures_dir).resolve()
    figures_dir.mkdir(parents=True, exist_ok=True)

    metrics_path = processed_dir / f"metrics_last{args.lookback_min}min.csv"
    trades_path = processed_dir / f"trades_last{args.lookback_min}min.csv"

    if not metrics_path.exists():
        raise FileNotFoundError(f"Missing metrics file: {metrics_path}. Run detect_events.py first.")
    if not trades_path.exists():
        raise FileNotFoundError(f"Missing trades file: {trades_path}. Run backtest.py first.")

    # ---- Plot 1: spread_bps distribution ----
    metrics = pd.read_csv(metrics_path)
    _require_columns(metrics, ["spread_bps"], "metrics CSV")

    spread = pd.to_numeric(metrics["spread_bps"], errors="coerce").dropna()
    if spread.empty:
        raise ValueError(f"No numeric spread_bps found in {metrics_path}")

    plt.figure()
    plt.hist(spread, bins=60)
    plt.xlabel("spread_bps")
    plt.ylabel("count")
    plt.title(f"Spread distribution (last {args.lookback_min} min)\nN={len(spread):,}")
    out1 = figures_dir / "spread_hist.png"
    plt.tight_layout()
    plt.savefig(out1, dpi=200)
    plt.close()

    # ---- Plot 2: pnl_net_bps distribution ----
    trades = pd.read_csv(trades_path)
    _require_columns(trades, ["pnl_net_bps"], "trades CSV")

    pnl_net = pd.to_numeric(trades["pnl_net_bps"], errors="coerce").dropna()
    if pnl_net.empty:
        raise ValueError(f"No numeric pnl_net_bps found in {trades_path}")

    plt.figure()
    plt.hist(pnl_net, bins=40)
    plt.xlabel("pnl_net_bps")
    plt.ylabel("count")
    plt.title(f"Net PnL distribution (last {args.lookback_min} min)\nN={len(pnl_net):,}")
    out2 = figures_dir / "pnl_net_hist.png"
    plt.tight_layout()
    plt.savefig(out2, dpi=200)
    plt.close()

    print(f"Saved: {out1}")
    print(f"Saved: {out2}")


if __name__ == "__main__":
    main()
