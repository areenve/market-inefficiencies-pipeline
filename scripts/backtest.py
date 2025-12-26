#!/usr/bin/env python3
"""
Backtest detected cross-venue dislocation events.

Inputs:
- config.yaml (db_path, out_dir, costs_bps, latency_ms)
- data/processed/events_last{lookback}min.csv (from scripts/detect_events.py)

Outputs:
- data/processed/trades_last{lookback}min.csv

This is a didactic, friction-aware backtest:
- Gross PnL (pnl_bps) is based on event spread_bps (or reconstructed from mids).
- Net PnL subtracts simple costs (fee+half_spread+slippage) for both legs, enter+exit.
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Dict, Any, Optional

import pandas as pd
import yaml
from pandas.errors import EmptyDataError


def load_cfg(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError("config.yaml must parse to a dict/object")
    return data


def total_cost_bps_from_cfg(cfg: Dict[str, Any]) -> float:
    # costs_bps: {fee_bps: 2, half_spread_bps: 1, slippage_bps: 3}
    costs = cfg.get("costs_bps", {}) or {}
    if not isinstance(costs, dict):
        return 0.0
    per_action_bps = float(sum(float(v) for v in costs.values()))
    # 2 legs (long+short) * (enter+exit) = 4 actions
    return 4.0 * per_action_bps


def nearest_mid(ticks: pd.DataFrame, ts_ms: int, venue: str) -> Optional[float]:
    """
    Get mid at/near ts_ms for a given venue using the nearest timestamp.
    Assumes ticks has columns: ts_ms, venue, mid
    """
    sub = ticks[ticks["venue"] == venue]
    if sub.empty:
        return None
    # Find nearest timestamp
    # (ticks are usually dense enough; this is fine for a didactic backtest)
    i = (sub["ts_ms"] - ts_ms).abs().idxmin()
    val = sub.loc[i, "mid"]
    try:
        return float(val)
    except Exception:
        return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", type=str, default="config.yaml", help="Path to config.yaml (relative to repo root)")
    ap.add_argument("--lookback-min", type=int, default=180, help="Lookback minutes (must match detect_events output)")
    args = ap.parse_args()

    root = Path(__file__).resolve().parents[1]
    cfg_path = (root / args.config).resolve()
    cfg = load_cfg(cfg_path)

    db_path = (root / cfg.get("db_path", "data/raw/dislocations.sqlite3")).resolve()
    proc_dir = (root / cfg.get("out_dir", "data/processed")).resolve()
    proc_dir.mkdir(parents=True, exist_ok=True)

    events_path = proc_dir / f"events_last{args.lookback_min}min.csv"
    trades_path = proc_dir / f"trades_last{args.lookback_min}min.csv"

    # --- load events safely (handles "0 events" file) ---
    if (not events_path.exists()) or events_path.stat().st_size == 0:
        print(f"No events to backtest (events file missing/empty): {events_path}")
        return

    try:
        events = pd.read_csv(events_path)
    except EmptyDataError:
        print(f"No events to backtest (events csv has no columns): {events_path}")
        return

    if events.empty:
        print("No events to backtest.")
        return
    # ---------------------------------------------------

    if not db_path.exists():
        raise FileNotFoundError(f"SQLite DB not found: {db_path}")

    conn = sqlite3.connect(str(db_path))
    ticks = pd.read_sql_query(
        "SELECT ts_ms, venue, mid FROM ticks ORDER BY ts_ms ASC",
        conn,
    )
    conn.close()

    if ticks.empty:
        print("Ticks table is empty — run collect_ticks first.")
        return

    # Ensure correct dtypes
    ticks["ts_ms"] = pd.to_numeric(ticks["ts_ms"], errors="coerce").astype("Int64")
    ticks = ticks.dropna(subset=["ts_ms", "venue", "mid"]).copy()
    ticks["ts_ms"] = ticks["ts_ms"].astype(int)
    ticks["mid"] = pd.to_numeric(ticks["mid"], errors="coerce")
    ticks = ticks.dropna(subset=["mid"]).copy()

    # Costs and latency
    total_cost_bps = total_cost_bps_from_cfg(cfg)
    latency_ms = cfg.get("latency_ms", 0) or 0
    try:
        latency_ms = int(latency_ms)
    except Exception:
        latency_ms = 0

    # Expected columns from detect_events:
    # start_ms, end_ms, min_venue, max_venue, min_mid, max_mid, spread_bps
    required_cols = {"start_ms", "end_ms", "min_venue", "max_venue"}
    missing = required_cols - set(events.columns)
    if missing:
        raise ValueError(f"Events file missing required columns: {sorted(missing)}")

    trades = []
    for r in events.itertuples(index=False):
        start_ms = int(getattr(r, "start_ms"))
        end_ms = int(getattr(r, "end_ms"))
        long_v = str(getattr(r, "min_venue"))
        short_v = str(getattr(r, "max_venue"))

        # Apply latency: assume you only act after latency
        entry_ts = start_ms + latency_ms
        exit_ts = end_ms + latency_ms

        # Gross pnl estimation:
        # Prefer spread_bps if present; otherwise reconstruct from mids at entry
        spread_bps = None
        if "spread_bps" in events.columns:
            try:
                spread_bps = float(getattr(r, "spread_bps"))
            except Exception:
                spread_bps = None

        # Entry mids (nearest)
        long_entry = nearest_mid(ticks, entry_ts, long_v)
        short_entry = nearest_mid(ticks, entry_ts, short_v)

        # Optional: exit mids
        long_exit = nearest_mid(ticks, exit_ts, long_v)
        short_exit = nearest_mid(ticks, exit_ts, short_v)

        # If spread_bps missing, estimate from entry
        # Using mid-based approximation: (short - long)/mid_avg * 10,000
        if spread_bps is None and (long_entry is not None) and (short_entry is not None):
            mid_avg = (long_entry + short_entry) / 2.0
            if mid_avg > 0:
                spread_bps = (short_entry - long_entry) / mid_avg * 10000.0

        # If still missing, skip
        if spread_bps is None:
            continue

        # A simple “capture” model:
        # gross pnl = spread_bps (didactic)
        pnl_bps = float(spread_bps)
        pnl_net_bps = pnl_bps - float(total_cost_bps)

        trades.append(
            {
                "start_ms": start_ms,
                "end_ms": end_ms,
                "duration_ms": end_ms - start_ms,
                "entry_ts": entry_ts,
                "exit_ts": exit_ts,
                "long_venue": long_v,
                "short_venue": short_v,
                "long_entry_mid": long_entry,
                "short_entry_mid": short_entry,
                "long_exit_mid": long_exit,
                "short_exit_mid": short_exit,
                "spread_bps": spread_bps,
                "pnl_bps": pnl_bps,
                "total_cost_bps": total_cost_bps,
                "pnl_net_bps": pnl_net_bps,
            }
        )

    trades_df = pd.DataFrame(trades)
    if trades_df.empty:
        print("No trades produced (missing spread_bps or venues not found in ticks).")
        return

    trades_df.to_csv(trades_path, index=False)
    print(f"Saved trades: {trades_path}")

    # Summary
    print("Summary:")
    print(trades_df[["pnl_bps", "pnl_net_bps"]].describe())


if __name__ == "__main__":
    main()
