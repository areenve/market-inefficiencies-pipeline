
import argparse
import sqlite3
from pathlib import Path

import yaml
import pandas as pd


def load_cfg(cfg_path: Path) -> dict:
    cfg = yaml.safe_load(cfg_path.read_text())
    if not isinstance(cfg, dict):
        raise ValueError("config.yaml must parse to a dict")
    return cfg


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", type=str, default="config.yaml")
    ap.add_argument("--lookback-min", type=int, default=180)
    ap.add_argument("--threshold-bps", type=float, default=None)
    ap.add_argument("--persistence-ms", type=int, default=None)
    args = ap.parse_args()

    root = Path(__file__).resolve().parents[1]
    cfg_path = (root / args.config).resolve()
    cfg = load_cfg(cfg_path)

    db_path = root / cfg.get("db_path", "data/raw/dislocations.sqlite3")
    out_dir = root / cfg.get("out_dir", "data/processed")
    out_dir.mkdir(parents=True, exist_ok=True)

    threshold_bps = float(args.threshold_bps if args.threshold_bps is not None else cfg.get("threshold_bps", 5.0))
    persistence_ms = int(args.persistence_ms if args.persistence_ms is not None else cfg.get("persistence_ms", 300))

    conn = sqlite3.connect(db_path)

    max_ts = pd.read_sql_query("SELECT MAX(ts_ms) AS mx FROM ticks", conn)["mx"].iloc[0]
    if pd.isna(max_ts):
        raise RuntimeError("ticks table is empty. Run collect first.")

    lb_ms = args.lookback_min * 60 * 1000
    start_ts = int(max_ts - lb_ms)

    ticks = pd.read_sql_query(
        "SELECT ts_ms, venue, mid FROM ticks WHERE ts_ms >= ? ORDER BY ts_ms ASC",
        conn,
        params=(start_ts,),
    )
    conn.close()

    # build metric series by “as-of” updating latest mid per venue
    last_mid = {}
    metric_rows = []

    for ts, venue, mid in ticks.itertuples(index=False):
        last_mid[venue] = float(mid)
        if len(last_mid) >= 2:
            items = list(last_mid.items())
            min_venue, min_mid = min(items, key=lambda x: x[1])
            max_venue, max_mid = max(items, key=lambda x: x[1])
            avg_mid = (min_mid + max_mid) / 2.0
            spread_bps = (max_mid - min_mid) / avg_mid * 1e4
            metric_rows.append((ts, min_venue, max_venue, min_mid, max_mid, spread_bps))

    metrics = pd.DataFrame(
        metric_rows,
        columns=["ts_ms", "min_venue", "max_venue", "min_mid", "max_mid", "spread_bps"],
    )

    if metrics.empty:
        print("No metrics computed (need at least 2 venues with data).")
        return

    # event detection: contiguous time above threshold
    events = []
    in_event = False
    start = peak = None
    peak_v = None

    for row in metrics.itertuples(index=False):
        ts = int(row.ts_ms)
        bps = float(row.spread_bps)

        if bps >= threshold_bps:
            if not in_event:
                in_event = True
                start = ts
                peak = bps
                peak_v = (row.min_venue, row.max_venue)
            else:
                if bps > peak:
                    peak = bps
                    peak_v = (row.min_venue, row.max_venue)
        else:
            if in_event:
                end = ts
                dur = end - start
                if dur >= persistence_ms:
                    events.append(
                        dict(
                            start_ms=start,
                            end_ms=end,
                            duration_ms=dur,
                            peak_bps=peak,
                            min_venue=peak_v[0],
                            max_venue=peak_v[1],
                            n_points=None,
                        )
                    )
                in_event = False

    # close trailing event
    if in_event:
        end = int(metrics["ts_ms"].iloc[-1])
        dur = end - start
        if dur >= persistence_ms:
            events.append(
                dict(
                    start_ms=start,
                    end_ms=end,
                    duration_ms=dur,
                    peak_bps=peak,
                    min_venue=peak_v[0],
                    max_venue=peak_v[1],
                    n_points=None,
                )
            )

    events_df = pd.DataFrame(events)

    metrics_path = out_dir / f"metrics_last{args.lookback_min}min.csv"
    events_path = out_dir / f"events_last{args.lookback_min}min.csv"
    metrics.to_csv(metrics_path, index=False)
    events_df.to_csv(events_path, index=False)

    print(f"Saved metrics: {metrics_path}")
    print(f"Saved events:  {events_path}")
    print(f"Events found: {len(events_df)}")


if __name__ == "__main__":
    main()
