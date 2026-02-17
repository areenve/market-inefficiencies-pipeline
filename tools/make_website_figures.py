from __future__ import annotations

from pathlib import Path
import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# =========================
# CONFIG
# =========================

# Your DB with ticks table
DB_PATH = Path("/Users/arina/Downloads/dislocations-2.sqlite3")

# Output folder in your website repo (from inside ~/market-inefficiencies-pipeline)
OUTDIR = Path("../areenve.github.io/public/images")

# Simple cost model for "net edge" (bps)
FEES_BPS = 2.0
SLIPPAGE_BPS = 1.0

# Spread tail threshold
TAIL_THRESHOLD_BPS = 3.0

# How many rows in "top signals" table image
TOP_N_SIGNALS_TABLE = 15

# How many venue pairs to show
TOP_K_VENUE_PAIRS = 12


# =========================
# UTIL
# =========================

def ensure_outdir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def savefig(path: Path) -> None:
    plt.tight_layout()
    plt.savefig(path, dpi=200)
    plt.close()


# =========================
# LOAD
# =========================

def load_ticks() -> pd.DataFrame:
    """
    Expects table ticks with columns:
    ts_ms (int ms), venue (str), bid (float), ask (float), mid (float)
    """
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DB not found: {DB_PATH}")

    conn = sqlite3.connect(str(DB_PATH))
    df = pd.read_sql_query("SELECT * FROM ticks;", conn)
    conn.close()

    if df.empty:
        raise RuntimeError("ticks table is empty.")

    required = {"ts_ms", "venue", "bid", "ask", "mid"}
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"ticks missing columns: {missing}. Found: {df.columns.tolist()}")

    df["ts_ms"] = df["ts_ms"].astype("int64")
    df["ts"] = pd.to_datetime(df["ts_ms"], unit="ms", utc=True)

    df["venue"] = df["venue"].astype(str)
    df["bid"] = df["bid"].astype(float)
    df["ask"] = df["ask"].astype(float)
    df["mid"] = df["mid"].astype(float)

    # spread in bps
    df["spread"] = df["ask"] - df["bid"]
    df["spread_bps"] = np.where(df["mid"] != 0, (df["spread"] / df["mid"]) * 1e4, np.nan)

    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.dropna(subset=["ts", "venue", "bid", "ask", "mid", "spread_bps"])
    return df


# =========================
# FIGURES: SPREAD
# =========================

def plot_spread_zoom(ticks: pd.DataFrame, outdir: Path) -> None:
    x = ticks["spread_bps"].dropna()
    x = x[(x >= 0) & (x <= 6)]
    if len(x) == 0:
        print("[skip] spread zoom: no data in 0..6 bps")
        return

    p50, p90, p95, p99 = np.percentile(x, [50, 90, 95, 99])

    plt.figure(figsize=(10, 5))
    plt.hist(x, bins=30, density=True)
    for val, name in [(p50, "p50"), (p90, "p90"), (p95, "p95"), (p99, "p99")]:
        plt.axvline(val, linestyle="--")
        plt.text(val, plt.ylim()[1] * 0.92, f"{name}={val:.2f}", rotation=90, va="top")

    plt.xlim(0, 6)
    plt.title(f"Bid–Ask Spread (0–6 bps) | N={len(x):,}")
    plt.xlabel("spread_bps")
    plt.ylabel("density")
    plt.grid(alpha=0.2)

    savefig(outdir / "spread_hist_zoom_0_6.png")
    print("[ok] spread_hist_zoom_0_6.png")


def plot_spread_tail(ticks: pd.DataFrame, outdir: Path) -> None:
    x = ticks["spread_bps"].dropna()
    tail = x[x >= TAIL_THRESHOLD_BPS]
    if len(tail) == 0:
        print("[skip] spread tail: no data in tail")
        return

    plt.figure(figsize=(10, 5))
    plt.hist(tail, bins=30)
    plt.yscale("log")
    plt.axvline(TAIL_THRESHOLD_BPS, linestyle="--")
    plt.title(f"Bid–Ask Spread Tail (>= {TAIL_THRESHOLD_BPS:g} bps) | N_tail={len(tail):,}")
    plt.xlabel("spread_bps")
    plt.ylabel("count (log scale)")
    plt.grid(alpha=0.2)

    savefig(outdir / "spread_hist_tail.png")
    print("[ok] spread_hist_tail.png")


# =========================
# SIGNALS (computed from ticks)
# =========================

def compute_signals_from_ticks(ticks: pd.DataFrame) -> pd.DataFrame:
    """
    Build executable cross-venue signals at each timestamp:
      - best buy venue = venue with MIN ask
      - best sell venue = venue with MAX bid
    Gross edge bps = (sell_bid - buy_ask) / mid * 1e4
    Net edge bps = gross - (fees + slippage)
    """
    df = ticks[["ts", "venue", "bid", "ask", "mid"]].copy()
    df = df.dropna(subset=["ts", "venue", "bid", "ask", "mid"])

    # Pivot to have columns per venue at each ts
    bid_p = df.pivot_table(index="ts", columns="venue", values="bid", aggfunc="last")
    ask_p = df.pivot_table(index="ts", columns="venue", values="ask", aggfunc="last")

    venues = list(bid_p.columns.intersection(ask_p.columns))
    if len(venues) < 2:
        return pd.DataFrame()

    best_sell_bid = bid_p[venues].max(axis=1)
    best_sell_venue = bid_p[venues].idxmax(axis=1)

    best_buy_ask = ask_p[venues].min(axis=1)
    best_buy_venue = ask_p[venues].idxmin(axis=1)

    # Same-venue not useful
    mask = best_buy_venue != best_sell_venue
    best_sell_bid = best_sell_bid[mask]
    best_sell_venue = best_sell_venue[mask]
    best_buy_ask = best_buy_ask[mask]
    best_buy_venue = best_buy_venue[mask]

    # Mid for bps scaling: use midpoint between best buy ask and best sell bid
    mid = (best_buy_ask + best_sell_bid) / 2.0
    gross_edge_bps = np.where(mid != 0, ((best_sell_bid - best_buy_ask) / mid) * 1e4, np.nan)

    signals = pd.DataFrame({
        "ts": best_sell_bid.index,
        "buy_venue": best_buy_venue.values,
        "sell_venue": best_sell_venue.values,
        "buy_ask": best_buy_ask.values,
        "sell_bid": best_sell_bid.values,
        "gross_edge_bps": gross_edge_bps,
    })

    signals["fees_bps"] = FEES_BPS
    signals["slippage_bps"] = SLIPPAGE_BPS
    signals["net_edge_bps"] = signals["gross_edge_bps"] - (signals["fees_bps"] + signals["slippage_bps"])

    signals = signals.replace([np.inf, -np.inf], np.nan)
    signals = signals.dropna(subset=["gross_edge_bps", "net_edge_bps"])
    return signals


def plot_venue_pair_counts(signals: pd.DataFrame, outdir: Path) -> None:
    if signals.empty:
        print("[skip] venue_pair_counts: no signals")
        return

    signals = signals.copy()
    signals["venue_pair"] = signals["buy_venue"].astype(str) + " → " + signals["sell_venue"].astype(str)
    counts = signals["venue_pair"].value_counts().head(TOP_K_VENUE_PAIRS)

    if len(counts) == 0:
        print("[skip] venue_pair_counts: no data")
        return

    plt.figure(figsize=(10, 5))
    plt.barh(counts.index[::-1], counts.values[::-1])
    plt.title("Top Venue Pairs by Signal Count")
    plt.xlabel("signal count")
    plt.grid(alpha=0.2)

    savefig(outdir / "venue_pair_counts.png")
    print("[ok] venue_pair_counts.png")


def plot_net_edge_dist(signals: pd.DataFrame, outdir: Path) -> None:
    if signals.empty:
        print("[skip] net_edge_dist: no signals")
        return

    x = signals["net_edge_bps"].dropna()
    if len(x) == 0:
        print("[skip] net_edge_dist: no net_edge_bps")
        return

    plt.figure(figsize=(10, 5))
    plt.hist(x, bins=40)
    plt.axvline(0, linestyle="--")
    plt.title("Net Edge After Costs (bps)")
    plt.xlabel("net_edge_bps")
    plt.ylabel("count")
    plt.grid(alpha=0.2)

    savefig(outdir / "net_edge_dist.png")
    print("[ok] net_edge_dist.png")


def make_top_signals_table(signals: pd.DataFrame, outdir: Path) -> None:
    if signals.empty:
        print("[skip] top_signals_table: no signals")
        return

    s = signals.sort_values("net_edge_bps", ascending=False).head(TOP_N_SIGNALS_TABLE).copy()
    s["ts"] = pd.to_datetime(s["ts"], utc=True).dt.strftime("%Y-%m-%d %H:%M:%S")

    cols = ["ts", "buy_venue", "sell_venue", "buy_ask", "sell_bid", "gross_edge_bps", "fees_bps", "slippage_bps", "net_edge_bps"]
    s = s[cols]

    # format numbers
    for c in ["buy_ask", "sell_bid"]:
        s[c] = s[c].map(lambda v: f"{v:.2f}")
    for c in ["gross_edge_bps", "fees_bps", "slippage_bps", "net_edge_bps"]:
        s[c] = s[c].map(lambda v: f"{v:.2f}")

    fig, ax = plt.subplots(figsize=(14, 0.6 + 0.35 * len(s)))
    ax.axis("off")

    tbl = ax.table(
        cellText=s.values,
        colLabels=s.columns,
        cellLoc="center",
        loc="center"
    )
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(9)
    tbl.scale(1, 1.2)

    plt.title("Top Signals (ranked by net edge after costs)", pad=12)

    outpath = outdir / "top_signals_table.png"
    plt.tight_layout()
    plt.savefig(outpath, dpi=200)
    plt.close()

    print("[ok] top_signals_table.png")


# =========================
# MAIN
# =========================

def main():
    ensure_outdir(OUTDIR)

    print("Loading ticks from:", DB_PATH)
    ticks = load_ticks()
    print("ticks rows:", len(ticks), "| venues:", sorted(ticks["venue"].unique().tolist())[:10], ("..." if ticks["venue"].nunique() > 10 else ""))

    # Spread figures
    plot_spread_zoom(ticks, OUTDIR)
    plot_spread_tail(ticks, OUTDIR)

    # Signals + venue figures
    signals = compute_signals_from_ticks(ticks)
    print("signals rows:", len(signals))
    plot_venue_pair_counts(signals, OUTDIR)
    plot_net_edge_dist(signals, OUTDIR)
    make_top_signals_table(signals, OUTDIR)

    print("\nDone. Images written to:", OUTDIR.resolve())


if __name__ == "__main__":
    main()

