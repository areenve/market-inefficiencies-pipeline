import time
import sqlite3
import argparse
from pathlib import Path

import yaml
import requests


VENUES_SUPPORTED = {"COINBASE", "KRAKEN", "BITSTAMP"}


def load_cfg(cfg_path: Path) -> dict:
    cfg = yaml.safe_load(cfg_path.read_text())
    if not isinstance(cfg, dict):
        raise ValueError("config.yaml must parse to a dict")
    return cfg


def ensure_ticks_table(conn: sqlite3.Connection):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ticks (
            ts_ms INTEGER NOT NULL,
            venue TEXT NOT NULL,
            bid REAL NOT NULL,
            ask REAL NOT NULL,
            mid REAL NOT NULL,
            PRIMARY KEY (ts_ms, venue)
        );
        """
    )
    conn.commit()


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def fetch_coinbase(session: requests.Session) -> tuple[float, float]:
    # Coinbase Exchange
    url = "https://api.exchange.coinbase.com/products/BTC-USD/ticker"
    r = session.get(url, timeout=10)
    r.raise_for_status()
    j = r.json()
    bid = float(j["bid"])
    ask = float(j["ask"])
    return bid, ask


def fetch_bitstamp(session: requests.Session) -> tuple[float, float]:
    url = "https://www.bitstamp.net/api/v2/ticker/btcusd/"
    r = session.get(url, timeout=10)
    r.raise_for_status()
    j = r.json()
    bid = float(j["bid"])
    ask = float(j["ask"])
    return bid, ask


def fetch_kraken(session: requests.Session) -> tuple[float, float]:
    url = "https://api.kraken.com/0/public/Ticker?pair=XBTUSD"
    r = session.get(url, timeout=10)
    r.raise_for_status()
    j = r.json()
    res = j["result"]
    key = next(iter(res.keys()))  # e.g. "XXBTZUSD"
    bid = float(res[key]["b"][0])
    ask = float(res[key]["a"][0])
    return bid, ask


FETCHERS = {
    "COINBASE": fetch_coinbase,
    "KRAKEN": fetch_kraken,
    "BITSTAMP": fetch_bitstamp,
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", type=str, default="config.yaml")
    ap.add_argument("--minutes", type=float, default=15.0, help="How long to collect")
    ap.add_argument("--interval-ms", type=int, default=None, help="Override config sample_interval_ms")
    args = ap.parse_args()

    root = Path(__file__).resolve().parents[1]
    cfg_path = (root / args.config).resolve()
    cfg = load_cfg(cfg_path)

    db_path = root / cfg.get("db_path", "data/raw/dislocations.sqlite3")
    db_path.parent.mkdir(parents=True, exist_ok=True)

    interval_ms = args.interval_ms if args.interval_ms is not None else int(cfg.get("sample_interval_ms", 1000))
    venues_cfg = cfg.get("venues", ["COINBASE", "KRAKEN", "BITSTAMP"])
    venues = [v.upper() for v in venues_cfg if v.upper() in VENUES_SUPPORTED]
    if not venues:
        raise ValueError(f"No supported venues enabled. Supported: {sorted(VENUES_SUPPORTED)}")

    print(f"Config: {cfg_path}")
    print(f"DB: {db_path}")
    print(f"Venues: {venues}")
    print(f"Interval: {interval_ms} ms")
    print(f"Minutes: {args.minutes}")

    conn = sqlite3.connect(db_path)
    ensure_ticks_table(conn)

    session = requests.Session()
    t0 = time.time()
    last_print = 0.0
    rows = 0

    try:
        while (time.time() - t0) < args.minutes * 60:
            ts = now_ms()
            for venue in venues:
                try:
                    bid, ask = FETCHERS[venue](session)
                    mid = (bid + ask) / 2.0
                    conn.execute(
                        "INSERT OR REPLACE INTO ticks (ts_ms, venue, bid, ask, mid) VALUES (?,?,?,?,?)",
                        (ts, venue, bid, ask, mid),
                    )
                    rows += 1
                except Exception as e:
                    print(f"[WARN] {venue}: {e}")

            conn.commit()

            if (time.time() - last_print) > 10:
                last_print = time.time()
                print(f"… collected rows so far: {rows}")

            time.sleep(interval_ms / 1000.0)

    except KeyboardInterrupt:
        print("\nStopped by user.")

    finally:
        conn.close()

    print(f"✅ Done. Inserted/updated ~{rows} rows into ticks.")


if __name__ == "__main__":
    main()
