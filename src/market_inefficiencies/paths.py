# src/market_inefficiencies/paths.py
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]  # .../market-inefficiencies

DATA_DIR = ROOT / "data"
DATA_RAW = DATA_DIR / "raw"
DATA_PROCESSED = DATA_DIR / "processed"

OUTPUTS_DIR = ROOT / "outputs"
FIGURES_DIR = OUTPUTS_DIR / "figures"
TABLES_DIR = OUTPUTS_DIR / "tables"
