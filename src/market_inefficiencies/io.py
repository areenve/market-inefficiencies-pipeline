# src/market_inefficiencies/io.py
from __future__ import annotations

from pathlib import Path
import zipfile
import shutil

from .paths import DATA_RAW, DATA_PROCESSED, FIGURES_DIR, TABLES_DIR


def ensure_project_dirs() -> None:
    DATA_RAW.mkdir(parents=True, exist_ok=True)
    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    TABLES_DIR.mkdir(parents=True, exist_ok=True)


def unzip_to_raw(zip_path: Path, folder_name: str) -> Path:
    if not zip_path.exists():
        raise FileNotFoundError(f"Zip not found: {zip_path}")

    out_dir = DATA_RAW / folder_name
    out_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(out_dir)

    macosx = out_dir / "__MACOSX"
    if macosx.exists():
        shutil.rmtree(macosx, ignore_errors=True)

    return out_dir
from pathlib import Path

def ensure_project_dirs() -> dict[str, Path]:
    """
    Create the standard project folder structure if it doesn't exist.
    Returns a dict of important paths.
    """
    root = Path(__file__).resolve().parents[2]  # project root (…/market-inefficiencies)

    paths = {
        "root": root,
        "data_raw": root / "data" / "raw",
        "data_processed": root / "data" / "processed",
        "outputs": root / "outputs",
        "figures": root / "outputs" / "figures",
        "tables": root / "outputs" / "tables",
        "reports": root / "reports",
    }

    for p in paths.values():
        if p.suffix == "":  # folder path
            p.mkdir(parents=True, exist_ok=True)

    return paths

