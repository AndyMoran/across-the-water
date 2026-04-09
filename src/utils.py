"""
Shared utilities for the Across the Water project.
"""
import os
import logging
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import numpy as np

# ── Paths ────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("atw")


# ── Date helpers ─────────────────────────────────────────────────────────────
SAMPLE_START = "2018-01-01"   # pre-2020 used only for IFA flow regression
MAIN_START   = "2020-01-01"   # IFA2 coupling start
MAIN_END     = "2026-04-01"


def utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


# ── Parquet helpers ──────────────────────────────────────────────────────────
def save(df: pd.DataFrame, name: str) -> Path:
    path = DATA_DIR / f"{name}.parquet"
    df.to_parquet(path, index=True)
    log.info("Saved %s  (%d rows, %d cols)", path.name, len(df), df.shape[1])
    return path


def load(name: str) -> pd.DataFrame:
    path = DATA_DIR / f"{name}.parquet"
    if not path.exists():
        raise FileNotFoundError(
            f"{path} not found — run Notebook 01 first."
        )
    df = pd.read_parquet(path)
    log.info("Loaded %s  (%d rows)", path.name, len(df))
    return df


# ── Date-range helpers ────────────────────────────────────────────────────────
def date_range_index(start: str = SAMPLE_START, end: str = MAIN_END) -> pd.DatetimeIndex:
    return pd.date_range(start, end, freq="D", tz="UTC")


# ── Regression helpers ────────────────────────────────────────────────────────
def season(month: int) -> str:
    return {12: "Winter", 1: "Winter", 2: "Winter",
            3: "Spring", 4: "Spring", 5: "Spring",
            6: "Summer", 7: "Summer", 8: "Summer",
            9: "Autumn", 10: "Autumn", 11: "Autumn"}[month]


def add_season_col(df: pd.DataFrame, date_col: str = None) -> pd.DataFrame:
    """Add a 'season' column based on the index or a named date column."""
    df = df.copy()
    idx = pd.to_datetime(df[date_col]) if date_col else df.index
    df["season"] = idx.month.map(season)
    return df
