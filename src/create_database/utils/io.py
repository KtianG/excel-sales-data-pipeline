from __future__ import annotations

from pathlib import Path

import pandas as pd


def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def write_excel(df: pd.DataFrame, path: Path) -> None:
    ensure_parent_dir(path)
    df.to_excel(path, index=False)


def write_csv(df: pd.DataFrame, path: Path) -> None:
    ensure_parent_dir(path)
    df.to_csv(path, index=False, encoding="utf-8-sig")
