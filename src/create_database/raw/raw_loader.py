from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from create_database.raw.raw_validators import validate_raw_dataframe


log = logging.getLogger(__name__)


def save_raw_to_csv(df: pd.DataFrame, output_path: Path) -> None:
    """
    Save raw dataframe to CSV without applying business transformations.
    """
    validate_raw_dataframe(df)

    if df.empty:
        raise ValueError("Cannot save empty raw dataframe.")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(
        output_path,
        index=False,
        encoding="utf-8",
        sep=",",
        na_rep="",
    )

    log.info("Saved raw data to %s (%s rows)", output_path, len(df))