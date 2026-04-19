from __future__ import annotations

import re
import pandas as pd

from create_database.exceptions import DataValidationError


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize column names without changing business meaning.

    Rules:
    - convert to string
    - strip leading/trailing whitespace
    - collapse multiple spaces into one
    - validate no duplicate column names after normalization
    """
    if df.empty:
        return df.copy()

    out = df.copy()

    normalized_columns = [
        re.sub(r"\s+", " ", str(col).strip())
        for col in out.columns
    ]

    # check duplicates after normalization
    duplicates = pd.Series(normalized_columns).duplicated(keep=False)
    if duplicates.any():
        dup_cols = (
            pd.Series(normalized_columns)[duplicates]
            .drop_duplicates()
            .tolist()
        )
        raise DataValidationError(
            f"Duplicate column names after normalization: {dup_cols}"
        )

    out.columns = normalized_columns

    return out