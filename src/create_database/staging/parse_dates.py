from __future__ import annotations

import pandas as pd

from create_database.exceptions import DataValidationError


def parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Parse raw report date into a proper datetime column.

    Rules:
    - use report_date_raw as source column
    - parse dates in DD.MM.YY format
    - raise validation error if parsing fails for any non-null value
    """
    if df.empty:
        return df.copy()

    if "report_date_raw" not in df.columns:
        raise DataValidationError(
            "parse_dates input is missing required column: 'report_date_raw'"
        )

    out = df.copy()

    out["report_date"] = pd.to_datetime(
        out["report_date_raw"],
        format="%d.%m.%y",
        errors="coerce",
    )

    invalid_mask = out["report_date"].isna() & out["report_date_raw"].notna()

    if invalid_mask.any():
        invalid_samples = (
            out.loc[invalid_mask, "report_date_raw"]
            .astype(str)
            .drop_duplicates()
            .head(20)
            .tolist()
        )
        raise DataValidationError(
            "Failed to parse some report_date_raw values using format '%d.%m.%y'. "
            f"Sample invalid values: {invalid_samples}"
        )

    return out