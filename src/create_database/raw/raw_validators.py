from __future__ import annotations

import logging

import pandas as pd

from create_database.exceptions import DataValidationError


log = logging.getLogger(__name__)

RED = "\033[91m"
RESET = "\033[0m"


REQUIRED_RAW_COLUMNS = {
    "source_file",
    "source_sheet",
    "source_sheet_clean",
    "report_date_raw",
    "spot_id_raw",
    "load_timestamp",
}

REQUIRED_NON_NULL_COLUMNS = {
    "source_file",
    "source_sheet",
    "report_date_raw",
    "spot_id_raw",
    "load_timestamp",
}


def validate_raw_dataframe(df: pd.DataFrame) -> None:
    """
    Validate that raw dataframe contains the minimum required technical columns
    and basic technical completeness.
    """
    if df.empty:
        log.error(
            "%sCRITICAL: Raw DataFrame is empty.%s",
            RED,
            RESET,
        )
        raise DataValidationError("Raw DataFrame is empty.")

    missing = REQUIRED_RAW_COLUMNS - set(df.columns)
    if missing:
        log.error(
            "%sCRITICAL: Missing required raw columns: %s%s",
            RED,
            sorted(missing),
            RESET,
        )
        raise DataValidationError(
            f"Missing required raw columns: {sorted(missing)}"
        )

    null_critical_columns = [
        col for col in REQUIRED_NON_NULL_COLUMNS
        if df[col].isna().any()
    ]
    if null_critical_columns:
        log.error(
            "%sCRITICAL: Required raw columns contain null values: %s%s",
            RED,
            sorted(null_critical_columns),
            RESET,
        )
        raise DataValidationError(
            f"Required raw columns contain null values: {sorted(null_critical_columns)}"
        )