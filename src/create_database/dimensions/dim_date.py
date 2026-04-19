from __future__ import annotations

from pathlib import Path

import pandas as pd

from create_database.exceptions import ConfigurationError, DataValidationError
from create_database.utils.json_loader import load_json


def build_dim_date(staging_df: pd.DataFrame, date_config_path: Path) -> pd.DataFrame:
    """
    Build dim_date from min(report_date) to max(report_date) found in staging_df.

    Output columns:
    - date
    - year
    - quarter
    - month
    - month_name
    - week
    - day
    - day_of_week
    - day_name
    - is_weekend
    - year_month
    - year_week
    """
    date_config = load_json(date_config_path)
    month_names, day_names = _validate_and_extract_date_config(date_config, date_config_path)

    if "report_date" not in staging_df.columns:
        raise DataValidationError("staging_df is missing required column: 'report_date'")

    date_series = pd.to_datetime(staging_df["report_date"], errors="coerce").dropna()

    if date_series.empty:
        raise DataValidationError("Cannot build dim_date: no valid report_date values found.")

    min_date = date_series.min().normalize()
    max_date = date_series.max().normalize()

    if min_date > max_date:
        raise DataValidationError("Cannot build dim_date: min_date is greater than max_date.")

    date_range = pd.date_range(start=min_date, end=max_date, freq="D")
    dim_df = pd.DataFrame({"date": date_range})

    iso_calendar = dim_df["date"].dt.isocalendar()

    dim_df["year"] = dim_df["date"].dt.year
    dim_df["quarter"] = dim_df["date"].dt.quarter
    dim_df["month"] = dim_df["date"].dt.month
    dim_df["month_name"] = dim_df["month"].map(month_names)

    dim_df["week"] = iso_calendar.week.astype(int)
    dim_df["day"] = dim_df["date"].dt.day
    dim_df["day_of_week"] = dim_df["date"].dt.dayofweek + 1  # Monday=1 ... Sunday=7
    dim_df["day_name"] = dim_df["day_of_week"].map(day_names)
    dim_df["is_weekend"] = dim_df["day_of_week"].isin([6, 7])

    if dim_df["month_name"].isna().any():
        missing_months = sorted(dim_df.loc[dim_df["month_name"].isna(), "month"].unique().tolist())
        raise ConfigurationError(
            f"date_dimension.json is missing month_names mappings for months: {missing_months}"
        )

    if dim_df["day_name"].isna().any():
        missing_days = sorted(dim_df.loc[dim_df["day_name"].isna(), "day_of_week"].unique().tolist())
        raise ConfigurationError(
            f"date_dimension.json is missing day_names mappings for day_of_week values: {missing_days}"
        )

    dim_df["year_month"] = (
        dim_df["year"].astype(str)
        + "-"
        + dim_df["month"].astype(str).str.zfill(2)
    )

    dim_df["year_week"] = (
        iso_calendar.year.astype(str)
        + "-W"
        + iso_calendar.week.astype(str).str.zfill(2)
    )

    dim_df["date"] = dim_df["date"].dt.date

    dim_df = dim_df.loc[
        :,
        [
            "date",
            "year",
            "quarter",
            "month",
            "month_name",
            "week",
            "day",
            "day_of_week",
            "day_name",
            "is_weekend",
            "year_month",
            "year_week",
        ],
    ].copy()

    return dim_df


def _validate_and_extract_date_config(
    date_config: object,
    date_config_path: Path,
) -> tuple[dict[int, str], dict[int, str]]:
    """
    Validate date_dimension.json structure and extract month/day name mappings.
    """
    if not isinstance(date_config, dict):
        raise ConfigurationError(
            f"date_dimension.json must contain a JSON object: {date_config_path}"
        )

    required_keys = {"month_names", "day_names"}
    missing = required_keys - set(date_config.keys())
    if missing:
        raise ConfigurationError(
            f"date_dimension.json is missing required keys: {sorted(missing)}"
        )

    month_names_raw = date_config["month_names"]
    day_names_raw = date_config["day_names"]

    if not isinstance(month_names_raw, dict):
        raise ConfigurationError("'month_names' must be an object in date_dimension.json")

    if not isinstance(day_names_raw, dict):
        raise ConfigurationError("'day_names' must be an object in date_dimension.json")

    try:
        month_names = {int(k): str(v).strip() for k, v in month_names_raw.items()}
        day_names = {int(k): str(v).strip() for k, v in day_names_raw.items()}
    except (TypeError, ValueError) as e:
        raise ConfigurationError(
            f"date_dimension.json contains invalid month/day mapping keys: {e}"
        ) from e

    return month_names, day_names