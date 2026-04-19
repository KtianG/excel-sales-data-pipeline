from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from create_database.exceptions import ConfigurationError
from create_database.utils.json_loader import load_json


log = logging.getLogger(__name__)

YELLOW = "\033[93m"
RESET = "\033[0m"


def enrich_sales_types(df: pd.DataFrame, sale_types_json_path: Path) -> pd.DataFrame:
    """
    Enrich staging data with sales-type-related logic.

    Current scope:
    - load fixed sale type configuration from JSON
    - detect dynamic percentage columns (e.g. 20%, 30%, 40%)
    - build grouped columns (e.g. Paczki = Paczki 1 + Paczki 2)
    - keep control column 'Suma'
    - compute sales_total_computed for validation
    - add comparison columns against control total
    - allow validation exclusions for selected technical rows
    """
    if df.empty:
        return df.copy()

    out = df.copy()

    config = load_json(sale_types_json_path)
    _validate_sale_types_config(config, sale_types_json_path)

    fixed_sale_types = config["fixed_sale_types"]
    grouped_columns = config["grouped_columns"]
    control_columns = config["control_columns"]
    validation_exclusions = config.get("validation_exclusions", [])

    percent_columns = _detect_percent_columns(out)

    # Ensure fixed sale type columns are numeric if they exist
    fixed_source_columns = [
        item["source_column"]
        for item in fixed_sale_types
        if item["source_column"] in out.columns
    ]
    _coerce_numeric_columns(out, fixed_source_columns)

    # Ensure dynamic percent columns are numeric
    _coerce_numeric_columns(out, percent_columns)

    # Build grouped columns from existing source columns only
    grouped_target_columns: list[str] = []
    grouped_source_columns_used: set[str] = set()

    for rule in grouped_columns:
        target_column = rule["target_column"]
        source_columns = rule["source_columns"]

        existing_source_columns = [col for col in source_columns if col in out.columns]
        grouped_source_columns_used.update(existing_source_columns)

        if existing_source_columns:
            _coerce_numeric_columns(out, existing_source_columns)
            out[target_column] = out[existing_source_columns].fillna(0).sum(axis=1)
        else:
            out[target_column] = 0

        grouped_target_columns.append(target_column)

    # Identify control column(s)
    control_source_columns = [
        item["source_column"]
        for item in control_columns
        if item["source_column"] in out.columns
    ]
    _coerce_numeric_columns(out, control_source_columns)

    # Compute total sales quantity from recognized sales-type columns
    computed_total_columns = _get_computed_total_columns(
        df=out,
        fixed_sale_types=fixed_sale_types,
        percent_columns=percent_columns,
        grouped_target_columns=grouped_target_columns,
        grouped_source_columns_used=grouped_source_columns_used,
    )

    if computed_total_columns:
        out["sales_total_computed"] = out[computed_total_columns].fillna(0).sum(axis=1)
    else:
        out["sales_total_computed"] = 0

    # Build exclusion mask for validation
    exclusion_mask = _build_validation_exclusion_mask(
        df=out,
        validation_exclusions=validation_exclusions,
    )
    out["sales_validation_excluded"] = exclusion_mask

    # For current version we assume one control total column, typically 'Suma'
    control_total_column = _get_control_total_column(control_columns)

    if control_total_column and control_total_column in out.columns:
        out["sales_total_difference"] = (
            out[control_total_column].fillna(0) - out["sales_total_computed"].fillna(0)
        )
        out["sales_total_matches_control"] = out["sales_total_difference"].fillna(0).eq(0)

        validation_mask = ~out["sales_validation_excluded"]
        mismatch_mask = validation_mask & (~out["sales_total_matches_control"])

        mismatches = int(mismatch_mask.sum())
        if mismatches > 0:
            log.warning(
                "%sCONTROL WARNING: %s row(s) have mismatch between '%s' and computed sales total.%s",
                YELLOW,
                mismatches,
                control_total_column,
                RESET,
            )
    else:
        out["sales_total_difference"] = pd.NA
        out["sales_total_matches_control"] = pd.NA

    return out


def _validate_sale_types_config(config: object, sale_types_json_path: Path) -> None:
    """
    Validate the minimal required structure of sale_types.json.
    """
    if not isinstance(config, dict):
        raise ConfigurationError(
            f"sale_types.json must contain a JSON object: {sale_types_json_path}"
        )

    required_top_level_keys = {
        "fixed_sale_types",
        "grouped_columns",
        "control_columns",
    }

    missing = required_top_level_keys - set(config.keys())
    if missing:
        raise ConfigurationError(
            f"sale_types.json is missing required keys: {sorted(missing)}"
        )

    if not isinstance(config["fixed_sale_types"], list):
        raise ConfigurationError("'fixed_sale_types' must be a list")

    if not isinstance(config["grouped_columns"], list):
        raise ConfigurationError("'grouped_columns' must be a list")

    if not isinstance(config["control_columns"], list):
        raise ConfigurationError("'control_columns' must be a list")

    validation_exclusions = config.get("validation_exclusions", [])
    if not isinstance(validation_exclusions, list):
        raise ConfigurationError("'validation_exclusions' must be a list")

    for item in config["fixed_sale_types"]:
        if not isinstance(item, dict):
            raise ConfigurationError("Each item in 'fixed_sale_types' must be an object")

        required_keys = {"source_column", "sale_type_code", "sale_type_name"}
        missing_item_keys = required_keys - set(item.keys())
        if missing_item_keys:
            raise ConfigurationError(
                f"Fixed sale type entry is missing keys: {sorted(missing_item_keys)}"
            )

    for item in config["grouped_columns"]:
        if not isinstance(item, dict):
            raise ConfigurationError("Each item in 'grouped_columns' must be an object")

        required_keys = {"target_column", "source_columns"}
        missing_item_keys = required_keys - set(item.keys())
        if missing_item_keys:
            raise ConfigurationError(
                f"Grouped column entry is missing keys: {sorted(missing_item_keys)}"
            )

        if not isinstance(item["source_columns"], list):
            raise ConfigurationError("'source_columns' must be a list")

    for item in config["control_columns"]:
        if not isinstance(item, dict):
            raise ConfigurationError("Each item in 'control_columns' must be an object")

        required_keys = {"source_column", "control_code", "description"}
        missing_item_keys = required_keys - set(item.keys())
        if missing_item_keys:
            raise ConfigurationError(
                f"Control column entry is missing keys: {sorted(missing_item_keys)}"
            )

    for item in validation_exclusions:
        if not isinstance(item, dict):
            raise ConfigurationError("Each item in 'validation_exclusions' must be an object")

        required_keys = {"column", "values", "description"}
        missing_item_keys = required_keys - set(item.keys())
        if missing_item_keys:
            raise ConfigurationError(
                f"Validation exclusion entry is missing keys: {sorted(missing_item_keys)}"
            )

        if not isinstance(item["values"], list):
            raise ConfigurationError("'values' in 'validation_exclusions' must be a list")


def _detect_percent_columns(df: pd.DataFrame) -> list[str]:
    """
    Detect dynamic discount columns such as 20%, 30%, or 40%.
    """
    return [col for col in df.columns if "%" in str(col)]


def _coerce_numeric_columns(df: pd.DataFrame, columns: list[str]) -> None:
    """
    Convert selected columns to numeric in place.
    Missing values remain NaN.
    """
    for col in columns:
        if col not in df.columns:
            continue

        df[col] = pd.to_numeric(df[col], errors="coerce")


def _get_computed_total_columns(
    df: pd.DataFrame,
    fixed_sale_types: list[dict],
    percent_columns: list[str],
    grouped_target_columns: list[str],
    grouped_source_columns_used: set[str],
) -> list[str]:
    """
    Return columns that should be used to compute total sales quantity.

    Rules:
    - use dynamic percentage columns
    - use grouped target columns
    - use fixed sale type source columns that are not consumed by grouped columns
    """
    computed_columns: list[str] = []

    for item in fixed_sale_types:
        source_column = item["source_column"]

        if source_column in grouped_source_columns_used:
            continue

        if source_column in df.columns:
            computed_columns.append(source_column)

    computed_columns.extend(percent_columns)
    computed_columns.extend(grouped_target_columns)

    return list(dict.fromkeys(computed_columns))


def _get_control_total_column(control_columns: list[dict]) -> str | None:
    """
    Return the control total column used for sales quantity validation.

    For the current version, one control total column is assumed,
    typically 'Suma'.
    """
    if not control_columns:
        return None

    return control_columns[0]["source_column"]


def _build_validation_exclusion_mask(
    df: pd.DataFrame,
    validation_exclusions: list[dict],
) -> pd.Series:
    """
    Build a boolean mask for rows excluded from sales total validation.

    Example use case:
    - Kod roboczy == '...technical code...' -> excluded from control comparison
    """
    exclusion_mask = pd.Series(False, index=df.index)

    for rule in validation_exclusions:
        column = rule["column"]
        values = rule["values"]

        if column not in df.columns:
            continue

        exclusion_mask |= df[column].isin(values)

    return exclusion_mask