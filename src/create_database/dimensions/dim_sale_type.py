from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from create_database.exceptions import ConfigurationError, DataValidationError
from create_database.utils.json_loader import load_json


def build_dim_sale_type(
    staging_df: pd.DataFrame,
    sale_types_json_path: Path,
) -> pd.DataFrame:
    """
    Build dim_sale_type from:
    - fixed sale types defined in sale_types.json
    - dynamic discount columns detected in staging_df (columns containing '%')

    Output columns:
    - sale_type_id
    - sale_type_code
    - sale_type_name
    - source_column
    - sale_type_kind
    - is_dynamic
    - discount_pct
    - is_active
    """

    if staging_df.empty:
        discount_df = pd.DataFrame()
    else:
        discount_df = _build_discount_sale_types_df(staging_df)

    config = load_json(sale_types_json_path)
    fixed_sale_types, grouped_columns = _validate_and_extract_config(config)

    fixed_df = _build_fixed_sale_types_df(
        fixed_sale_types=fixed_sale_types,
        grouped_columns=grouped_columns,
    )

    discount_df = _build_discount_sale_types_df(staging_df)

    dim_df = pd.concat([fixed_df, discount_df], ignore_index=True)

    if dim_df.empty:
        raise DataValidationError("dim_sale_type is empty.")

    if dim_df["sale_type_code"].duplicated().any():
        duplicates = (
            dim_df.loc[dim_df["sale_type_code"].duplicated(), "sale_type_code"]
            .drop_duplicates()
            .tolist()
        )
        raise DataValidationError(
            f"Duplicate sale_type_code values in dim_sale_type: {duplicates}"
        )

    dim_df = _sort_dim_sale_type(dim_df)
    dim_df = _assign_sale_type_ids(dim_df)

    dim_df["discount_pct"] = dim_df["discount_pct"].astype("Int64")
    dim_df["is_active"] = dim_df["is_active"].astype(bool)
    dim_df["is_dynamic"] = dim_df["is_dynamic"].astype(bool)

    return dim_df


def _validate_and_extract_config(config: object) -> tuple[list[dict], list[dict]]:
    """
    Validate the part of sale_types.json needed for dim_sale_type.
    """
    if not isinstance(config, dict):
        raise ConfigurationError("sale_types.json must contain a JSON object.")

    required_keys = {"fixed_sale_types", "grouped_columns"}
    missing = required_keys - set(config.keys())
    if missing:
        raise ConfigurationError(
            f"sale_types.json is missing required keys for dim_sale_type: {sorted(missing)}"
        )

    fixed_sale_types = config["fixed_sale_types"]
    grouped_columns = config["grouped_columns"]

    if not isinstance(fixed_sale_types, list):
        raise ConfigurationError("'fixed_sale_types' must be a list.")

    if not isinstance(grouped_columns, list):
        raise ConfigurationError("'grouped_columns' must be a list.")

    for item in fixed_sale_types:
        if not isinstance(item, dict):
            raise ConfigurationError(
                "Each item in 'fixed_sale_types' must be an object.")

        required_item_keys = {"source_column",
                              "sale_type_code", "sale_type_name"}
        missing_item_keys = required_item_keys - set(item.keys())
        if missing_item_keys:
            raise ConfigurationError(
                f"Fixed sale type entry is missing keys: {sorted(missing_item_keys)}"
            )

    for item in grouped_columns:
        if not isinstance(item, dict):
            raise ConfigurationError(
                "Each item in 'grouped_columns' must be an object.")

        required_item_keys = {"target_column", "source_columns"}
        missing_item_keys = required_item_keys - set(item.keys())
        if missing_item_keys:
            raise ConfigurationError(
                f"Grouped column entry is missing keys: {sorted(missing_item_keys)}"
            )

        if not isinstance(item["source_columns"], list):
            raise ConfigurationError(
                "'source_columns' in grouped_columns must be a list.")

    return fixed_sale_types, grouped_columns


def _build_fixed_sale_types_df(
    fixed_sale_types: list[dict],
    grouped_columns: list[dict],
) -> pd.DataFrame:
    """
    Build fixed sale type records.

    Important rule:
    - grouped target columns replace their underlying technical source columns
    """
    grouped_source_to_target: dict[str, str] = {}


    for rule in grouped_columns:
        target_column = rule["target_column"]

        for source_column in rule["source_columns"]:
            grouped_source_to_target[source_column] = target_column

    records: list[dict] = []
    seen_codes: set[str] = set()

    # Group fixed sale types by sale_type_code
    grouped_by_code: dict[str, list[dict]] = {}
    for item in fixed_sale_types:
        grouped_by_code.setdefault(item["sale_type_code"], []).append(item)

    for sale_type_code, items in grouped_by_code.items():
        sale_type_name = items[0]["sale_type_name"]

        source_columns = [item["source_column"] for item in items]
        mapped_targets = [
            grouped_source_to_target.get(source_column, source_column)
            for source_column in source_columns
        ]

        # Use the first unique mapped target as the final source column representation
        unique_targets = list(dict.fromkeys(mapped_targets))
        source_column = unique_targets[0]

        if sale_type_code in seen_codes:
            continue

        seen_codes.add(sale_type_code)

        records.append(
            {
                "sale_type_code": sale_type_code,
                "sale_type_name": sale_type_name,
                "source_column": source_column,
                "sale_type_kind": "FIXED",
                "is_dynamic": False,
                "discount_pct": pd.NA,
                "is_active": True,
            }
        )

    return pd.DataFrame(records)


def _build_discount_sale_types_df(staging_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build discount sale type records from dynamic staging columns containing '%'.
    """
    percent_columns = [col for col in staging_df.columns if "%" in str(col)]

    records: list[dict] = []

    for column in sorted(percent_columns, key=_extract_discount_pct_for_sort):
        discount_pct = _extract_discount_pct(column)

        if discount_pct is None:
            raise DataValidationError(
                f"Could not parse discount percentage from column: {column}"
            )

        records.append(
            {
                "sale_type_code": f"DISCOUNT_{discount_pct}",
                "sale_type_name": column,
                "source_column": column,
                "sale_type_kind": "DISCOUNT",
                "is_dynamic": True,
                "discount_pct": discount_pct,
                "is_active": True,
            }
        )

    return pd.DataFrame(records)


def _extract_discount_pct(column_name: str) -> int | None:
    """
    Extract numeric discount percentage from strings like '30%'.
    """
    match = re.search(r"(\d+)\s*%", str(column_name))
    if not match:
        return None

    return int(match.group(1))


def _extract_discount_pct_for_sort(column_name: str) -> tuple[int, str]:
    """
    Sorting helper for percentage columns.
    """
    pct = _extract_discount_pct(column_name)
    if pct is None:
        return (9999, str(column_name))

    return (pct, str(column_name))


def _sort_dim_sale_type(dim_df: pd.DataFrame) -> pd.DataFrame:
    """
    Sort dim_sale_type in readable logical order:
    1. FULL_PRICE
    2. TGTG
    3. DISCOUNT_* by percentage ascending
    4. everything else
    """
    out = dim_df.copy()

    def build_sort_key(row: pd.Series) -> tuple[int, int, str]:
        code = row["sale_type_code"]
        discount_pct = row["discount_pct"]

        if code == "FULL_PRICE":
            return (1, 0, code)

        if code == "TGTG":
            return (2, 0, code)

        if str(code).startswith("DISCOUNT_"):
            pct = 9999 if pd.isna(discount_pct) else int(discount_pct)
            return (3, pct, code)

        return (9, 0, code)

    out["_sort_key"] = out.apply(build_sort_key, axis=1)
    out = out.sort_values("_sort_key", kind="mergesort").drop(
        columns="_sort_key")
    out = out.reset_index(drop=True)

    return out


def _assign_sale_type_ids(dim_df: pd.DataFrame) -> pd.DataFrame:
    """
    Assign sequential technical IDs after final sort.
    """
    out = dim_df.copy()
    out.insert(0, "sale_type_id", range(1, len(out) + 1))
    return out
