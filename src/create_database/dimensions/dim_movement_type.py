from __future__ import annotations

from pathlib import Path

import pandas as pd

from create_database.exceptions import ConfigurationError, DataValidationError
from create_database.utils.json_loader import load_json


ALLOWED_MOVEMENT_DIRECTIONS = {"IN", "OUT"}


def build_dim_movement_type(movement_types_json_path: Path) -> pd.DataFrame:
    """
    Build dim_movement_type from movement_types.json.

    Output columns:
    - movement_type_id
    - movement_type_column
    - movement_type_name
    - movement_direction
    - is_additional_detail
    - parent_movement_type_column
    """
    movement_types_data = load_json(movement_types_json_path)
    _validate_movement_types_json(movement_types_data, movement_types_json_path)

    dim_df = pd.DataFrame(movement_types_data).copy()

    if dim_df.empty:
        raise DataValidationError(
            "dim_movement_type cannot be built from empty movement_types.json."
        )

    dim_df["movement_type_column"] = dim_df["movement_type_column"].map(
        _normalize_required_text
    )
    dim_df["movement_type_name"] = dim_df["movement_type_name"].map(
        _normalize_required_text
    )
    dim_df["movement_direction"] = dim_df["movement_direction"].map(
        _normalize_required_text
    )
    dim_df["parent_movement_type_column"] = dim_df["parent_movement_type_column"].map(
        _normalize_nullable_text
    )

    dim_df["is_additional_detail"] = dim_df["is_additional_detail"].astype(bool)

    _validate_dim_movement_type_dataframe(dim_df)

    dim_df = _sort_dim_movement_type(dim_df)
    dim_df.insert(0, "movement_type_id", range(1, len(dim_df) + 1))

    dim_df = dim_df.loc[
        :,
        [
            "movement_type_id",
            "movement_type_column",
            "movement_type_name",
            "movement_direction",
            "is_additional_detail",
            "parent_movement_type_column",
        ],
    ].copy()

    return dim_df


def _validate_movement_types_json(
    movement_types_data: object,
    movement_types_json_path: Path,
) -> None:
    """
    Validate structure of movement_types.json.
    """
    if not isinstance(movement_types_data, list):
        raise ConfigurationError(
            f"movement_types.json must contain a list of objects: {movement_types_json_path}"
        )

    required_keys = {
        "movement_type_column",
        "movement_type_name",
        "movement_direction",
        "is_additional_detail",
        "parent_movement_type_column",
    }

    for idx, item in enumerate(movement_types_data):
        if not isinstance(item, dict):
            raise ConfigurationError(
                f"Entry #{idx} in movement_types.json must be an object."
            )

        missing = required_keys - set(item.keys())
        if missing:
            raise ConfigurationError(
                f"Entry #{idx} in movement_types.json is missing keys: {sorted(missing)}"
            )


def _validate_dim_movement_type_dataframe(dim_df: pd.DataFrame) -> None:
    """
    Validate normalized dim_movement_type dataframe before assigning IDs.
    """
    if dim_df["movement_type_column"].isna().any():
        raise DataValidationError("Some movement_type_column values are empty.")

    if dim_df["movement_type_name"].isna().any():
        raise DataValidationError("Some movement_type_name values are empty.")

    if dim_df["movement_direction"].isna().any():
        raise DataValidationError("Some movement_direction values are empty.")

    invalid_directions = sorted(
        set(dim_df["movement_direction"].dropna()) - ALLOWED_MOVEMENT_DIRECTIONS
    )
    if invalid_directions:
        raise DataValidationError(
            f"Invalid movement_direction values: {invalid_directions}. "
            f"Allowed values: {sorted(ALLOWED_MOVEMENT_DIRECTIONS)}"
        )

    if dim_df["movement_type_column"].duplicated().any():
        duplicates = (
            dim_df.loc[
                dim_df["movement_type_column"].duplicated(),
                "movement_type_column",
            ]
            .drop_duplicates()
            .tolist()
        )
        raise DataValidationError(
            f"Duplicate movement_type_column values in movement_types.json: {duplicates}"
        )

    existing_columns = set(dim_df["movement_type_column"].dropna())
    parent_columns = set(dim_df["parent_movement_type_column"].dropna())

    missing_parents = sorted(parent_columns - existing_columns)
    if missing_parents:
        raise DataValidationError(
            f"parent_movement_type_column points to missing movement_type_column values: {missing_parents}"
        )


def _sort_dim_movement_type(dim_df: pd.DataFrame) -> pd.DataFrame:
    """
    Sort dim_movement_type deterministically.

    Recommended order:
    1. non-additional records
    2. additional detail records
    3. then by movement_direction
    4. then by movement_type_name
    """
    out = dim_df.copy()

    out = out.sort_values(
        by=[
            "is_additional_detail",
            "movement_direction",
            "movement_type_name",
            "movement_type_column",
        ],
        ascending=[True, True, True, True],
        kind="mergesort",
    ).reset_index(drop=True)

    return out


def _normalize_required_text(value: object) -> object:
    """
    Normalize required text value.
    Empty values are not allowed.
    """
    if pd.isna(value):
        return pd.NA

    text = " ".join(str(value).strip().split())

    if not text:
        return pd.NA

    return text


def _normalize_nullable_text(value: object) -> object:
    """
    Normalize nullable text value.
    Empty values become pd.NA.
    """
    if pd.isna(value):
        return pd.NA

    text = " ".join(str(value).strip().split())

    if not text:
        return pd.NA

    return text