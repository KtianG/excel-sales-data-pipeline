from __future__ import annotations

from pathlib import Path

import pandas as pd

from create_database.exceptions import ConfigurationError, DataValidationError
from create_database.utils.json_loader import load_json


def build_dim_spot(spots_json_path: Path) -> pd.DataFrame:
    """
    Build dim_spot from spots.json.

    Output columns:
    - spot_id
    - spot_id_raw
    - spot_name
    - address
    - city
    """
    spots_data = load_json(spots_json_path)
    _validate_spots_json(spots_data, spots_json_path)

    dim_df = pd.DataFrame(spots_data).copy()

    if dim_df.empty:
        raise DataValidationError("dim_spot cannot be built from empty spots.json.")

    dim_df["spot_id_raw"] = pd.to_numeric(dim_df["spot_id_raw"], errors="coerce")

    if dim_df["spot_id_raw"].isna().any():
        raise DataValidationError("Some spot_id_raw values in spots.json are not numeric.")

    if dim_df["spot_id_raw"].duplicated().any():
        duplicates = (
            dim_df.loc[dim_df["spot_id_raw"].duplicated(), "spot_id_raw"]
            .drop_duplicates()
            .tolist()
        )
        raise DataValidationError(
            f"Duplicate spot_id_raw values in spots.json: {duplicates}"
        )

    dim_df["spot_name"] = dim_df["spot_name"].map(_normalize_nullable_text)
    dim_df["city"] = dim_df["city"].map(_normalize_nullable_text)
    dim_df["street"] = dim_df["street"].map(_normalize_nullable_text)

    dim_df = dim_df.rename(columns={"street": "address"})
    dim_df = dim_df.loc[:, ["spot_id_raw", "spot_name", "address", "city"]].copy()

    dim_df = dim_df.sort_values("spot_id_raw", kind="mergesort").reset_index(drop=True)
    dim_df.insert(0, "spot_id", range(1, len(dim_df) + 1))

    return dim_df


def _validate_spots_json(spots_data: object, spots_json_path: Path) -> None:
    """
    Validate structure of spots.json for dim_spot.
    """
    if not isinstance(spots_data, list):
        raise ConfigurationError(
            f"spots.json must contain a list of objects: {spots_json_path}"
        )

    required_keys = {"spot_id_raw", "spot_name", "city", "street"}

    for idx, item in enumerate(spots_data):
        if not isinstance(item, dict):
            raise ConfigurationError(f"Entry #{idx} in spots.json must be an object.")

        missing = required_keys - set(item.keys())
        if missing:
            raise ConfigurationError(
                f"Entry #{idx} in spots.json is missing keys: {sorted(missing)}"
            )


def _normalize_nullable_text(value: object) -> str | pd.NA:
    """
    Normalize text fields while preserving missing values.
    Empty strings become pd.NA.
    """
    if pd.isna(value):
        return pd.NA

    text = " ".join(str(value).strip().split())

    if not text:
        return pd.NA

    return text