from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from create_database.exceptions import DataValidationError
from create_database.utils.json_loader import load_json


log = logging.getLogger(__name__)

RED = "\033[91m"
RESET = "\033[0m"


REQUIRED_SPOT_COLUMNS = {
    "spot_id_raw",
    "spot_name",
    "city",
    "street",
}


def map_spots(df: pd.DataFrame, spots_json_path: Path) -> pd.DataFrame:
    """
    Enrich staging data with spot attributes based on spot_id_raw.

    Uses spots.json as a lookup source.

    Adds:
    - spot_name
    - city
    - street
    - spot_mapping_found
    """
    if df.empty:
        return df.copy()

    out = df.copy()

    if "spot_id_raw" not in out.columns:
        raise DataValidationError("map_spots input is missing required column: 'spot_id_raw'")

    spots_data = load_json(spots_json_path)

    if not isinstance(spots_data, list):
        raise DataValidationError(f"Expected a list in spots JSON: {spots_json_path}")

    spots_df = pd.DataFrame(spots_data)

    if spots_df.empty:
        for col in ["spot_name", "city", "street"]:
            out[col] = pd.NA
        out["spot_mapping_found"] = False
        return out

    missing = REQUIRED_SPOT_COLUMNS - set(spots_df.columns)
    if missing:
        raise DataValidationError(
            f"spots JSON is missing required columns: {sorted(missing)}"
        )

    spots_df = spots_df.loc[:, ["spot_id_raw", "spot_name", "city", "street"]].copy()
    spots_df["spot_id_raw"] = pd.to_numeric(spots_df["spot_id_raw"], errors="coerce")

    out["spot_id_raw"] = pd.to_numeric(out["spot_id_raw"], errors="coerce")

    out = out.merge(
        spots_df,
        how="left",
        on="spot_id_raw",
        validate="m:1",
    )

    out["spot_mapping_found"] = out["spot_name"].notna()

    missing_mask = ~out["spot_mapping_found"]

    if missing_mask.any():
        missing_spots = (
            out.loc[missing_mask, "spot_id_raw"]
            .dropna()
            .unique()
            .tolist()
        )

        log.error(
            "%sMissing spot mapping for spot_id_raw values: %s%s",
            RED,
            missing_spots,
            RESET,
        )

        raise DataValidationError(
            f"Missing spot mapping for spot_id_raw values: {missing_spots}"
        )

    return out