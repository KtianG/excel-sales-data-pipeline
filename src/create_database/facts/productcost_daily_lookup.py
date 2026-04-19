from __future__ import annotations

import logging

import pandas as pd

from create_database.exceptions import DataValidationError


log = logging.getLogger(__name__)

REQUIRED_FACT_PRODUCTCOST_COLUMNS = {
    "date",
    "product_id",
    "productcost",
}

REQUIRED_DIM_DATE_COLUMNS = {
    "date",
}


def build_productcost_daily_lookup(
    fact_productcost_df: pd.DataFrame,
    dim_date_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build daily productcost lookup table from fact_productcost and dim_date.

    Rules:
    - for each product_id, use known product cost changes from fact_productcost
    - expand to daily rows using dim_date
    - forward-fill product cost from the last known value
    - do not create rows before the first known product cost date for a product

    Output columns:
    - date
    - product_id
    - productcost
    """
    _validate_productcost_daily_lookup_inputs(
        fact_productcost_df=fact_productcost_df,
        dim_date_df=dim_date_df,
    )

    productcost_df = fact_productcost_df.loc[:, ["date", "product_id", "productcost"]].copy()
    date_df = dim_date_df.loc[:, ["date"]].copy()

    productcost_df["date"] = pd.to_datetime(productcost_df["date"], errors="coerce").dt.date
    date_df["date"] = pd.to_datetime(date_df["date"], errors="coerce").dt.date

    if productcost_df["date"].isna().any():
        raise DataValidationError(
            "productcost_daily_lookup input fact_productcost contains invalid date values."
        )

    if date_df["date"].isna().any():
        raise DataValidationError(
            "productcost_daily_lookup input dim_date contains invalid date values."
        )

    if productcost_df.duplicated(subset=["date", "product_id"], keep=False).any():
        raise DataValidationError(
            "fact_productcost contains duplicate rows for grain date + product_id."
        )

    all_dates = sorted(date_df["date"].dropna().unique().tolist())
    if not all_dates:
        raise DataValidationError("dim_date does not contain any valid dates.")

    max_dim_date = max(all_dates)

    result_frames: list[pd.DataFrame] = []

    grouped = productcost_df.groupby("product_id", dropna=False)

    for product_id, group in grouped:
        product_df = group.sort_values("date", kind="mergesort").copy()

        first_known_date = product_df["date"].min()
        if pd.isna(first_known_date):
            continue

        applicable_dates = [d for d in all_dates if first_known_date <= d <= max_dim_date]
        if not applicable_dates:
            continue

        product_calendar_df = pd.DataFrame(
            {
                "date": applicable_dates,
                "product_id": product_id,
            }
        )

        merged_df = product_calendar_df.merge(
            product_df.loc[:, ["date", "product_id", "productcost"]],
            how="left",
            on=["date", "product_id"],
            validate="1:1",
        )

        merged_df = merged_df.sort_values("date", kind="mergesort").reset_index(drop=True)
        merged_df["productcost"] = merged_df["productcost"].ffill()

        merged_df = merged_df.loc[merged_df["productcost"].notna()].copy()

        result_frames.append(merged_df)

    if not result_frames:
        log.warning("productcost_daily_lookup is empty after transformation.")
        return pd.DataFrame(columns=["date", "product_id", "productcost"])

    lookup_df = pd.concat(result_frames, axis=0, ignore_index=True)

    lookup_df["product_id"] = pd.to_numeric(lookup_df["product_id"], errors="coerce")
    lookup_df["productcost"] = pd.to_numeric(lookup_df["productcost"], errors="coerce")

    _validate_final_productcost_daily_lookup(lookup_df)

    lookup_df = lookup_df.sort_values(
        by=["product_id", "date"],
        ascending=[True, True],
        kind="mergesort",
    ).reset_index(drop=True)

    lookup_df = lookup_df.loc[:, ["date", "product_id", "productcost"]].copy()

    return lookup_df


def _validate_productcost_daily_lookup_inputs(
    fact_productcost_df: pd.DataFrame,
    dim_date_df: pd.DataFrame,
) -> None:
    missing_fact_productcost = REQUIRED_FACT_PRODUCTCOST_COLUMNS - set(fact_productcost_df.columns)
    if missing_fact_productcost:
        raise DataValidationError(
            "productcost_daily_lookup fact_productcost input is missing columns: "
            f"{sorted(missing_fact_productcost)}"
        )

    missing_dim_date = REQUIRED_DIM_DATE_COLUMNS - set(dim_date_df.columns)
    if missing_dim_date:
        raise DataValidationError(
            f"productcost_daily_lookup dim_date input is missing columns: {sorted(missing_dim_date)}"
        )


def _validate_final_productcost_daily_lookup(lookup_df: pd.DataFrame) -> None:
    """
    Final validation of productcost_daily_lookup output.
    """
    required_columns = {"date", "product_id", "productcost"}
    missing = required_columns - set(lookup_df.columns)
    if missing:
        raise DataValidationError(
            f"productcost_daily_lookup output is missing required columns: {sorted(missing)}"
        )

    if lookup_df.empty:
        return

    null_columns = ["date", "product_id", "productcost"]
    for col in null_columns:
        if lookup_df[col].isna().any():
            raise DataValidationError(
                f"productcost_daily_lookup output contains null values in column: '{col}'"
            )

    duplicated_mask = lookup_df.duplicated(
        subset=["date", "product_id"],
        keep=False,
    )
    if duplicated_mask.any():
        raise DataValidationError(
            "productcost_daily_lookup output contains duplicate rows for grain date + product_id."
        )

    if (lookup_df["productcost"] < 0).any():
        log.warning("productcost_daily_lookup contains negative product cost values.")