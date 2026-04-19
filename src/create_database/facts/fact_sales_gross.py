from __future__ import annotations

import logging

import pandas as pd

from create_database.exceptions import DataValidationError


log = logging.getLogger(__name__)

REQUIRED_STAGING_COLUMNS = {
    "report_date",
    "spot_id_raw",
    "product_work_key",
    "is_heated",
    "Brutto",
}

REQUIRED_PRODUCT_IDENTITY_COLUMNS = {
    "product_work_key",
    "product_business_key",
}

REQUIRED_DIM_PRODUCT_COLUMNS = {
    "product_business_key",
    "product_id",
}

REQUIRED_DIM_SPOT_COLUMNS = {
    "spot_id_raw",
    "spot_id",
}


def build_fact_sales_gross(
    staging_df: pd.DataFrame,
    product_identity_df: pd.DataFrame,
    dim_product_df: pd.DataFrame,
    dim_spot_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build fact_sales_gross from staging data and dimension tables.

    Final grain:
    - date
    - spot_id
    - product_id
    - is_heated

    Measure:
    - amount
    """
    _validate_fact_sales_gross_inputs(
        staging_df=staging_df,
        product_identity_df=product_identity_df,
        dim_product_df=dim_product_df,
        dim_spot_df=dim_spot_df,
    )

    fact_df = _prepare_sales_gross_base(staging_df)
    fact_df = _attach_product_business_key(
        fact_df=fact_df,
        product_identity_df=product_identity_df,
    )
    fact_df = _attach_product_id(
        fact_df=fact_df,
        dim_product_df=dim_product_df,
    )
    fact_df = _attach_spot_id(
        fact_df=fact_df,
        dim_spot_df=dim_spot_df,
    )

    fact_df = _aggregate_fact_sales_gross(fact_df)
    _validate_final_fact_sales_gross(fact_df)

    return fact_df


def _validate_fact_sales_gross_inputs(
    staging_df: pd.DataFrame,
    product_identity_df: pd.DataFrame,
    dim_product_df: pd.DataFrame,
    dim_spot_df: pd.DataFrame,
) -> None:
    missing_staging = REQUIRED_STAGING_COLUMNS - set(staging_df.columns)
    if missing_staging:
        raise DataValidationError(
            f"fact_sales_gross staging input is missing columns: {sorted(missing_staging)}"
        )

    missing_product_identity = REQUIRED_PRODUCT_IDENTITY_COLUMNS - set(
        product_identity_df.columns
    )
    if missing_product_identity:
        raise DataValidationError(
            "fact_sales_gross product_identity input is missing columns: "
            f"{sorted(missing_product_identity)}"
        )

    missing_dim_product = REQUIRED_DIM_PRODUCT_COLUMNS - set(dim_product_df.columns)
    if missing_dim_product:
        raise DataValidationError(
            f"fact_sales_gross dim_product input is missing columns: {sorted(missing_dim_product)}"
        )

    missing_dim_spot = REQUIRED_DIM_SPOT_COLUMNS - set(dim_spot_df.columns)
    if missing_dim_spot:
        raise DataValidationError(
            f"fact_sales_gross dim_spot input is missing columns: {sorted(missing_dim_spot)}"
        )


def _prepare_sales_gross_base(staging_df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare minimal base DataFrame for fact_sales_gross.
    """
    base_columns = [
        "report_date",
        "spot_id_raw",
        "product_work_key",
        "is_heated",
        "Brutto",
    ]

    fact_df = staging_df.loc[:, base_columns].copy()

    fact_df["Brutto"] = pd.to_numeric(fact_df["Brutto"], errors="coerce")
    fact_df = fact_df.loc[fact_df["Brutto"].notna()].copy()
    fact_df = fact_df.loc[fact_df["Brutto"] != 0].copy()

    fact_df["report_date"] = pd.to_datetime(
        fact_df["report_date"],
        errors="coerce",
    ).dt.date

    if fact_df["report_date"].isna().any():
        raise DataValidationError(
            "fact_sales_gross contains invalid report_date values."
        )

    return fact_df.reset_index(drop=True)


def _attach_product_business_key(
    fact_df: pd.DataFrame,
    product_identity_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Attach product_business_key using product_work_key.
    """
    identity_map_df = (
        product_identity_df.loc[:, ["product_work_key", "product_business_key"]]
        .drop_duplicates(subset=["product_work_key"])
        .copy()
    )

    out = fact_df.merge(
        identity_map_df,
        how="left",
        on="product_work_key",
        validate="m:1",
    )

    if out["product_business_key"].isna().any():
        missing_keys = (
            out.loc[out["product_business_key"].isna(), "product_work_key"]
            .drop_duplicates()
            .tolist()
        )
        raise DataValidationError(
            "Some fact_sales_gross rows have no product_business_key mapping "
            f"for product_work_key values: {missing_keys[:20]}"
        )

    return out


def _attach_product_id(
    fact_df: pd.DataFrame,
    dim_product_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Attach product_id using product_business_key.
    """
    product_map_df = (
        dim_product_df.loc[:, ["product_business_key", "product_id"]]
        .drop_duplicates(subset=["product_business_key"])
        .copy()
    )

    out = fact_df.merge(
        product_map_df,
        how="left",
        on="product_business_key",
        validate="m:1",
    )

    if out["product_id"].isna().any():
        missing_keys = (
            out.loc[out["product_id"].isna(), "product_business_key"]
            .drop_duplicates()
            .tolist()
        )
        raise DataValidationError(
            "Some fact_sales_gross rows have no product_id mapping "
            f"for product_business_key values: {missing_keys[:20]}"
        )

    out["product_id"] = out["product_id"].astype(int)

    return out


def _attach_spot_id(
    fact_df: pd.DataFrame,
    dim_spot_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Attach spot_id using spot_id_raw.
    """
    spot_map_df = (
        dim_spot_df.loc[:, ["spot_id_raw", "spot_id"]]
        .drop_duplicates(subset=["spot_id_raw"])
        .copy()
    )

    out = fact_df.merge(
        spot_map_df,
        how="left",
        on="spot_id_raw",
        validate="m:1",
    )

    if out["spot_id"].isna().any():
        missing_keys = (
            out.loc[out["spot_id"].isna(), "spot_id_raw"]
            .drop_duplicates()
            .tolist()
        )
        raise DataValidationError(
            "Some fact_sales_gross rows have no spot_id mapping "
            f"for spot_id_raw values: {missing_keys[:20]}"
        )

    out["spot_id"] = out["spot_id"].astype(int)

    return out


def _aggregate_fact_sales_gross(fact_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate rows to final grain.
    """
    pre_agg_sum = pd.to_numeric(fact_df["Brutto"], errors="coerce").fillna(0).sum()

    out = (
        fact_df.groupby(
            ["report_date", "spot_id", "product_id", "is_heated"],
            dropna=False,
            as_index=False,
        )["Brutto"]
        .sum()
        .rename(
            columns={
                "report_date": "date",
                "Brutto": "amount",
            }
        )
    )

    post_agg_sum = pd.to_numeric(out["amount"], errors="coerce").fillna(0).sum()

    if round(float(pre_agg_sum), 6) != round(float(post_agg_sum), 6):
        raise DataValidationError(
            f"fact_sales_gross aggregation changed total amount: "
            f"before={pre_agg_sum}, after={post_agg_sum}"
        )

    out["amount"] = pd.to_numeric(out["amount"], errors="coerce")

    return out


def _validate_final_fact_sales_gross(fact_df: pd.DataFrame) -> None:
    """
    Final validation of fact_sales_gross output.
    """
    required_columns = {
        "date",
        "spot_id",
        "product_id",
        "is_heated",
        "amount",
    }

    missing = required_columns - set(fact_df.columns)
    if missing:
        raise DataValidationError(
            f"fact_sales_gross output is missing required columns: {sorted(missing)}"
        )

    if fact_df.empty:
        log.warning("fact_sales_gross is empty after transformation.")
        return

    null_columns = ["date", "spot_id", "product_id", "is_heated", "amount"]
    for col in null_columns:
        if fact_df[col].isna().any():
            raise DataValidationError(
                f"fact_sales_gross output contains null values in column: '{col}'"
            )

    duplicated_mask = fact_df.duplicated(
        subset=["date", "spot_id", "product_id", "is_heated"],
        keep=False,
    )
    if duplicated_mask.any():
        raise DataValidationError(
            "fact_sales_gross output contains duplicate rows for final grain."
        )

    if (fact_df["amount"] < 0).any():
        log.warning("fact_sales_gross contains negative amount values.")