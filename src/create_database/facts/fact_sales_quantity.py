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
}

REQUIRED_PRODUCT_IDENTITY_COLUMNS = {
    "product_work_key",
    "product_business_key",
}

REQUIRED_DIM_PRODUCT_COLUMNS = {
    "product_business_key",
    "product_id",
}

REQUIRED_DIM_SALE_TYPE_COLUMNS = {
    "sale_type_id",
    "source_column",
}

REQUIRED_DIM_SPOT_COLUMNS = {
    "spot_id_raw",
    "spot_id",
}


def build_fact_sales_quantity(
    staging_df: pd.DataFrame,
    product_identity_df: pd.DataFrame,
    dim_product_df: pd.DataFrame,
    dim_sale_type_df: pd.DataFrame,
    dim_spot_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build fact_sales_quantity from staging data and dimension tables.

    Final grain:
    - date
    - spot_id
    - product_id
    - sale_type_id
    - is_heated

    Measure:
    - quantity
    """
    _validate_fact_sales_quantity_inputs(
        staging_df=staging_df,
        product_identity_df=product_identity_df,
        dim_product_df=dim_product_df,
        dim_sale_type_df=dim_sale_type_df,
        dim_spot_df=dim_spot_df,
    )

    sales_columns = _get_sales_quantity_columns(
        staging_df=staging_df,
        dim_sale_type_df=dim_sale_type_df,
    )

    if not sales_columns:
        raise DataValidationError(
            "No sales quantity columns found in staging_df based on dim_sale_type source_column."
        )

    fact_df = _melt_sales_quantity(
        staging_df=staging_df,
        sales_columns=sales_columns,
    )

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

    fact_df = _attach_sale_type_id(
        fact_df=fact_df,
        dim_sale_type_df=dim_sale_type_df,
    )

    fact_df = _aggregate_fact_sales_quantity(fact_df)
    _validate_final_fact_sales_quantity(fact_df)

    return fact_df


def _validate_fact_sales_quantity_inputs(
    staging_df: pd.DataFrame,
    product_identity_df: pd.DataFrame,
    dim_product_df: pd.DataFrame,
    dim_sale_type_df: pd.DataFrame,
    dim_spot_df: pd.DataFrame,
) -> None:
    missing_staging = REQUIRED_STAGING_COLUMNS - set(staging_df.columns)
    if missing_staging:
        raise DataValidationError(
            f"fact_sales_quantity staging input is missing columns: {sorted(missing_staging)}"
        )

    missing_product_identity = REQUIRED_PRODUCT_IDENTITY_COLUMNS - set(
        product_identity_df.columns
    )
    if missing_product_identity:
        raise DataValidationError(
            "fact_sales_quantity product_identity input is missing columns: "
            f"{sorted(missing_product_identity)}"
        )

    missing_dim_product = REQUIRED_DIM_PRODUCT_COLUMNS - set(dim_product_df.columns)
    if missing_dim_product:
        raise DataValidationError(
            f"fact_sales_quantity dim_product input is missing columns: {sorted(missing_dim_product)}"
        )

    missing_dim_sale_type = REQUIRED_DIM_SALE_TYPE_COLUMNS - set(dim_sale_type_df.columns)
    if missing_dim_sale_type:
        raise DataValidationError(
            "fact_sales_quantity dim_sale_type input is missing columns: "
            f"{sorted(missing_dim_sale_type)}"
        )

    missing_dim_spot = REQUIRED_DIM_SPOT_COLUMNS - set(dim_spot_df.columns)
    if missing_dim_spot:
        raise DataValidationError(
            f"fact_sales_quantity dim_spot input is missing columns: {sorted(missing_dim_spot)}"
        )


def _get_sales_quantity_columns(
    staging_df: pd.DataFrame,
    dim_sale_type_df: pd.DataFrame,
) -> list[str]:
    """
    Get sales quantity columns from dim_sale_type.source_column
    that actually exist in staging_df.

    Columns like 'Suma' should not be present in dim_sale_type,
    so this function stays generic and dimension-driven.
    """
    source_columns = (
        dim_sale_type_df["source_column"]
        .dropna()
        .astype(str)
        .str.strip()
        .tolist()
    )

    existing_columns = [col for col in source_columns if col in staging_df.columns]

    # keep order, remove duplicates
    return list(dict.fromkeys(existing_columns))


def _melt_sales_quantity(
    staging_df: pd.DataFrame,
    sales_columns: list[str],
) -> pd.DataFrame:
    """
    Transform wide sales quantity columns into long fact-ready rows.
    """
    base_columns = [
        "report_date",
        "spot_id_raw",
        "product_work_key",
        "is_heated",
    ]

    fact_df = staging_df.loc[:, base_columns + sales_columns].copy()

    fact_df = fact_df.melt(
        id_vars=base_columns,
        value_vars=sales_columns,
        var_name="sale_type_source_column",
        value_name="quantity",
    )

    fact_df["quantity"] = pd.to_numeric(fact_df["quantity"], errors="coerce")
    fact_df = fact_df.loc[fact_df["quantity"].notna()].copy()
    fact_df = fact_df.loc[fact_df["quantity"] != 0].copy()

    fact_df["report_date"] = pd.to_datetime(fact_df["report_date"], errors="coerce").dt.date

    if fact_df["report_date"].isna().any():
        raise DataValidationError(
            "fact_sales_quantity contains invalid report_date values after melt."
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
            "Some fact_sales_quantity rows have no product_business_key mapping "
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
            "Some fact_sales_quantity rows have no product_id mapping "
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
            "Some fact_sales_quantity rows have no spot_id mapping "
            f"for spot_id_raw values: {missing_keys[:20]}"
        )

    out["spot_id"] = out["spot_id"].astype(int)

    return out


def _attach_sale_type_id(
    fact_df: pd.DataFrame,
    dim_sale_type_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Attach sale_type_id using source_column.
    """
    sale_type_map_df = (
        dim_sale_type_df.loc[:, ["source_column", "sale_type_id"]]
        .dropna(subset=["source_column"])
        .drop_duplicates(subset=["source_column"])
        .copy()
    )

    out = fact_df.merge(
        sale_type_map_df,
        how="left",
        left_on="sale_type_source_column",
        right_on="source_column",
        validate="m:1",
    )

    if out["sale_type_id"].isna().any():
        missing_columns = (
            out.loc[out["sale_type_id"].isna(), "sale_type_source_column"]
            .drop_duplicates()
            .tolist()
        )
        raise DataValidationError(
            "Some fact_sales_quantity rows have no sale_type_id mapping "
            f"for source columns: {missing_columns[:20]}"
        )

    out["sale_type_id"] = out["sale_type_id"].astype(int)

    return out


def _aggregate_fact_sales_quantity(fact_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate rows to final grain.
    """
    pre_agg_sum = pd.to_numeric(fact_df["quantity"], errors="coerce").fillna(0).sum()

    out = (
        fact_df.groupby(
            ["report_date", "spot_id", "product_id", "sale_type_id", "is_heated"],
            dropna=False,
            as_index=False,
        )["quantity"]
        .sum()
        .rename(columns={"report_date": "date"})
    )

    post_agg_sum = pd.to_numeric(out["quantity"], errors="coerce").fillna(0).sum()

    if round(float(pre_agg_sum), 6) != round(float(post_agg_sum), 6):
        raise DataValidationError(
            f"fact_sales_quantity aggregation changed total quantity: "
            f"before={pre_agg_sum}, after={post_agg_sum}"
        )

    out["quantity"] = pd.to_numeric(out["quantity"], errors="coerce")

    return out

def _validate_final_fact_sales_quantity(fact_df: pd.DataFrame) -> None:
    """
    Final validation of fact_sales_quantity output.
    """
    required_columns = {
        "date",
        "spot_id",
        "product_id",
        "sale_type_id",
        "is_heated",
        "quantity",
    }

    missing = required_columns - set(fact_df.columns)
    if missing:
        raise DataValidationError(
            f"fact_sales_quantity output is missing required columns: {sorted(missing)}"
        )

    if fact_df.empty:
        log.warning("fact_sales_quantity is empty after transformation.")
        return

    null_columns = ["date", "spot_id", "product_id", "sale_type_id", "is_heated", "quantity"]
    for col in null_columns:
        if fact_df[col].isna().any():
            raise DataValidationError(
                f"fact_sales_quantity output contains null values in column: '{col}'"
            )

    duplicated_mask = fact_df.duplicated(
        subset=["date", "spot_id", "product_id", "sale_type_id", "is_heated"],
        keep=False,
    )
    if duplicated_mask.any():
        raise DataValidationError(
            "fact_sales_quantity output contains duplicate rows for final grain."
        )

    if (fact_df["quantity"] < 0).any():
        log.warning("fact_sales_quantity contains negative quantity values.")