from __future__ import annotations

import logging

import pandas as pd

from create_database.exceptions import DataValidationError


log = logging.getLogger(__name__)

YELLOW = "\033[93m"
RESET = "\033[0m"

REQUIRED_STAGING_COLUMNS = {
    "report_date",
    "spot_id_raw",
    "product_work_key",
    "koszt-przyjęcie",
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

REQUIRED_DIM_MOVEMENT_TYPE_COLUMNS = {
    "movement_type_id",
    "movement_type_column",
    "movement_direction",
    "is_additional_detail",
}

REQUIRED_PRODUCTCOST_LOOKUP_COLUMNS = {
    "date",
    "product_id",
    "productcost",
}


def build_fact_inventory_movement(
    staging_df: pd.DataFrame,
    product_identity_df: pd.DataFrame,
    dim_product_df: pd.DataFrame,
    dim_spot_df: pd.DataFrame,
    dim_movement_type_df: pd.DataFrame,
    productcost_daily_lookup_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build fact_inventory_movement from staging data and dimension tables.

    Rules:
    - movement quantity comes from movement columns defined in dim_movement_type
    - cost for 'Przyjęto' comes directly from staging column 'koszt-przyjęcie'
    - cost for all other movement types is calculated as:
      quantity * product cost from productcost_daily_lookup
    - output grain:
      date + spot_id + product_id + movement_type_id

    Output columns:
    - date
    - spot_id
    - product_id
    - movement_type_id
    - quantity
    - cost
    """
    _validate_fact_inventory_movement_inputs(
        staging_df=staging_df,
        product_identity_df=product_identity_df,
        dim_product_df=dim_product_df,
        dim_spot_df=dim_spot_df,
        dim_movement_type_df=dim_movement_type_df,
        productcost_daily_lookup_df=productcost_daily_lookup_df,
    )

    movement_columns = _get_movement_columns(
        staging_df=staging_df,
        dim_movement_type_df=dim_movement_type_df,
    )

    if not movement_columns:
        raise DataValidationError(
            "No inventory movement columns found in staging_df based on dim_movement_type movement_type_column."
        )

    fact_df = _melt_inventory_movement(
        staging_df=staging_df,
        movement_columns=movement_columns,
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

    fact_df = _attach_movement_type(
        fact_df=fact_df,
        dim_movement_type_df=dim_movement_type_df,
    )

    fact_df = _attach_productcost(
        fact_df=fact_df,
        productcost_daily_lookup_df=productcost_daily_lookup_df,
    )

    fact_df = _calculate_movement_cost(fact_df)
    fact_df = _aggregate_fact_inventory_movement(fact_df)
    _validate_final_fact_inventory_movement(fact_df)

    return fact_df


def _validate_fact_inventory_movement_inputs(
    staging_df: pd.DataFrame,
    product_identity_df: pd.DataFrame,
    dim_product_df: pd.DataFrame,
    dim_spot_df: pd.DataFrame,
    dim_movement_type_df: pd.DataFrame,
    productcost_daily_lookup_df: pd.DataFrame,
) -> None:
    missing_staging = REQUIRED_STAGING_COLUMNS - set(staging_df.columns)
    if missing_staging:
        raise DataValidationError(
            "fact_inventory_movement staging input is missing columns: "
            f"{sorted(missing_staging)}"
        )

    missing_product_identity = REQUIRED_PRODUCT_IDENTITY_COLUMNS - set(
        product_identity_df.columns
    )
    if missing_product_identity:
        raise DataValidationError(
            "fact_inventory_movement product_identity input is missing columns: "
            f"{sorted(missing_product_identity)}"
        )

    missing_dim_product = REQUIRED_DIM_PRODUCT_COLUMNS - set(dim_product_df.columns)
    if missing_dim_product:
        raise DataValidationError(
            "fact_inventory_movement dim_product input is missing columns: "
            f"{sorted(missing_dim_product)}"
        )

    missing_dim_spot = REQUIRED_DIM_SPOT_COLUMNS - set(dim_spot_df.columns)
    if missing_dim_spot:
        raise DataValidationError(
            "fact_inventory_movement dim_spot input is missing columns: "
            f"{sorted(missing_dim_spot)}"
        )

    missing_dim_movement = REQUIRED_DIM_MOVEMENT_TYPE_COLUMNS - set(
        dim_movement_type_df.columns
    )
    if missing_dim_movement:
        raise DataValidationError(
            "fact_inventory_movement dim_movement_type input is missing columns: "
            f"{sorted(missing_dim_movement)}"
        )

    missing_productcost_lookup = REQUIRED_PRODUCTCOST_LOOKUP_COLUMNS - set(
        productcost_daily_lookup_df.columns
    )
    if missing_productcost_lookup:
        raise DataValidationError(
            "fact_inventory_movement productcost_daily_lookup input is missing columns: "
            f"{sorted(missing_productcost_lookup)}"
        )


def _get_movement_columns(
    staging_df: pd.DataFrame,
    dim_movement_type_df: pd.DataFrame,
) -> list[str]:
    """
    Get movement columns from dim_movement_type.movement_type_column
    that actually exist in staging_df.
    """
    movement_columns = (
        dim_movement_type_df["movement_type_column"]
        .dropna()
        .astype(str)
        .str.strip()
        .tolist()
    )

    existing_columns = [col for col in movement_columns if col in staging_df.columns]

    return list(dict.fromkeys(existing_columns))


def _melt_inventory_movement(
    staging_df: pd.DataFrame,
    movement_columns: list[str],
) -> pd.DataFrame:
    """
    Transform wide movement columns into long fact-ready rows.
    """
    base_columns = [
        "report_date",
        "spot_id_raw",
        "product_work_key",
        "koszt-przyjęcie",
    ]

    fact_df = staging_df.loc[:, base_columns + movement_columns].copy()

    fact_df = fact_df.melt(
        id_vars=base_columns,
        value_vars=movement_columns,
        var_name="movement_type_source_column",
        value_name="quantity",
    )

    fact_df["quantity"] = pd.to_numeric(fact_df["quantity"], errors="coerce")
    fact_df["koszt-przyjęcie"] = pd.to_numeric(
        fact_df["koszt-przyjęcie"], errors="coerce"
    )

    fact_df = fact_df.loc[fact_df["quantity"].notna()].copy()
    fact_df = fact_df.loc[fact_df["quantity"] != 0].copy()

    fact_df["report_date"] = pd.to_datetime(
        fact_df["report_date"],
        errors="coerce",
    ).dt.date

    if fact_df["report_date"].isna().any():
        raise DataValidationError(
            "fact_inventory_movement contains invalid report_date values after melt."
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
            "Some fact_inventory_movement rows have no product_business_key mapping "
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
            "Some fact_inventory_movement rows have no product_id mapping "
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
            "Some fact_inventory_movement rows have no spot_id mapping "
            f"for spot_id_raw values: {missing_keys[:20]}"
        )

    out["spot_id"] = out["spot_id"].astype(int)

    return out


def _attach_movement_type(
    fact_df: pd.DataFrame,
    dim_movement_type_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Attach movement_type_id and selected movement attributes using movement_type_column.
    """
    movement_map_df = (
        dim_movement_type_df.loc[
            :,
            [
                "movement_type_column",
                "movement_type_id",
                "movement_direction",
                "is_additional_detail",
            ],
        ]
        .drop_duplicates(subset=["movement_type_column"])
        .copy()
    )

    out = fact_df.merge(
        movement_map_df,
        how="left",
        left_on="movement_type_source_column",
        right_on="movement_type_column",
        validate="m:1",
    )

    if out["movement_type_id"].isna().any():
        missing_columns = (
            out.loc[out["movement_type_id"].isna(), "movement_type_source_column"]
            .drop_duplicates()
            .tolist()
        )
        raise DataValidationError(
            "Some fact_inventory_movement rows have no movement_type_id mapping "
            f"for movement source columns: {missing_columns[:20]}"
        )

    out["movement_type_id"] = out["movement_type_id"].astype(int)

    return out


def _attach_productcost(
    fact_df: pd.DataFrame,
    productcost_daily_lookup_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Attach daily productcost by date + product_id.
    """
    productcost_map_df = (
        productcost_daily_lookup_df.loc[:, ["date", "product_id", "productcost"]]
        .drop_duplicates(subset=["date", "product_id"])
        .copy()
    )

    out = fact_df.merge(
        productcost_map_df,
        how="left",
        left_on=["report_date", "product_id"],
        right_on=["date", "product_id"],
        validate="m:1",
    )

    return out


def _calculate_movement_cost(fact_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate movement cost.

    Rules:
    - for movement_type_source_column == 'Przyjęto':
      use staging 'koszt-przyjęcie'
    - for all other movement types:
      use quantity * productcost
    - if no cost can be determined:
      log warning and set cost = 0
    """
    out = fact_df.copy()

    out["cost"] = pd.NA

    receipt_mask = out["movement_type_source_column"].eq("Przyjęto")
    non_receipt_mask = ~receipt_mask

    # Direct source cost for receipts
    out.loc[receipt_mask, "cost"] = out.loc[receipt_mask, "koszt-przyjęcie"]

    # Calculated cost for other movement types where productcost exists
    non_receipt_with_productcost_mask = non_receipt_mask & out["productcost"].notna()
    out.loc[non_receipt_with_productcost_mask, "cost"] = (
        pd.to_numeric(out.loc[non_receipt_with_productcost_mask, "quantity"], errors="coerce")
        * pd.to_numeric(out.loc[non_receipt_with_productcost_mask, "productcost"], errors="coerce")
    )

    # Non-receipt rows without productcost -> warning + cost = 0
    missing_productcost_mask = non_receipt_mask & out["productcost"].isna()
    if missing_productcost_mask.any():
        missing_sample = (
            out.loc[
                missing_productcost_mask,
                ["report_date", "product_id", "movement_type_source_column"],
            ]
            .drop_duplicates()
            .head(20)
            .to_dict(orient="records")
        )
        log.warning(
            "%sINVENTORY COST WARNING: %s non-receipt row(s) have no productcost available. "
            "Setting cost = 0. Sample: %s%s",
            YELLOW,
            int(missing_productcost_mask.sum()),
            missing_sample,
            RESET,
        )
        out.loc[missing_productcost_mask, "cost"] = 0

    # Receipt rows without source cost -> fallback to quantity * productcost if possible
    missing_receipt_cost_mask = receipt_mask & out["cost"].isna()
    if missing_receipt_cost_mask.any():
        fallback_possible_mask = missing_receipt_cost_mask & out["productcost"].notna()

        if fallback_possible_mask.any():
            fallback_count = int(fallback_possible_mask.sum())
            log.warning(
                "%sINVENTORY COST WARNING: %s receipt row(s) have missing source 'koszt-przyjęcie'. "
                "Using quantity * productcost fallback.%s",
                YELLOW,
                fallback_count,
                RESET,
            )
            out.loc[fallback_possible_mask, "cost"] = (
                pd.to_numeric(out.loc[fallback_possible_mask, "quantity"], errors="coerce")
                * pd.to_numeric(out.loc[fallback_possible_mask, "productcost"], errors="coerce")
            )

        still_missing_mask = receipt_mask & out["cost"].isna()
        if still_missing_mask.any():
            missing_sample = (
                out.loc[
                    still_missing_mask,
                    ["report_date", "product_id", "movement_type_source_column"],
                ]
                .drop_duplicates()
                .head(20)
                .to_dict(orient="records")
            )
            log.warning(
                "%sINVENTORY COST WARNING: %s receipt row(s) have no cost available "
                "from 'koszt-przyjęcie' or fallback productcost. Setting cost = 0. Sample: %s%s",
                YELLOW,
                int(still_missing_mask.sum()),
                missing_sample,
                RESET,
            )
            out.loc[still_missing_mask, "cost"] = 0

    out["cost"] = pd.to_numeric(out["cost"], errors="coerce").fillna(0)

    return out


def _aggregate_fact_inventory_movement(fact_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate rows to final grain.
    """
    pre_agg_quantity_sum = pd.to_numeric(
        fact_df["quantity"], errors="coerce"
    ).fillna(0).sum()
    pre_agg_cost_sum = pd.to_numeric(
        fact_df["cost"], errors="coerce"
    ).fillna(0).sum()

    out = (
        fact_df.groupby(
            ["report_date", "spot_id", "product_id", "movement_type_id"],
            dropna=False,
            as_index=False,
        )[["quantity", "cost"]]
        .sum()
        .rename(columns={"report_date": "date"})
    )

    post_agg_quantity_sum = pd.to_numeric(
        out["quantity"], errors="coerce"
    ).fillna(0).sum()
    post_agg_cost_sum = pd.to_numeric(
        out["cost"], errors="coerce"
    ).fillna(0).sum()

    if pre_agg_quantity_sum != post_agg_quantity_sum:
        raise DataValidationError(
            "fact_inventory_movement aggregation changed total quantity: "
            f"before={pre_agg_quantity_sum}, after={post_agg_quantity_sum}"
        )

    if round(float(pre_agg_cost_sum), 6) != round(float(post_agg_cost_sum), 6):
        raise DataValidationError(
            "fact_inventory_movement aggregation changed total cost: "
            f"before={pre_agg_cost_sum}, after={post_agg_cost_sum}"
        )

    out["quantity"] = pd.to_numeric(out["quantity"], errors="coerce")
    out["cost"] = pd.to_numeric(out["cost"], errors="coerce")

    return out


def _validate_final_fact_inventory_movement(fact_df: pd.DataFrame) -> None:
    """
    Final validation of fact_inventory_movement output.
    """
    required_columns = {
        "date",
        "spot_id",
        "product_id",
        "movement_type_id",
        "quantity",
        "cost",
    }

    missing = required_columns - set(fact_df.columns)
    if missing:
        raise DataValidationError(
            "fact_inventory_movement output is missing required columns: "
            f"{sorted(missing)}"
        )

    if fact_df.empty:
        log.warning("fact_inventory_movement is empty after transformation.")
        return

    null_columns = ["date", "spot_id", "product_id", "movement_type_id", "quantity", "cost"]
    for col in null_columns:
        if fact_df[col].isna().any():
            raise DataValidationError(
                f"fact_inventory_movement output contains null values in column: '{col}'"
            )

    duplicated_mask = fact_df.duplicated(
        subset=["date", "spot_id", "product_id", "movement_type_id"],
        keep=False,
    )
    if duplicated_mask.any():
        raise DataValidationError(
            "fact_inventory_movement output contains duplicate rows for final grain."
        )

    if (fact_df["cost"] < 0).any():
        log.warning("fact_inventory_movement contains negative cost values.")

    if (fact_df["quantity"] < 0).any():
        log.warning("fact_inventory_movement contains negative quantity values.")