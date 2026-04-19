from __future__ import annotations

import logging

import pandas as pd

from create_database.exceptions import DataValidationError


log = logging.getLogger(__name__)

YELLOW = "\033[93m"
RESET = "\033[0m"

REQUIRED_STAGING_COLUMNS = {
    "report_date",
    "product_work_key",
    "Przyjęto",
    "koszt-jednostka",
}

REQUIRED_PRODUCT_IDENTITY_COLUMNS = {
    "product_work_key",
    "product_business_key",
}

REQUIRED_DIM_PRODUCT_COLUMNS = {
    "product_business_key",
    "product_id",
}


def build_fact_productcost(
    staging_df: pd.DataFrame,
    product_identity_df: pd.DataFrame,
    dim_product_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build fact_productcost from staging data and product dimension mapping.

    Rules:
    - use only rows where 'Przyjęto' > 0
    - use 'koszt-jednostka' as the source unit cost
    - if multiple koszt-jednostka values exist for the same date + product_id:
      choose the highest value and log a warning

    Final grain:
    - date
    - product_id

    Measure:
    - productcost
    """
    _validate_fact_productcost_inputs(
        staging_df=staging_df,
        product_identity_df=product_identity_df,
        dim_product_df=dim_product_df,
    )

    fact_df = _prepare_productcost_base(staging_df)
    fact_df = _attach_product_business_key(
        fact_df=fact_df,
        product_identity_df=product_identity_df,
    )
    fact_df = _attach_product_id(
        fact_df=fact_df,
        dim_product_df=dim_product_df,
    )
    fact_df = _aggregate_fact_productcost(fact_df)
    _validate_final_fact_productcost(fact_df)

    return fact_df


def _validate_fact_productcost_inputs(
    staging_df: pd.DataFrame,
    product_identity_df: pd.DataFrame,
    dim_product_df: pd.DataFrame,
) -> None:
    missing_staging = REQUIRED_STAGING_COLUMNS - set(staging_df.columns)
    if missing_staging:
        raise DataValidationError(
            f"fact_productcost staging input is missing columns: {sorted(missing_staging)}"
        )

    missing_product_identity = REQUIRED_PRODUCT_IDENTITY_COLUMNS - set(
        product_identity_df.columns
    )
    if missing_product_identity:
        raise DataValidationError(
            "fact_productcost product_identity input is missing columns: "
            f"{sorted(missing_product_identity)}"
        )

    missing_dim_product = REQUIRED_DIM_PRODUCT_COLUMNS - set(dim_product_df.columns)
    if missing_dim_product:
        raise DataValidationError(
            f"fact_productcost dim_product input is missing columns: {sorted(missing_dim_product)}"
        )


def _prepare_productcost_base(staging_df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare minimal base DataFrame for fact_productcost.

    Keep only rows where:
    - Przyjęto > 0
    - koszt-jednostka is not null
    """
    base_columns = [
        "report_date",
        "product_work_key",
        "Przyjęto",
        "koszt-jednostka",
    ]

    fact_df = staging_df.loc[:, base_columns].copy()

    fact_df["Przyjęto"] = pd.to_numeric(fact_df["Przyjęto"], errors="coerce")
    fact_df["koszt-jednostka"] = pd.to_numeric(
        fact_df["koszt-jednostka"], errors="coerce"
    )

    fact_df = fact_df.loc[fact_df["Przyjęto"].notna()].copy()
    fact_df = fact_df.loc[fact_df["Przyjęto"] > 0].copy()
    fact_df = fact_df.loc[fact_df["koszt-jednostka"].notna()].copy()

    fact_df["report_date"] = pd.to_datetime(
        fact_df["report_date"],
        errors="coerce",
    ).dt.date

    if fact_df["report_date"].isna().any():
        raise DataValidationError(
            "fact_productcost contains invalid report_date values."
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
            "Some fact_productcost rows have no product_business_key mapping "
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
            "Some fact_productcost rows have no product_id mapping "
            f"for product_business_key values: {missing_keys[:20]}"
        )

    out["product_id"] = out["product_id"].astype(int)

    return out


def _aggregate_fact_productcost(fact_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate rows to final grain date + product_id.

    If multiple koszt-jednostka values exist for the same grain:
    - log warning
    - choose the highest value
    """
    working_df = fact_df.loc[:, ["report_date", "product_id", "koszt-jednostka"]].copy()
    working_df = working_df.rename(
        columns={
            "report_date": "date",
            "koszt-jednostka": "productcost",
        }
    )

    conflict_summary = (
        working_df.groupby(["date", "product_id"], dropna=False)["productcost"]
        .nunique(dropna=True)
        .reset_index(name="productcost_unique_count")
    )

    conflict_rows = conflict_summary.loc[
        conflict_summary["productcost_unique_count"] > 1
    ].copy()

    if not conflict_rows.empty:
        sample_conflicts = (
            conflict_rows.sort_values(["date", "product_id"])
            .head(20)
            .to_dict(orient="records")
        )

        log.warning(
            "%sPRODUCTCOST WARNING: %s date + product_id combinations have multiple product cost values. "
            "Using the highest productcost. Sample: %s%s",
            YELLOW,
            len(conflict_rows),
            sample_conflicts,
            RESET,
        )

    out = (
        working_df.groupby(["date", "product_id"], dropna=False, as_index=False)["productcost"]
        .max()
    )

    out["productcost"] = pd.to_numeric(out["productcost"], errors="coerce")

    return out


def _validate_final_fact_productcost(fact_df: pd.DataFrame) -> None:
    """
    Final validation of fact_productcost output.
    """
    required_columns = {
        "date",
        "product_id",
        "productcost",
    }

    missing = required_columns - set(fact_df.columns)
    if missing:
        raise DataValidationError(
            f"fact_productcost output is missing required columns: {sorted(missing)}"
        )

    if fact_df.empty:
        log.warning("fact_productcost is empty after transformation.")
        return

    null_columns = ["date", "product_id", "productcost"]
    for col in null_columns:
        if fact_df[col].isna().any():
            raise DataValidationError(
                f"fact_productcost output contains null values in column: '{col}'"
            )

    duplicated_mask = fact_df.duplicated(
        subset=["date", "product_id"],
        keep=False,
    )
    if duplicated_mask.any():
        raise DataValidationError(
            "fact_productcost output contains duplicate rows for final grain."
        )

    if (fact_df["productcost"] < 0).any():
        log.warning("fact_productcost contains negative productcost values.")