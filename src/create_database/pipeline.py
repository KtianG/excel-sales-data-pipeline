from __future__ import annotations

import logging

import pandas as pd

from create_database.config.paths import DAILY_REPORTS_DIR, JSON_DIR, OUTPUT_DIR, TEMP_DIR
from create_database.config.settings import (
    DEBUG_EXPORT_DIM_DATE,
    DEBUG_EXPORT_DIM_MOVEMENT_TYPE,
    DEBUG_EXPORT_DIM_PRODUCT,
    DEBUG_EXPORT_DIM_SALE_TYPE,
    DEBUG_EXPORT_DIM_SPOT,
    DEBUG_EXPORT_EXTRACT,
    DEBUG_EXPORT_FACT_PRODUCTCOST,
    DEBUG_EXPORT_FACT_INVENTORY_MOVEMENT,
    DEBUG_EXPORT_FACT_SALES_GROSS,
    DEBUG_EXPORT_FACT_SALES_QUANTITY,
    DEBUG_EXPORT_PRODUCTCOST_DAILY_LOOKUP,
    DEBUG_EXPORT_PRODUCT_IDENTITY,
    DEBUG_EXPORT_RAW,
    DEBUG_EXPORT_STAGING,
    DIM_DATE_DEBUG_EXPORT_FILENAME,
    DIM_DATE_EXPORT_FILENAME,
    DIM_MOVEMENT_TYPE_DEBUG_EXPORT_FILENAME,
    DIM_MOVEMENT_TYPE_EXPORT_FILENAME,
    DIM_PRODUCT_DEBUG_EXPORT_FILENAME,
    DIM_PRODUCT_EXPORT_FILENAME,
    DIM_SALE_TYPE_DEBUG_EXPORT_FILENAME,
    DIM_SALE_TYPE_EXPORT_FILENAME,
    DIM_SPOT_DEBUG_EXPORT_FILENAME,
    DIM_SPOT_EXPORT_FILENAME,
    EXTRACT_EXPORT_FILENAME,
    FACT_PRODUCTCOST_DEBUG_EXPORT_FILENAME,
    FACT_PRODUCTCOST_EXPORT_FILENAME,
    FACT_INVENTORY_MOVEMENT_DEBUG_EXPORT_FILENAME,
    FACT_INVENTORY_MOVEMENT_EXPORT_FILENAME,
    FACT_SALES_GROSS_DEBUG_EXPORT_FILENAME,
    FACT_SALES_GROSS_EXPORT_FILENAME,
    FACT_SALES_QUANTITY_DEBUG_EXPORT_FILENAME,
    FACT_SALES_QUANTITY_EXPORT_FILENAME,
    PRODUCTCOST_DAILY_LOOKUP_DEBUG_EXPORT_FILENAME,
    PRODUCT_IDENTITY_DEBUG_EXPORT_FILENAME,
    RAW_DEBUG_EXPORT_FILENAME,
    RAW_EXPORT_FILENAME,
    STAGING_DEBUG_EXPORT_FILENAME,
    SUPPORTED_EXTENSIONS,
)
from create_database.dimensions.dim_date import build_dim_date
from create_database.dimensions.dim_movement_type import build_dim_movement_type
from create_database.dimensions.dim_product import build_dim_product
from create_database.dimensions.dim_sale_type import build_dim_sale_type
from create_database.dimensions.dim_spot import build_dim_spot
from create_database.dimensions.product_identity import build_product_identity
from create_database.extract.excel_reader import read_all_excel_files
from create_database.extract.file_scanner import scan_excel_files
from create_database.facts.fact_productcost import build_fact_productcost
from create_database.facts.fact_inventory_movement import build_fact_inventory_movement
from create_database.facts.fact_sales_gross import build_fact_sales_gross
from create_database.facts.fact_sales_quantity import build_fact_sales_quantity
from create_database.facts.productcost_daily_lookup import build_productcost_daily_lookup
from create_database.raw.raw_loader import save_raw_to_csv
from create_database.staging.staging_builder import build_staging
from create_database.utils.io import write_csv, write_excel


log = logging.getLogger(__name__)

CSV_OUTPUT_DIR = OUTPUT_DIR / "csv"


def run_pipeline() -> None:
    excel_files = scan_excel_files(
        input_dir=DAILY_REPORTS_DIR,
        extensions=SUPPORTED_EXTENSIONS,
    )

    if not excel_files:
        log.info("No Excel files found in: %s", DAILY_REPORTS_DIR)
        return

    log.info("Found %s Excel files.", len(excel_files))

    extract_df = read_all_excel_files(excel_files)

    if extract_df.empty:
        log.info("No rows extracted from eligible sheets.")
        return

    log.info("Extracted %s rows and %s columns.", *extract_df.shape)

    _export_extract_debug(extract_df)
    _save_raw_layer(extract_df)
    _export_raw_debug(extract_df)

    staging_df = build_staging(extract_df)
    log.info(
        "Built staging dataframe with %s rows and %s columns.",
        *staging_df.shape,
    )
    _export_staging_debug(staging_df)

    dim_sale_type_df = build_dim_sale_type(
        staging_df=staging_df,
        sale_types_json_path=JSON_DIR / "sale_types.json",
    )
    log.info(
        "Built dim_sale_type dataframe with %s rows and %s columns.",
        *dim_sale_type_df.shape,
    )
    _export_dim_sale_type_debug(dim_sale_type_df)

    dim_spot_df = build_dim_spot(
        spots_json_path=JSON_DIR / "spots.json",
    )
    log.info(
        "Built dim_spot dataframe with %s rows and %s columns.",
        *dim_spot_df.shape,
    )
    _export_dim_spot_debug(dim_spot_df)

    dim_date_df = build_dim_date(
        staging_df=staging_df,
        date_config_path=JSON_DIR / "date_dimension.json",
    )
    log.info(
        "Built dim_date dataframe with %s rows and %s columns.",
        *dim_date_df.shape,
    )
    _export_dim_date_debug(dim_date_df)

    dim_movement_type_df = build_dim_movement_type(
        movement_types_json_path=JSON_DIR / "movement_types.json",
    )
    log.info(
        "Built dim_movement_type dataframe with %s rows and %s columns.",
        *dim_movement_type_df.shape,
    )
    _export_dim_movement_type_debug(dim_movement_type_df)

    product_identity_df = build_product_identity(
        staging_df=staging_df,
    )
    log.info(
        "Built product_identity dataframe with %s rows and %s columns.",
        *product_identity_df.shape,
    )
    _export_product_identity_debug(product_identity_df)

    log.info("Starting dim_product build...")

    dim_product_df = build_dim_product(
        staging_df=staging_df,
        product_identity_df=product_identity_df,
        source_rules_json_path=JSON_DIR / "product_source_rules.json",
    )

    log.info("dim_product build finished.")
    log.info(
        "Built dim_product dataframe with %s rows and %s columns.",
        *dim_product_df.shape,
    )
    _export_dim_product_debug(dim_product_df)

    fact_sales_quantity_df = build_fact_sales_quantity(
        staging_df=staging_df,
        product_identity_df=product_identity_df,
        dim_product_df=dim_product_df,
        dim_sale_type_df=dim_sale_type_df,
        dim_spot_df=dim_spot_df,
    )
    log.info(
        "Built fact_sales_quantity dataframe with %s rows and %s columns.",
        *fact_sales_quantity_df.shape,
    )
    _export_fact_sales_quantity_debug(fact_sales_quantity_df)

    fact_sales_gross_df = build_fact_sales_gross(
        staging_df=staging_df,
        product_identity_df=product_identity_df,
        dim_product_df=dim_product_df,
        dim_spot_df=dim_spot_df,
    )
    log.info(
        "Built fact_sales_gross dataframe with %s rows and %s columns.",
        *fact_sales_gross_df.shape,
    )
    _export_fact_sales_gross_debug(fact_sales_gross_df)

    fact_productcost_df = build_fact_productcost(
        staging_df=staging_df,
        product_identity_df=product_identity_df,
        dim_product_df=dim_product_df,
    )
    log.info(
        "Built fact_productcost dataframe with %s rows and %s columns.",
        *fact_productcost_df.shape,
    )
    _export_fact_productcost_debug(fact_productcost_df)

    productcost_daily_lookup_df = build_productcost_daily_lookup(
        fact_productcost_df=fact_productcost_df,
        dim_date_df=dim_date_df,
    )
    log.info(
        "Built productcost_daily_lookup dataframe with %s rows and %s columns.",
        *productcost_daily_lookup_df.shape,
    )
    _export_productcost_daily_lookup_debug(productcost_daily_lookup_df)

    fact_inventory_movement_df = build_fact_inventory_movement(
        staging_df=staging_df,
        product_identity_df=product_identity_df,
        dim_product_df=dim_product_df,
        dim_spot_df=dim_spot_df,
        dim_movement_type_df=dim_movement_type_df,
        productcost_daily_lookup_df=productcost_daily_lookup_df,
    )
    log.info(
        "Built fact_inventory_movement dataframe with %s rows and %s columns.",
        *fact_inventory_movement_df.shape,
    )
    _export_fact_inventory_movement_debug(fact_inventory_movement_df)

    _export_model_csvs(
        dim_date_df=dim_date_df,
        dim_spot_df=dim_spot_df,
        dim_sale_type_df=dim_sale_type_df,
        dim_movement_type_df=dim_movement_type_df,
        dim_product_df=dim_product_df,
        fact_sales_quantity_df=fact_sales_quantity_df,
        fact_sales_gross_df=fact_sales_gross_df,
        fact_productcost_df=fact_productcost_df,
        fact_inventory_movement_df=fact_inventory_movement_df,
    )


def _export_model_csvs(
    dim_date_df: pd.DataFrame,
    dim_spot_df: pd.DataFrame,
    dim_sale_type_df: pd.DataFrame,
    dim_movement_type_df: pd.DataFrame,
    dim_product_df: pd.DataFrame,
    fact_sales_quantity_df: pd.DataFrame,
    fact_sales_gross_df: pd.DataFrame,
    fact_productcost_df: pd.DataFrame,
    fact_inventory_movement_df: pd.DataFrame,
) -> None:
    _export_csv(dim_date_df, DIM_DATE_EXPORT_FILENAME)
    _export_csv(dim_spot_df, DIM_SPOT_EXPORT_FILENAME)
    _export_csv(dim_sale_type_df, DIM_SALE_TYPE_EXPORT_FILENAME)
    _export_csv(dim_movement_type_df, DIM_MOVEMENT_TYPE_EXPORT_FILENAME)
    _export_csv(dim_product_df, DIM_PRODUCT_EXPORT_FILENAME)

    _export_csv(fact_sales_quantity_df, FACT_SALES_QUANTITY_EXPORT_FILENAME)
    _export_csv(fact_sales_gross_df, FACT_SALES_GROSS_EXPORT_FILENAME)
    _export_csv(fact_productcost_df, FACT_PRODUCTCOST_EXPORT_FILENAME)
    _export_csv(fact_inventory_movement_df, FACT_INVENTORY_MOVEMENT_EXPORT_FILENAME)


def _export_csv(df: pd.DataFrame, filename: str) -> None:
    output_path = CSV_OUTPUT_DIR / filename
    write_csv(df, output_path)
    log.info("CSV written to: %s", output_path)


def _export_extract_debug(extract_df: pd.DataFrame) -> None:
    if not DEBUG_EXPORT_EXTRACT:
        return

    extract_debug_path = TEMP_DIR / EXTRACT_EXPORT_FILENAME
    write_excel(extract_df, extract_debug_path)
    log.info("Extract debug file written to: %s", extract_debug_path)


def _save_raw_layer(extract_df: pd.DataFrame) -> None:
    raw_output_path = OUTPUT_DIR / RAW_EXPORT_FILENAME
    save_raw_to_csv(extract_df, raw_output_path)
    log.info("Raw CSV written to: %s", raw_output_path)


def _export_raw_debug(extract_df: pd.DataFrame) -> None:
    if not DEBUG_EXPORT_RAW:
        return

    raw_debug_path = TEMP_DIR / RAW_DEBUG_EXPORT_FILENAME
    write_excel(extract_df, raw_debug_path)
    log.info("Raw debug file written to: %s", raw_debug_path)


def _export_staging_debug(staging_df: pd.DataFrame) -> None:
    if not DEBUG_EXPORT_STAGING:
        return

    staging_debug_path = TEMP_DIR / STAGING_DEBUG_EXPORT_FILENAME
    write_excel(staging_df, staging_debug_path)
    log.info("Staging debug file written to: %s", staging_debug_path)


def _export_dim_sale_type_debug(dim_sale_type_df: pd.DataFrame) -> None:
    if not DEBUG_EXPORT_DIM_SALE_TYPE:
        return

    dim_sale_type_debug_path = TEMP_DIR / DIM_SALE_TYPE_DEBUG_EXPORT_FILENAME
    write_excel(dim_sale_type_df, dim_sale_type_debug_path)
    log.info("dim_sale_type debug file written to: %s", dim_sale_type_debug_path)


def _export_dim_spot_debug(dim_spot_df: pd.DataFrame) -> None:
    if not DEBUG_EXPORT_DIM_SPOT:
        return

    dim_spot_debug_path = TEMP_DIR / DIM_SPOT_DEBUG_EXPORT_FILENAME
    write_excel(dim_spot_df, dim_spot_debug_path)
    log.info("dim_spot debug file written to: %s", dim_spot_debug_path)


def _export_dim_date_debug(dim_date_df: pd.DataFrame) -> None:
    if not DEBUG_EXPORT_DIM_DATE:
        return

    dim_date_debug_path = TEMP_DIR / DIM_DATE_DEBUG_EXPORT_FILENAME
    write_excel(dim_date_df, dim_date_debug_path)
    log.info("dim_date debug file written to: %s", dim_date_debug_path)


def _export_dim_movement_type_debug(dim_movement_type_df: pd.DataFrame) -> None:
    if not DEBUG_EXPORT_DIM_MOVEMENT_TYPE:
        return

    dim_movement_type_debug_path = TEMP_DIR / DIM_MOVEMENT_TYPE_DEBUG_EXPORT_FILENAME
    write_excel(dim_movement_type_df, dim_movement_type_debug_path)
    log.info(
        "dim_movement_type debug file written to: %s",
        dim_movement_type_debug_path,
    )


def _export_product_identity_debug(product_identity_df: pd.DataFrame) -> None:
    if not DEBUG_EXPORT_PRODUCT_IDENTITY:
        return

    product_identity_debug_path = TEMP_DIR / PRODUCT_IDENTITY_DEBUG_EXPORT_FILENAME
    write_excel(product_identity_df, product_identity_debug_path)
    log.info(
        "product_identity debug file written to: %s",
        product_identity_debug_path,
    )


def _export_dim_product_debug(dim_product_df: pd.DataFrame) -> None:
    if not DEBUG_EXPORT_DIM_PRODUCT:
        return

    dim_product_debug_path = TEMP_DIR / DIM_PRODUCT_DEBUG_EXPORT_FILENAME
    write_excel(dim_product_df, dim_product_debug_path)
    log.info("dim_product debug file written to: %s", dim_product_debug_path)


def _export_fact_sales_quantity_debug(fact_sales_quantity_df: pd.DataFrame) -> None:
    if not DEBUG_EXPORT_FACT_SALES_QUANTITY:
        return

    fact_sales_quantity_debug_path = TEMP_DIR / FACT_SALES_QUANTITY_DEBUG_EXPORT_FILENAME
    write_excel(fact_sales_quantity_df, fact_sales_quantity_debug_path)
    log.info(
        "fact_sales_quantity debug file written to: %s",
        fact_sales_quantity_debug_path,
    )


def _export_fact_sales_gross_debug(fact_sales_gross_df: pd.DataFrame) -> None:
    if not DEBUG_EXPORT_FACT_SALES_GROSS:
        return

    fact_sales_gross_debug_path = TEMP_DIR / FACT_SALES_GROSS_DEBUG_EXPORT_FILENAME
    write_excel(fact_sales_gross_df, fact_sales_gross_debug_path)
    log.info(
        "fact_sales_gross debug file written to: %s",
        fact_sales_gross_debug_path,
    )


def _export_fact_productcost_debug(fact_productcost_df: pd.DataFrame) -> None:
    if not DEBUG_EXPORT_FACT_PRODUCTCOST:
        return

    fact_productcost_debug_path = TEMP_DIR / FACT_PRODUCTCOST_DEBUG_EXPORT_FILENAME
    write_excel(fact_productcost_df, fact_productcost_debug_path)
    log.info(
        "fact_productcost debug file written to: %s",
        fact_productcost_debug_path,
    )


def _export_productcost_daily_lookup_debug(productcost_daily_lookup_df: pd.DataFrame) -> None:
    if not DEBUG_EXPORT_PRODUCTCOST_DAILY_LOOKUP:
        return

    productcost_daily_lookup_debug_path = TEMP_DIR / PRODUCTCOST_DAILY_LOOKUP_DEBUG_EXPORT_FILENAME
    write_excel(productcost_daily_lookup_df, productcost_daily_lookup_debug_path)
    log.info(
        "productcost_daily_lookup debug file written to: %s",
        productcost_daily_lookup_debug_path,
    )


def _export_fact_inventory_movement_debug(
    fact_inventory_movement_df: pd.DataFrame,
) -> None:
    if not DEBUG_EXPORT_FACT_INVENTORY_MOVEMENT:
        return

    fact_inventory_movement_debug_path = (
        TEMP_DIR / FACT_INVENTORY_MOVEMENT_DEBUG_EXPORT_FILENAME
    )
    write_excel(fact_inventory_movement_df, fact_inventory_movement_debug_path)
    log.info(
        "fact_inventory_movement debug file written to: %s",
        fact_inventory_movement_debug_path,
    )