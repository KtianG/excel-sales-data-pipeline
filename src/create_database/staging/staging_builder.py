from __future__ import annotations

from pathlib import Path
import pandas as pd

from create_database.config.paths import JSON_DIR
from create_database.staging.clean_products import clean_products
from create_database.staging.map_spots import map_spots
from create_database.staging.enrich_sales_types import enrich_sales_types
from create_database.staging.normalize_columns import normalize_columns
from create_database.staging.parse_dates import parse_dates
from create_database.staging.normalize_metrics import normalize_metrics


def build_staging(
    df: pd.DataFrame,
    json_dir: Path = JSON_DIR,
) -> pd.DataFrame:
    """
    Build staging dataframe from raw extract.

    Steps:
    1. normalize column names
    2. parse report_date_raw into report_date
    3. convert metric columns to numeric
    4. map spot identifiers using JSON configuration
    5. enrich sales types based on mapping rules
    6. clean and standardize product-related fields
    """
    if df.empty:
        return df.copy()

    return (
        df.pipe(normalize_columns)
        .pipe(parse_dates)
        .pipe(normalize_metrics)
        .pipe(map_spots, spots_json_path=json_dir / "spots.json")
        .pipe(enrich_sales_types, sale_types_json_path=json_dir / "sale_types.json")
        .pipe(clean_products, corrections_json_path=json_dir / "product_name_corrections.json")
    )