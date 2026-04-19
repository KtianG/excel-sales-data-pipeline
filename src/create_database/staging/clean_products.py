from __future__ import annotations

import logging
import re
from pathlib import Path

import pandas as pd

from create_database.exceptions import ConfigurationError, DataValidationError
from create_database.utils.json_loader import load_json


log = logging.getLogger(__name__)

YELLOW = "\033[93m"
RESET = "\033[0m"

MISSING_NAME_VALUES = {"", "0", ")"}
WORK_CODE_COLUMN_NAME = "Kod roboczy"
INTERNAL_CODE_COLUMN_NAME = "Kod wewnętrzny"


def clean_products(df: pd.DataFrame, corrections_json_path: Path) -> pd.DataFrame:
    """
    Clean and enrich product-related fields in staging data.

    Current scope:
    - preserve raw product fields
    - detect heated products from prefix in product code
    - clean product code and product name
    - normalize internal recipe-like code if column exists
    - build product_work_key
    - choose the longest name per product_work_key
    - store alternative names
    - apply canonical corrections from JSON
    - build fallback product group from product code
    - build final product group
    - flag system products
    - flag suspiciously short product names (excluding system products)
    """
    if df.empty:
        return df.copy()

    out = df.copy()

    _validate_required_input_columns(out)

    corrections_config = load_json(corrections_json_path)
    corrections_map = _validate_and_extract_corrections(
        corrections_config, corrections_json_path
    )

    # Preserve raw fields
    out["product_code_raw"] = out[WORK_CODE_COLUMN_NAME]
    out["product_name_raw"] = out["Nazwa"]

    if INTERNAL_CODE_COLUMN_NAME in out.columns:
        out["recipe_number_raw"] = out[INTERNAL_CODE_COLUMN_NAME]
    else:
        out["recipe_number_raw"] = pd.NA

    # Heated flag from raw code
    out["is_heated"] = out["product_code_raw"].map(_detect_is_heated)

    # Clean basic fields
    out["product_code_clean"] = out["product_code_raw"].map(_clean_product_code)
    out["product_name_clean"] = out.apply(_build_product_name_clean, axis=1)

    # Internal code normalization
    out["recipe_number_clean"] = out["recipe_number_raw"].map(_normalize_recipe_number)
    out["has_recipe_number"] = out["recipe_number_clean"].notna()

    # Identity basis
    out["product_identity_basis"] = out["has_recipe_number"].map(
        {True: "RECIPE_NUMBER", False: "DATE_CODE"}
    )

    # Working key
    out["product_work_key"] = out.apply(_build_product_work_key, axis=1)

    # Group-level chosen name and alternative names
    product_name_selected_map = _build_selected_name_map(out)
    alternative_names_map = _build_alternative_names_map(out)

    out["product_name_selected"] = out["product_work_key"].map(product_name_selected_map)
    out["product_alternative_names"] = out["product_work_key"].map(
        lambda key: alternative_names_map.get(key, {}).get("joined_names", "")
    )
    out["product_alternative_names_count"] = out["product_work_key"].map(
        lambda key: alternative_names_map.get(key, {}).get("count", 0)
    )

    # Apply canonical corrections to selected name
    correction_result = out["product_name_selected"].map(
        lambda name: _apply_name_correction(name, corrections_map)
    )

    out["product_name_canonical"] = correction_result.map(lambda x: x["canonical_name"])
    out["product_canonical_group"] = correction_result.map(lambda x: x["canonical_group"])
    out["product_name_corrected"] = correction_result.map(lambda x: x["corrected"])
    out["product_correction_action"] = correction_result.map(lambda x: x["action"])

    # Build fallback/final product group
    out["product_group_fallback"] = out["product_code_clean"].map(
        _build_product_group_fallback
    )
    out["product_group_final"] = out["product_canonical_group"].fillna(
        out["product_group_fallback"]
    )

    # Flags
    out["is_system_product"] = out["product_group_final"].eq("SYSTEM")
    out["is_product_name_missing"] = out["product_name_selected"].isna()
    out["is_product_code_missing"] = out["product_code_clean"].isna()

    out["is_short_product_name_warning"] = (
        out["product_name_selected"].map(_is_short_name)
        & (~out["is_system_product"])
    )

    _log_short_name_warning(out)

    return out


def _validate_required_input_columns(df: pd.DataFrame) -> None:
    required_columns = {WORK_CODE_COLUMN_NAME, "Nazwa", "report_date"}

    missing = required_columns - set(df.columns)
    if missing:
        raise DataValidationError(
            f"clean_products input is missing required columns: {sorted(missing)}"
        )


def _validate_and_extract_corrections(
    config: object, corrections_json_path: Path
) -> dict:
    """
    Validate structure of product_name_corrections.json and return name_corrections mapping.
    """
    if not isinstance(config, dict):
        raise ConfigurationError(
            f"product_name_corrections.json must contain a JSON object: {corrections_json_path}"
        )

    if "name_corrections" not in config:
        raise ConfigurationError(
            "product_name_corrections.json is missing required key: 'name_corrections'"
        )

    name_corrections = config["name_corrections"]
    if not isinstance(name_corrections, dict):
        raise ConfigurationError("'name_corrections' must be an object/dictionary")

    for source_name, item in name_corrections.items():
        if not isinstance(item, dict):
            raise ConfigurationError(
                f"Correction entry for '{source_name}' must be an object"
            )

        required_keys = {"canonical_name", "canonical_group", "action"}
        missing = required_keys - set(item.keys())
        if missing:
            raise ConfigurationError(
                f"Correction entry for '{source_name}' is missing keys: {sorted(missing)}"
            )

    return name_corrections


def _clean_basic_text(value: object) -> str | None:
    """
    Basic text cleaning:
    - cast to string
    - trim spaces
    - collapse repeated spaces
    - convert missing placeholders to NA
    """
    if pd.isna(value):
        return pd.NA

    text = str(value).strip()
    text = re.sub(r"\s+", " ", text)

    if text in MISSING_NAME_VALUES:
        return pd.NA

    return text if text else pd.NA


def _detect_is_heated(value: object) -> bool:
    """
    Detect heated sale from product code prefix .
    """
    if pd.isna(value):
        return False

    text = str(value).strip().upper()
    return text.startswith("C_") or text.startswith("C-")


def _clean_product_code(value: object) -> str | None:
    """
    Clean product code:
    - basic text cleaning
    - remove leading prefix used to mark heated sale
    """
    text = _clean_basic_text(value)

    if pd.isna(text):
        return pd.NA

    text = str(text).strip()
    text = re.sub(r"^C[_-]", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text).strip()

    if text in MISSING_NAME_VALUES or not text:
        return pd.NA

    return text


def _build_product_name_clean(row: pd.Series) -> str | None:
    """
    Build cleaned product name with fallback:
    product name -> if invalid, fallback to cleaned work code.
    """
    raw_name = _clean_basic_text(row.get("product_name_raw"))
    raw_code = row.get("product_code_clean")

    if pd.notna(raw_name):
        return raw_name

    if pd.notna(raw_code):
        return raw_code

    return pd.NA


def _normalize_recipe_number(value: object) -> str | None:
    """
    Normalize anonymized internal code.

    Accepted:
    - non-empty string values
    - anything except missing / NA placeholders

    Rejected:
    - empty values
    - 'NA'
    """
    if pd.isna(value):
        return pd.NA

    text = str(value).strip()
    if not text or text.upper() == "NA":
        return pd.NA

    return text


def _build_product_work_key(row: pd.Series) -> str:
    """
    Build product working key:
    - if internal code exists -> RECIPE_<internal_code>
    - otherwise -> dd_mm_yy_<product_code_clean>
    """
    recipe_number = row.get("recipe_number_clean")
    if pd.notna(recipe_number):
        return f"RECIPE_{recipe_number}"

    report_date = row.get("report_date")
    product_code = row.get("product_code_clean")

    if pd.isna(report_date):
        date_part = "NO_DATE"
    else:
        report_date = pd.Timestamp(report_date)
        date_part = report_date.strftime("%d_%m_%y")

    if pd.isna(product_code):
        code_part = "NO_CODE"
    else:
        code_part = str(product_code)

    return f"{date_part}_{code_part}"


def _build_selected_name_map(df: pd.DataFrame) -> dict[str, str | None]:
    """
    For each product_work_key choose the longest available product_name_clean.
    """
    selected_name_map: dict[str, str | None] = {}

    grouped = df.groupby("product_work_key", dropna=False)

    for work_key, group in grouped:
        names = (
            group["product_name_clean"]
            .dropna()
            .astype(str)
            .str.strip()
        )
        names = [name for name in names if name]

        if not names:
            selected_name_map[work_key] = pd.NA
            continue

        names_sorted = sorted(names, key=lambda x: (-len(x), x))
        selected_name_map[work_key] = names_sorted[0]

    return selected_name_map


def _build_alternative_names_map(df: pd.DataFrame) -> dict[str, dict[str, object]]:
    """
    Build alternative names list per product_work_key.
    """
    result: dict[str, dict[str, object]] = {}

    grouped = df.groupby("product_work_key", dropna=False)

    for work_key, group in grouped:
        names = (
            group["product_name_clean"]
            .dropna()
            .astype(str)
            .str.strip()
        )
        names = [name for name in names if name]

        unique_names = sorted(set(names))

        result[work_key] = {
            "joined_names": " | ".join(unique_names),
            "count": len(unique_names),
        }

    return result


def _apply_name_correction(
    product_name_selected: object, corrections_map: dict
) -> dict[str, object]:
    """
    Apply canonical name correction if selected product name exists in corrections map.
    """
    if pd.isna(product_name_selected):
        return {
            "canonical_name": pd.NA,
            "canonical_group": pd.NA,
            "corrected": False,
            "action": pd.NA,
        }

    selected_name = str(product_name_selected)

    if selected_name not in corrections_map:
        return {
            "canonical_name": selected_name,
            "canonical_group": pd.NA,
            "corrected": False,
            "action": "keep",
        }

    item = corrections_map[selected_name]

    return {
        "canonical_name": item["canonical_name"],
        "canonical_group": item["canonical_group"],
        "corrected": item["canonical_name"] != selected_name or item["action"] == "replace",
        "action": item["action"],
    }


def _build_product_group_fallback(value: object) -> str | None:
    """
    Build fallback product group from cleaned product code.

    Rules:
    1. If code starts with CAT + digits, keep that category, e.g.:
       CAT01-X-3-4 -> CAT01
       CAT17-2 -> CAT17

    2. Otherwise:
       - remove fragments like '-X-*-*'
       - replace '-' with spaces
       - remove digits
       - collapse spaces
       - replace spaces with underscores
       - uppercase final result
    """
    if pd.isna(value):
        return pd.NA

    text = str(value).strip().upper()

    cat_match = re.match(r"^(?:C[_-])?(CAT\d+)", text, flags=re.IGNORECASE)
    if cat_match:
        return cat_match.group(1).upper()

    text = re.sub(r"-X-[^-]+-[^-]+", "", text)
    text = text.replace("-", " ")
    text = re.sub(r"\d+", "", text)
    text = re.sub(r"\s+", " ", text).strip()

    if not text:
        return pd.NA

    return text.replace(" ", "_")


def _is_short_name(value: object) -> bool:
    """
    Flag suspiciously short product names (<= 6 chars).
    """
    if pd.isna(value):
        return False

    text = str(value).strip()
    return len(text) <= 6


def _log_short_name_warning(df: pd.DataFrame) -> None:
    """
    Log warning for suspiciously short product names.
    System products are excluded from this warning.
    """
    warning_rows = df.loc[df["is_short_product_name_warning"]].copy()

    if warning_rows.empty:
        return

    warning_summary = (
        warning_rows.loc[:, ["product_work_key", "product_name_selected"]]
        .drop_duplicates()
        .sort_values(["product_name_selected", "product_work_key"])
    )

    count = len(warning_summary)

    log.warning(
        "%sPRODUCT WARNING: %s product(s) have selected name length <= 6. Check staging export for details.%s",
        YELLOW,
        count,
        RESET,
    )