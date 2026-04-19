from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

import pandas as pd

from create_database.exceptions import ConfigurationError, DataValidationError
from create_database.utils.json_loader import load_json


log = logging.getLogger(__name__)

YELLOW = "\033[93m"
RESET = "\033[0m"


REQUIRED_STAGING_COLUMNS = {
    "product_work_key",
    "recipe_number_clean",
    "product_name_selected",
    "product_alternative_names",
    "product_group_final",
    "product_code_clean",
    "product_code_raw",
}

REQUIRED_IDENTITY_COLUMNS = {
    "product_work_key",
    "product_business_key",
    "product_identity_type",
    "is_historical_stable",
}


def build_dim_product(
    staging_df: pd.DataFrame,
    product_identity_df: pd.DataFrame,
    source_rules_json_path: Path,
) -> pd.DataFrame:
    """
    Build dim_product from staging data and product identity mapping.

    Output columns:
    - product_id
    - product_business_key
    - product_identity_type
    - is_historical_stable
    - recipe_number
    - product_name
    - normalized_product_name
    - product_type
    - source
    - product_alternative_names
    """

    if staging_df.empty:
        raise DataValidationError("dim_product input staging_df is empty.")
    
    _validate_dim_product_inputs(staging_df, product_identity_df)

    source_rules_config = load_json(source_rules_json_path)
    source_rules = _validate_and_prepare_source_rules(
        source_rules_config,
        source_rules_json_path,
    )

    merged_df = staging_df.merge(
        product_identity_df,
        how="left",
        on="product_work_key",
        validate="m:1",
    )

    if merged_df["product_business_key"].isna().any():
        missing_keys = (
            merged_df.loc[merged_df["product_business_key"].isna(), "product_work_key"]
            .drop_duplicates()
            .tolist()
        )
        raise DataValidationError(
            f"Some staging rows have no product identity mapping: {missing_keys[:20]}"
        )

    records: list[dict[str, object]] = []

    grouped = merged_df.groupby("product_business_key", dropna=False)

    for product_business_key, group in grouped:
        product_identity_type = _resolve_product_identity_type(
            group["product_identity_type"]
        )
        is_historical_stable = product_identity_type == "RECIPE"

        recipe_number = _get_first_non_null(group["recipe_number_clean"])
        product_name = _select_longest_name(group["product_name_selected"])
        normalized_product_name = _normalize_product_name(product_name)
        product_type = _resolve_product_type(
            group["product_group_final"],
            product_business_key=product_business_key,
        )
        source = _resolve_product_source(
            group=group,
            source_rules=source_rules,
            product_business_key=product_business_key,
        )
        product_alternative_names = _collect_alternative_names(group)

        records.append(
            {
                "product_business_key": product_business_key,
                "product_identity_type": product_identity_type,
                "is_historical_stable": bool(is_historical_stable),
                "recipe_number": recipe_number,
                "product_name": product_name,
                "normalized_product_name": normalized_product_name,
                "product_type": product_type,
                "source": source,
                "product_alternative_names": product_alternative_names,
            }
        )

    dim_df = pd.DataFrame(records)

    if dim_df.empty:
        raise DataValidationError("dim_product is empty.")

    if dim_df["product_business_key"].duplicated().any():
        duplicates = (
            dim_df.loc[
                dim_df["product_business_key"].duplicated(),
                "product_business_key",
            ]
            .drop_duplicates()
            .tolist()
        )
        raise DataValidationError(
            f"Duplicate product_business_key values in dim_product: {duplicates}"
        )

    dim_df = dim_df.sort_values(
        "product_business_key",
        kind="mergesort",
    ).reset_index(drop=True)
    dim_df.insert(0, "product_id", range(1, len(dim_df) + 1))

    dim_df = dim_df.loc[
        :,
        [
            "product_id",
            "product_business_key",
            "product_identity_type",
            "is_historical_stable",
            "recipe_number",
            "product_name",
            "normalized_product_name",
            "product_type",
            "source",
            "product_alternative_names",
        ],
    ].copy()

    return dim_df


def _validate_dim_product_inputs(
    staging_df: pd.DataFrame,
    product_identity_df: pd.DataFrame,
) -> None:
    missing_staging = REQUIRED_STAGING_COLUMNS - set(staging_df.columns)
    if missing_staging:
        raise DataValidationError(
            f"dim_product staging input is missing columns: {sorted(missing_staging)}"
        )

    missing_identity = REQUIRED_IDENTITY_COLUMNS - set(product_identity_df.columns)
    if missing_identity:
        raise DataValidationError(
            f"dim_product identity input is missing columns: {sorted(missing_identity)}"
        )


def _validate_and_prepare_source_rules(
    config: object,
    source_rules_json_path: Path,
) -> dict[str, Any]:
    """
    Validate product_source_rules.json and return normalized config.
    """
    if not isinstance(config, dict):
        raise ConfigurationError(
            f"product_source_rules.json must contain a JSON object: {source_rules_json_path}"
        )

    required_top_level_keys = {"default_source", "rules"}
    missing = required_top_level_keys - set(config.keys())
    if missing:
        raise ConfigurationError(
            f"product_source_rules.json is missing required keys: {sorted(missing)}"
        )

    default_source = config["default_source"]
    rules = config["rules"]

    if not isinstance(default_source, str) or not default_source.strip():
        raise ConfigurationError("'default_source' must be a non-empty string")

    if not isinstance(rules, list):
        raise ConfigurationError("'rules' must be a list")

    normalized_rules: list[dict[str, Any]] = []

    for idx, rule in enumerate(rules):
        if not isinstance(rule, dict):
            raise ConfigurationError(
                f"Rule #{idx} in product_source_rules.json must be an object"
            )

        required_rule_keys = {"rule_name", "priority", "conditions", "source"}
        missing_rule_keys = required_rule_keys - set(rule.keys())
        if missing_rule_keys:
            raise ConfigurationError(
                f"Rule #{idx} in product_source_rules.json is missing keys: {sorted(missing_rule_keys)}"
            )

        rule_name = rule["rule_name"]
        priority = rule["priority"]
        conditions = rule["conditions"]
        source = rule["source"]

        if not isinstance(rule_name, str) or not rule_name.strip():
            raise ConfigurationError(
                f"Rule #{idx}: 'rule_name' must be a non-empty string"
            )

        if not isinstance(priority, int):
            raise ConfigurationError(
                f"Rule '{rule_name}': 'priority' must be an integer"
            )

        if not isinstance(conditions, dict):
            raise ConfigurationError(
                f"Rule '{rule_name}': 'conditions' must be an object"
            )

        if not isinstance(source, str) or not source.strip():
            raise ConfigurationError(
                f"Rule '{rule_name}': 'source' must be a non-empty string"
            )

        allowed_condition_keys = {
            "has_recipe_number",
            "product_code_contains",
            "product_name_in",
            "product_name_regex",
            "product_group_in",
        }
        unsupported_keys = set(conditions.keys()) - allowed_condition_keys
        if unsupported_keys:
            raise ConfigurationError(
                f"Rule '{rule_name}': unsupported condition keys: {sorted(unsupported_keys)}"
            )

        if "has_recipe_number" in conditions and not isinstance(
            conditions["has_recipe_number"], bool
        ):
            raise ConfigurationError(
                f"Rule '{rule_name}': 'has_recipe_number' must be boolean"
            )

        for list_key in (
            "product_code_contains",
            "product_name_in",
            "product_name_regex",
            "product_group_in",
        ):
            if list_key in conditions:
                if not isinstance(conditions[list_key], list):
                    raise ConfigurationError(
                        f"Rule '{rule_name}': '{list_key}' must be a list"
                    )
                if not all(
                    isinstance(item, str) and item.strip()
                    for item in conditions[list_key]
                ):
                    raise ConfigurationError(
                        f"Rule '{rule_name}': '{list_key}' must contain non-empty strings only"
                    )

        normalized_rules.append(
            {
                "rule_name": rule_name.strip(),
                "priority": priority,
                "conditions": conditions,
                "source": source.strip(),
            }
        )

    normalized_rules = sorted(
        normalized_rules,
        key=lambda item: (item["priority"], item["rule_name"]),
    )

    return {
        "default_source": default_source.strip(),
        "rules": normalized_rules,
    }


def _get_first_non_null(series: pd.Series) -> object:
    """
    Return first non-null value or pd.NA.
    """
    values = series.dropna()
    if values.empty:
        return pd.NA
    return values.iloc[0]


def _select_longest_name(series: pd.Series) -> str:
    """
    Select the longest non-null product name.
    """
    names = (
        series.dropna()
        .astype(str)
        .str.strip()
        .tolist()
    )
    names = [name for name in names if name]

    if not names:
        raise DataValidationError("dim_product found product without product_name.")

    names_sorted = sorted(names, key=lambda x: (-len(x), x))
    return names_sorted[0]


def _normalize_product_name(value: object) -> str | None:
    """
    Build normalized_product_name for matching/debug/reporting support.
    """
    if pd.isna(value):
        return pd.NA

    text = str(value).strip().lower()
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"[^\w\sąćęłńóśźż]", "", text, flags=re.UNICODE)
    text = text.replace(" ", "_").strip("_")

    if not text:
        return pd.NA

    return text


def _resolve_product_type(
    series: pd.Series,
    product_business_key: str,
) -> object:
    """
    Resolve product_type from product_group_final.

    Rules:
    - normally one non-null value is expected
    - if multiple values exist, log a warning and choose the most frequent one
    """
    values = series.dropna().astype(str)

    if values.empty:
        return pd.NA

    unique_values = values.unique().tolist()

    if len(unique_values) > 1:
        log.warning(
            "%sPRODUCT TYPE WARNING: product '%s' has multiple product_group_final values: %s. "
            "Using the most frequent value.%s",
            YELLOW,
            product_business_key,
            unique_values,
            RESET,
        )

    counts = values.value_counts(dropna=True)
    return counts.index[0]


def _resolve_product_source(
    group: pd.DataFrame,
    source_rules: dict[str, Any],
    product_business_key: str,
) -> str:
    """
    Resolve source using ordered rules from product_source_rules.json.
    First matching rule wins, otherwise default_source is returned.
    """
    context = _build_source_rule_context(group)

    for rule in source_rules["rules"]:
        if _rule_matches(rule["conditions"], context):
            return rule["source"]

    default_source = source_rules["default_source"]

    if not default_source:
        raise DataValidationError(
            f"No source resolved for product '{product_business_key}' and no valid default_source configured."
        )

    return default_source


def _build_source_rule_context(group: pd.DataFrame) -> dict[str, Any]:
    """
    Build context used for source rule evaluation.
    """
    recipe_values = group["recipe_number_clean"].dropna()
    has_recipe_number = not recipe_values.empty

    product_codes: list[str] = []
    for column_name in ("product_code_raw", "product_code_clean"):
        if column_name not in group.columns:
            continue

        values = (
            group[column_name]
            .dropna()
            .astype(str)
            .str.strip()
            .tolist()
        )
        product_codes.extend(value for value in values if value)

    product_names: list[str] = []
    if "product_name_selected" in group.columns:
        selected_names = (
            group["product_name_selected"]
            .dropna()
            .astype(str)
            .str.strip()
            .tolist()
        )
        product_names.extend(value for value in selected_names if value)

    if "product_alternative_names" in group.columns:
        alt_values = (
            group["product_alternative_names"]
            .dropna()
            .astype(str)
            .tolist()
        )
        for value in alt_values:
            parts = [part.strip() for part in value.split("|") if part.strip()]
            product_names.extend(parts)

    product_groups: list[str] = []
    if "product_group_final" in group.columns:
        values = (
            group["product_group_final"]
            .dropna()
            .astype(str)
            .str.strip()
            .tolist()
        )
        product_groups.extend(value for value in values if value)

    return {
        "has_recipe_number": has_recipe_number,
        "product_codes": product_codes,
        "product_names": product_names,
        "product_groups": product_groups,
    }


def _rule_matches(
    conditions: dict[str, Any],
    context: dict[str, Any],
) -> bool:
    """
    Evaluate whether all conditions in a rule match the product context.
    """
    if "has_recipe_number" in conditions:
        if context["has_recipe_number"] != conditions["has_recipe_number"]:
            return False

    if "product_code_contains" in conditions:
        patterns = [pattern.upper() for pattern in conditions["product_code_contains"]]
        codes = [code.upper() for code in context["product_codes"]]

        if not any(pattern in code for code in codes for pattern in patterns):
            return False

    if "product_name_in" in conditions:
        valid_names = {name.strip().lower() for name in conditions["product_name_in"]}
        context_names = {name.strip().lower() for name in context["product_names"]}

        if context_names.isdisjoint(valid_names):
            return False

    if "product_name_regex" in conditions:
        patterns = conditions["product_name_regex"]
        names = context["product_names"]

        if not any(
            re.search(pattern, name, flags=re.IGNORECASE)
            for name in names
            for pattern in patterns
        ):
            return False

    if "product_group_in" in conditions:
        valid_groups = {group.strip() for group in conditions["product_group_in"]}
        context_groups = {group.strip() for group in context["product_groups"]}

        if context_groups.isdisjoint(valid_groups):
            return False

    return True


def _collect_alternative_names(group: pd.DataFrame) -> str | None:
    """
    Collect all unique names from:
    - product_name_selected
    - product_alternative_names
    """
    names: set[str] = set()

    selected_names = (
        group["product_name_selected"]
        .dropna()
        .astype(str)
        .str.strip()
        .tolist()
    )
    names.update(name for name in selected_names if name)

    alt_values = group["product_alternative_names"].dropna().astype(str).tolist()
    for value in alt_values:
        parts = [part.strip() for part in value.split("|") if part.strip()]
        names.update(parts)

    if not names:
        return pd.NA

    sorted_names = sorted(names)
    return " | ".join(sorted_names)


def _resolve_product_identity_type(series: pd.Series) -> str:
    """
    Resolve final product identity type for dim_product.

    Priority:
    RECIPE > NAME_MATCH > TEMPORARY
    """
    values = set(series.dropna().astype(str).tolist())

    if "RECIPE" in values:
        return "RECIPE"

    if "NAME_MATCH" in values:
        return "NAME_MATCH"

    if "TEMPORARY" in values:
        return "TEMPORARY"

    raise DataValidationError(
        "Could not resolve product_identity_type for dim_product."
    )