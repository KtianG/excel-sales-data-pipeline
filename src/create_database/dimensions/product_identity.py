from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from create_database.exceptions import DataValidationError


log = logging.getLogger(__name__)

YELLOW = "\033[93m"
RESET = "\033[0m"


REQUIRED_IDENTITY_COLUMNS = {
    "report_date",
    "product_work_key",
    "recipe_number_clean",
    "product_name_selected",
    "product_alternative_names",
    "product_group_final",
}


def build_product_identity(staging_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build product identity mapping from staging data.

    Rules:
    1. Process candidates chronologically from oldest to newest.
    2. Products with recipe number are resolved first and treated as stable.
    3. Products without recipe number are matched by exact normalized name:
        - only if product_group_final matches
    4. If no match is found, create a temporary product.
    5. If the same exact name appears with different product_group_final values,
       do not merge them. Create separate products and log a warning.

    Output columns:
    - product_work_key
    - product_business_key
    - product_identity_type
    - is_historical_stable
    - identity_match_source
    """

    if staging_df.empty:
        return pd.DataFrame(
            columns=[
            "product_work_key",
            "product_business_key",
            "product_identity_type",
            "is_historical_stable",
            "identity_match_source",
            ]
        )
    
    _validate_identity_input(staging_df)

    candidates_df = _prepare_product_candidates(staging_df)

    identity_rows: list[dict[str, Any]] = []

    products_by_key: dict[str, dict[str, Any]] = {}
    name_index: dict[str, list[str]] = {}

    for candidate in candidates_df.itertuples(index=False):
        product_work_key = candidate.product_work_key
        recipe_number = candidate.recipe_number_clean
        product_name = candidate.product_name_selected
        product_group = candidate.product_group_final
        first_seen_date = candidate.first_seen_date
        alternative_names = _split_alternative_names(candidate.product_alternative_names)

        if pd.notna(recipe_number):
            product_business_key = f"RECIPE_{recipe_number}"
            product_identity_type = "RECIPE"
            is_historical_stable = True
            identity_match_source = "RECIPE"

            if product_business_key not in products_by_key:
                products_by_key[product_business_key] = _build_product_state(
                    product_business_key=product_business_key,
                    product_identity_type=product_identity_type,
                    is_historical_stable=is_historical_stable,
                    recipe_number=recipe_number,
                    product_group_final=product_group,
                    first_seen_date=first_seen_date,
                    names=[product_name, *alternative_names],
                )
            else:
                _update_product_state(
                    product_state=products_by_key[product_business_key],
                    new_group=product_group,
                    new_names=[product_name, *alternative_names],
                    product_work_key=product_work_key,
                    warn_on_group_conflict=True,
                )

            _register_names_in_index(
                name_index=name_index,
                product_business_key=product_business_key,
                names=products_by_key[product_business_key]["names"],
            )

        else:
            normalized_name = _normalize_identity_name(product_name)
            can_match_by_name = _can_match_by_name(product_name)

            matched_product_business_key = None

            if can_match_by_name and normalized_name is not None:
                matched_product_business_key = _find_name_match(
                    normalized_name=normalized_name,
                    product_group_final=product_group,
                    name_index=name_index,
                    products_by_key=products_by_key,
                )

            if matched_product_business_key is not None:
                product_business_key = matched_product_business_key
                product_identity_type = "NAME_MATCH"
                is_historical_stable = False
                identity_match_source = "NAME_MATCH"

                _update_product_state(
                    product_state=products_by_key[product_business_key],
                    new_group=product_group,
                    new_names=[product_name, *alternative_names],
                    product_work_key=product_work_key,
                    warn_on_group_conflict=True,
                )

                _register_names_in_index(
                    name_index=name_index,
                    product_business_key=product_business_key,
                    names=products_by_key[product_business_key]["names"],
                )
            else:
                product_business_key = f"TEMP_{product_work_key}"
                product_identity_type = "TEMPORARY"
                is_historical_stable = False
                identity_match_source = "TEMP"

                products_by_key[product_business_key] = _build_product_state(
                    product_business_key=product_business_key,
                    product_identity_type=product_identity_type,
                    is_historical_stable=is_historical_stable,
                    recipe_number=pd.NA,
                    product_group_final=product_group,
                    first_seen_date=first_seen_date,
                    names=[product_name, *alternative_names],
                )

                _register_names_in_index(
                    name_index=name_index,
                    product_business_key=product_business_key,
                    names=products_by_key[product_business_key]["names"],
                )

        identity_rows.append(
            {
                "product_work_key": product_work_key,
                "product_business_key": product_business_key,
                "product_identity_type": product_identity_type,
                "is_historical_stable": is_historical_stable,
                "identity_match_source": identity_match_source,
            }
        )

    identity_df = pd.DataFrame(identity_rows).drop_duplicates(
        subset=["product_work_key"],
        keep="first",
    )

    if identity_df["product_work_key"].duplicated().any():
        raise DataValidationError("Duplicate product_work_key values in product identity output.")

    return identity_df


def _validate_identity_input(df: pd.DataFrame) -> None:
    missing = REQUIRED_IDENTITY_COLUMNS - set(df.columns)
    if missing:
        raise DataValidationError(
            f"product_identity input is missing required columns: {sorted(missing)}"
        )


def _prepare_product_candidates(staging_df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare unique product candidates from staging data.

    One candidate per product_work_key with:
    - earliest appearance date
    - selected name
    - recipe number
    - product group
    - alternative names
    """
    candidates = (
        staging_df.loc[
            :,
            [
                "report_date",
                "product_work_key",
                "recipe_number_clean",
                "product_name_selected",
                "product_alternative_names",
                "product_group_final",
            ],
        ]
        .copy()
        .rename(columns={"report_date": "first_seen_date"})
    )

    candidates["first_seen_date"] = pd.to_datetime(
        candidates["first_seen_date"], errors="coerce"
    )

    candidates = (
        candidates.sort_values(
            by=["first_seen_date", "product_work_key"],
            ascending=[True, True],
            kind="mergesort",
        )
        .drop_duplicates(subset=["product_work_key"], keep="first")
        .reset_index(drop=True)
    )

    candidates["has_recipe_number"] = candidates["recipe_number_clean"].notna()

    candidates = candidates.sort_values(
        by=["first_seen_date", "has_recipe_number", "product_work_key"],
        ascending=[True, False, True],
        kind="mergesort",
    ).reset_index(drop=True)

    return candidates


def _build_product_state(
    product_business_key: str,
    product_identity_type: str,
    is_historical_stable: bool,
    recipe_number: object,
    product_group_final: object,
    first_seen_date: object,
    names: list[object],
) -> dict[str, Any]:
    """
    Internal state of a product tracked during identity matching.
    """
    clean_names = _prepare_name_set(names)

    return {
        "product_business_key": product_business_key,
        "product_identity_type": product_identity_type,
        "is_historical_stable": is_historical_stable,
        "recipe_number": recipe_number,
        "product_group_final": product_group_final,
        "first_seen_date": first_seen_date,
        "names": clean_names,
    }


def _update_product_state(
    product_state: dict[str, Any],
    new_group: object,
    new_names: list[object],
    product_work_key: str,
    warn_on_group_conflict: bool = True,
) -> None:
    """
    Update existing product state with additional names.

    The caller is responsible for controlling match eligibility.
    This function only logs a warning if a group conflict appears
    for an already matched product.
    """

    existing_group = product_state["product_group_final"]

    if (
        warn_on_group_conflict
        and pd.notna(existing_group)
        and pd.notna(new_group)
        and str(existing_group) != str(new_group)
    ):
        log.warning(
            "%sPRODUCT GROUP WARNING: product_work_key '%s' matched existing product '%s' "
            "but product_group_final differs ('%s' vs '%s').%s",
            YELLOW,
            product_work_key,
            product_state["product_business_key"],
            existing_group,
            new_group,
            RESET,
        )

    product_state["names"].update(_prepare_name_set(new_names))


def _find_name_match(
    normalized_name: str,
    product_group_final: object,
    name_index: dict[str, list[str]],
    products_by_key: dict[str, dict[str, Any]],
) -> str | None:
    """
    Find an exact name match only if product_group_final is the same.
    """
    candidate_keys = name_index.get(normalized_name, [])

    if not candidate_keys:
        return None

    for product_business_key in candidate_keys:
        existing_group = products_by_key[product_business_key]["product_group_final"]

        if _same_group(existing_group, product_group_final):
            return product_business_key

    return None


def _register_names_in_index(
    name_index: dict[str, list[str]],
    product_business_key: str,
    names: set[str],
) -> None:
    """
    Register normalized names in index.
    All normalized names are indexed for exact matching.
    """
    for name in names:
        normalized_name = _normalize_identity_name(name)

        if normalized_name is None:
            continue

        if normalized_name not in name_index:
            name_index[normalized_name] = []

        if product_business_key not in name_index[normalized_name]:
            name_index[normalized_name].append(product_business_key)


def _prepare_name_set(names: list[object]) -> set[str]:
    """
    Normalize list of names into clean unique set.
    """
    result: set[str] = set()

    for name in names:
        if pd.isna(name):
            continue

        text = str(name).strip()
        if not text:
            continue

        result.add(text)

    return result


def _split_alternative_names(value: object) -> list[str]:
    """
    Split 'a | b | c' string into list of names.
    """
    if pd.isna(value):
        return []

    text = str(value).strip()
    if not text:
        return []

    return [part.strip() for part in text.split("|") if part.strip()]


def _normalize_identity_name(value: object) -> str | None:
    """
    Normalize name for exact identity matching.

    Rules:
    - strip
    - lowercase
    - collapse repeated spaces
    """
    if pd.isna(value):
        return None

    text = str(value).strip().lower()
    text = " ".join(text.split())

    if not text:
        return None

    return text


def _can_match_by_name(value: object) -> bool:
    """
    Determine if a product can be matched by name.

    Current rule:
    - any non-empty normalized name is eligible
    """
    normalized_name = _normalize_identity_name(value)
    return normalized_name is not None


def _same_group(left: object, right: object) -> bool:
    """
    Compare product_group_final values with NA-awareness.
    """
    if pd.isna(left) and pd.isna(right):
        return True

    if pd.isna(left) or pd.isna(right):
        return False

    return str(left) == str(right)