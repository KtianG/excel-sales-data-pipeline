from __future__ import annotations

import pandas as pd


KNOWN_NUMERIC_COLUMNS = [
    "Przyjęto",
    "Sprzedaż",
    "Suma",
    "Brutto",
    "Zwrócono",
    "koszt-jednostka",
    "koszt-przyjęcie",
    "Paczki",
    "Paczki 1",
    "Paczki 2",
    "Uszkodzenia - Przyjęcie",
    "Uszkodzenia - Zwrot",
]


def normalize_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert known metric columns to numeric where present.

    Rules:
    - convert configured metric columns if they exist
    - convert percentage columns if they exist
    - normalize decimal comma to decimal point before conversion
    """
    if df.empty:
        return df.copy()

    out = df.copy()

    numeric_cols = [col for col in KNOWN_NUMERIC_COLUMNS if col in out.columns]
    percent_cols = [col for col in out.columns if "%" in str(col)]

    cols_to_convert = list(dict.fromkeys(numeric_cols + percent_cols))

    for col in cols_to_convert:
        series = out[col]

        if pd.api.types.is_object_dtype(series) or pd.api.types.is_string_dtype(series):
            series = series.astype(str).str.replace(",", ".", regex=False).str.strip()

        out[col] = pd.to_numeric(series, errors="coerce")

    return out