from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

from create_database.exceptions import SourceDataError
from create_database.extract.metadata import extract_file_metadata, extract_sheet_metadata
from create_database.extract.sheet_selector import select_eligible_sheets


log = logging.getLogger(__name__)

YELLOW = "\033[93m"
RESET = "\033[0m"


def _choose_engine(file_path: Path) -> str:
    """
    Choose the pandas Excel engine based on file extension.
    """
    suffix = file_path.suffix.lower()

    if suffix == ".xlsx":
        return "openpyxl"
    if suffix == ".xls":
        return "xlrd"

    log.warning(
        "%sSOURCE FILE ERROR: Unsupported file extension: %s%s",
        YELLOW,
        file_path.suffix,
        RESET,
    )
    raise SourceDataError(f"Unsupported file extension: {file_path.suffix}")


def read_single_excel_file(file_path: Path) -> pd.DataFrame:
    """
    Read all eligible sheets from a single Excel file
    and return one combined raw extract DataFrame.

    No business cleaning is applied here.
    """
    file_meta = extract_file_metadata(file_path)
    engine = _choose_engine(file_path)
    load_timestamp = datetime.now()

    try:
        with pd.ExcelFile(file_path, engine=engine) as excel_file:
            eligible_sheets = select_eligible_sheets(excel_file.sheet_names)

            if not eligible_sheets:
                log.info("No eligible sheets found in file: %s", file_path.name)
                return pd.DataFrame()

            frames: list[pd.DataFrame] = []

            for sheet_name in eligible_sheets:
                sheet_meta = extract_sheet_metadata(sheet_name)

                try:
                    df = excel_file.parse(sheet_name=sheet_name)
                except Exception as exc:
                    log.warning(
                        "%sSOURCE SHEET ERROR: Could not parse sheet '%s' in file '%s'%s",
                        YELLOW,
                        sheet_name,
                        file_path.name,
                        RESET,
                    )
                    raise SourceDataError(
                        f"Could not parse sheet '{sheet_name}' in file '{file_path.name}'"
                    ) from exc

                df = df.copy()

                df["source_file"] = file_meta.source_file
                df["source_sheet"] = sheet_meta.source_sheet
                df["source_sheet_clean"] = sheet_meta.source_sheet_clean
                df["report_date_raw"] = file_meta.report_date_raw
                df["spot_id_raw"] = sheet_meta.spot_id_raw
                df["load_timestamp"] = load_timestamp

                frames.append(df)

    except SourceDataError:
        raise
    except Exception as exc:
        log.warning(
            "%sSOURCE FILE ERROR: Could not open Excel file: %s%s",
            YELLOW,
            file_path.name,
            RESET,
        )
        raise SourceDataError(
            f"Could not open Excel file: {file_path.name}"
        ) from exc

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, axis=0, join="outer", ignore_index=True)


def read_all_excel_files(file_paths: list[Path]) -> pd.DataFrame:
    """
    Read all Excel files and combine them into one raw extract DataFrame.
    """
    if not file_paths:
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []

    for file_path in file_paths:
        df = read_single_excel_file(file_path)
        if not df.empty:
            frames.append(df)

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, axis=0, join="outer", ignore_index=True)