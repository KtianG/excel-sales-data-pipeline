from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path

from create_database.exceptions import SourceDataError


log = logging.getLogger(__name__)

YELLOW = "\033[93m"
RESET = "\033[0m"

DATE_PATTERN = re.compile(r"(\d{2}\.\d{2}\.\d{2})")
SPOT_ID_PATTERN = re.compile(r"#(\d+)\s*$")


@dataclass(frozen=True)
class FileMetadata:
    source_file: str
    report_date_raw: str


@dataclass(frozen=True)
class SheetMetadata:
    source_sheet: str
    source_sheet_clean: str
    spot_id_raw: int


def extract_file_metadata(file_path: Path) -> FileMetadata:
    """
    Extract metadata from filename, including raw report date in dd.mm.yy format.
    """
    match = DATE_PATTERN.search(file_path.name)
    if not match:
        log.warning(
            "%sSOURCE WARNING: Filename does not contain date dd.mm.yy: %s%s",
            YELLOW,
            file_path.name,
            RESET,
        )
        raise SourceDataError(
            f"Filename does not contain date dd.mm.yy: {file_path.name}"
        )

    return FileMetadata(
        source_file=file_path.name,
        report_date_raw=match.group(1),
    )


def normalize_sheet_name(sheet_name: str) -> str:
    """
    Normalize spacing around '#<spot_id>' in sheet names.
    """
    return re.sub(r"\s*#\s*", "#", str(sheet_name).strip())


def extract_sheet_metadata(sheet_name: str) -> SheetMetadata:
    """
    Extract cleaned sheet name and spot_id_raw from sheet name.
    """
    cleaned = normalize_sheet_name(sheet_name)
    match = SPOT_ID_PATTERN.search(cleaned)

    if not match:
        log.warning(
            "%sSOURCE WARNING: Sheet name does not contain valid '#spot_id': %s%s",
            YELLOW,
            sheet_name,
            RESET,
        )
        raise SourceDataError(
            f"Sheet name does not contain valid '#spot_id': {sheet_name}"
        )

    spot_id_raw = int(match.group(1))

    return SheetMetadata(
        source_sheet=sheet_name,
        source_sheet_clean=cleaned,
        spot_id_raw=spot_id_raw,
    )