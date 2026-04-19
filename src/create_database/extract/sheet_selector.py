from __future__ import annotations

import re


SPOT_SHEET_PATTERN = re.compile(r"#\d+")


def select_eligible_sheets(sheet_names: list[str]) -> list[str]:
    """
    Return sheets that represent sales points.

    Current rule:
    - sheet name must contain pattern '#<digits>' (e.g. '#123')

    This avoids accidental matches like 'backup#old'.
    """
    return [
        sheet
        for sheet in sheet_names
        if isinstance(sheet, str) and SPOT_SHEET_PATTERN.search(sheet)
    ]