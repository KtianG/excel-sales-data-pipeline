from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from create_database.exceptions import ConfigurationError


log = logging.getLogger(__name__)

RED = "\033[91m"
RESET = "\033[0m"


def load_json(path: Path) -> Any:
    """
    Load JSON content from file.

    Raises ConfigurationError on any failure.
    """
    if not path.exists():
        log.error(
            "%sCRITICAL: JSON file not found: %s%s",
            RED,
            path,
            RESET,
        )
        raise ConfigurationError(f"JSON file not found: {path}")

    if not path.is_file():
        log.error(
            "%sCRITICAL: Path is not a file: %s%s",
            RED,
            path,
            RESET,
        )
        raise ConfigurationError(f"Path is not a file: {path}")

    try:
        with path.open("r", encoding="utf-8") as file:
            return json.load(file)

    except json.JSONDecodeError as exc:
        log.error(
            "%sCRITICAL: Invalid JSON format in file: %s%s",
            RED,
            path,
            RESET,
        )
        raise ConfigurationError(
            f"Invalid JSON format in file: {path}"
        ) from exc

    except Exception as exc:
        log.error(
            "%sCRITICAL: Failed to read JSON file: %s%s",
            RED,
            path,
            RESET,
        )
        raise ConfigurationError(
            f"Failed to read JSON file: {path}"
        ) from exc
