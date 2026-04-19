from __future__ import annotations

import logging
import sys
import traceback

from create_database.exceptions import CreateDatabaseError
from create_database.pipeline import run_pipeline


RED = "\033[91m"
RESET = "\033[0m"


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s: %(message)s",
    )


def main() -> None:
    configure_logging()

    try:
        run_pipeline()

    except CreateDatabaseError as e:
        logging.error(
            "%sPIPELINE ERROR: %s%s",
            RED,
            str(e),
            RESET,
        )
        sys.exit(1)

    except Exception as e:
        logging.error(
            "%sUNEXPECTED ERROR: %s%s",
            RED,
            str(e),
            RESET,
        )

        # Short traceback (clean, without full stack dump)
        short_tb = "".join(traceback.format_exception_only(type(e), e)).strip()
        logging.error("%s%s%s", RED, short_tb, RESET)

        sys.exit(1)