from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]

DATA_DIR = PROJECT_ROOT / "data"
INPUT_DIR = DATA_DIR / "input"
DAILY_REPORTS_DIR = INPUT_DIR / "daily_reports"
OUTPUT_DIR = DATA_DIR / "output"
TEMP_DIR = DATA_DIR / "temp"

JSON_DIR = PROJECT_ROOT / "json"
DOCS_DIR = PROJECT_ROOT / "docs"