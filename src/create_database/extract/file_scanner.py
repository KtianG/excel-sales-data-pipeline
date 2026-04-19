from __future__ import annotations

from pathlib import Path


def scan_excel_files(input_dir: Path, extensions: tuple[str, ...]) -> list[Path]:
    """
    Return sorted list of Excel files from input directory.

    Rules:
    - only files directly in input_dir (no recursion)
    - file extension match is case-insensitive
    - returned list is sorted for deterministic processing
    """
    if not input_dir.exists():
        raise ValueError(f"Input directory does not exist: {input_dir}")

    if not input_dir.is_dir():
        raise ValueError(f"Input path is not a directory: {input_dir}")

    normalized_ext = {ext.lower() for ext in extensions}

    files: list[Path] = []

    for file in input_dir.iterdir():
        if not file.is_file():
            continue

        if file.suffix.lower() in normalized_ext:
            files.append(file)

    return sorted(files)