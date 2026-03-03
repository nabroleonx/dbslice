import os
import re
from pathlib import Path
from typing import TextIO


def parse_file_mode(mode: int | str) -> int:
    """Parse a file mode from int or octal string."""
    if isinstance(mode, int):
        parsed = mode
    else:
        normalized = mode.strip().lower()
        if normalized.startswith("0o"):
            normalized = normalized[2:]
        if not re.fullmatch(r"[0-7]{3,4}", normalized):
            raise ValueError(
                "File mode must be an octal value like 600 or 0o600 (range 000-777)"
            )
        parsed = int(normalized, 8)

    if parsed < 0 or parsed > 0o777:
        raise ValueError("File mode must be between 000 and 777 (octal)")

    return parsed


def open_text_file_secure(
    path: str | Path,
    file_mode: int,
    encoding: str = "utf-8",
) -> TextIO:
    """
    Open a text file for writing with explicit permissions.

    Uses os.open/os.fdopen so the requested mode is applied on creation,
    and reapplied to existing files for deterministic hardening behavior.
    """
    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
    fd = os.open(Path(path), flags, file_mode)
    try:
        # Enforce permissions even when the file already exists.
        os.fchmod(fd, file_mode)
    except OSError:
        # Keep best effort behavior on platforms that may not support fchmod.
        pass

    return os.fdopen(fd, "w", encoding=encoding)


def write_text_file_secure(
    path: str | Path,
    content: str,
    file_mode: int,
    encoding: str = "utf-8",
) -> None:
    """Write text to a file with explicit permissions."""
    with open_text_file_secure(path, file_mode=file_mode, encoding=encoding) as f:
        f.write(content)
