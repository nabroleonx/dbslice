import re
from dataclasses import dataclass, field
from typing import Any


@dataclass
class PIIDetection:
    """A single PII detection result."""

    table: str
    column: str
    pattern_name: str
    match_count: int
    sample_size: int
    confidence: str  # "high", "medium", "low"

    @property
    def match_rate(self) -> float:
        """Fraction of sampled values that matched."""
        if self.sample_size == 0:
            return 0.0
        return self.match_count / self.sample_size


# Compiled regex patterns for PII detection
_PII_PATTERNS: dict[str, tuple[re.Pattern[str], str]] = {
    "email": (
        re.compile(r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b"),
        "high",
    ),
    "ssn": (
        re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
        "high",
    ),
    "phone": (
        re.compile(r"\b(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b"),
        "medium",
    ),
    "ipv4": (
        re.compile(r"\b(?:(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\b"),
        "medium",
    ),
    "ipv6": (
        re.compile(r"\b(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}\b"),
        "medium",
    ),
    "credit_card": (
        re.compile(r"(?<!\d)(?:\d[\s-]?){13,19}(?!\d)"),
        "high",  # Confidence raised only if Luhn passes
    ),
}

_PAN_CANDIDATE_RE = re.compile(r"(?<!\d)(?:\d[\s-]?){13,19}(?!\d)")


def _luhn_check(number: str) -> bool:
    """Validate a number string using the Luhn algorithm."""
    digits = [int(d) for d in number if d.isdigit()]
    if len(digits) < 13:
        return False
    checksum = 0
    for i, d in enumerate(reversed(digits)):
        if i % 2 == 1:
            d *= 2
            if d > 9:
                d -= 9
        checksum += d
    return checksum % 10 == 0


def _extract_pan_candidates(text: str) -> list[str]:
    """Extract PAN-like candidates and normalize separators before Luhn checks."""
    candidates: list[str] = []
    for match in _PAN_CANDIDATE_RE.findall(text):
        digits_only = "".join(ch for ch in match if ch.isdigit())
        if 13 <= len(digits_only) <= 19:
            candidates.append(digits_only)
    return candidates


@dataclass
class PIIScanner:
    """
    Scans data values for PII using regex patterns.

    Usage:
        scanner = PIIScanner(patterns=["email", "ssn", "credit_card"])
        detections = scanner.scan_column("users", "notes", sample_values)
    """

    patterns: list[str] = field(default_factory=lambda: list(_PII_PATTERNS.keys()))
    """Which PII patterns to scan for."""

    min_match_rate: float = 0.1
    """Minimum fraction of values that must match to report a detection (default: 10%)."""

    def scan_column(
        self,
        table: str,
        column: str,
        values: list[Any],
    ) -> list[PIIDetection]:
        """
        Scan a list of column values for PII patterns.

        Args:
            table: Table name
            column: Column name
            values: Sample of values from the column

        Returns:
            List of PIIDetection results for patterns that matched
        """
        # Only scan string-like values
        str_values = [str(v) for v in values if v is not None and str(v).strip()]
        if not str_values:
            return []

        detections: list[PIIDetection] = []
        sample_size = len(str_values)

        for pattern_name in self.patterns:
            if pattern_name not in _PII_PATTERNS:
                continue

            regex, base_confidence = _PII_PATTERNS[pattern_name]
            match_count = 0

            for val in str_values:
                if pattern_name == "credit_card":
                    pan_candidates = _extract_pan_candidates(val)
                    if any(_luhn_check(candidate) for candidate in pan_candidates):
                        match_count += 1
                else:
                    matches = regex.findall(val)
                    if matches:
                        match_count += 1

            if match_count == 0:
                continue

            match_rate = match_count / sample_size
            if match_rate < self.min_match_rate:
                continue

            # Adjust confidence based on match rate
            if match_rate >= 0.8:
                confidence = "high"
            elif match_rate >= 0.3:
                confidence = base_confidence
            else:
                confidence = "low" if base_confidence == "medium" else "medium"

            detections.append(
                PIIDetection(
                    table=table,
                    column=column,
                    pattern_name=pattern_name,
                    match_count=match_count,
                    sample_size=sample_size,
                    confidence=confidence,
                )
            )

        return detections

    def scan_rows(
        self,
        table: str,
        rows: list[dict[str, Any]],
        skip_columns: set[str] | None = None,
    ) -> list[PIIDetection]:
        """
        Scan all text columns in a set of rows for PII.

        Args:
            table: Table name
            rows: List of row dictionaries
            skip_columns: Columns to skip (e.g., already anonymized)

        Returns:
            List of PIIDetection results
        """
        if not rows:
            return []

        skip = skip_columns or set()
        all_detections: list[PIIDetection] = []

        # Collect values per column
        columns: dict[str, list[Any]] = {}
        for row in rows:
            for col, val in row.items():
                if col in skip:
                    continue
                if val is not None and isinstance(val, (str, int, float)):
                    columns.setdefault(col, []).append(val)

        for col, values in columns.items():
            detections = self.scan_column(table, col, values)
            all_detections.extend(detections)

        return all_detections
