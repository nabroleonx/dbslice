from __future__ import annotations

import datetime
import re
from typing import Any

# Per 45 CFR 164.514(b)(2)(i)(B): Geographic data smaller than state must be
# removed, EXCEPT the initial 3 digits of a ZIP code may be retained if the
# geographic unit formed by combining all ZIP codes with the same 3 initial
# digits contains more than 20,000 people.
#
# The following 3-digit ZIP prefixes have population < 20,000 per US Census
# and must be changed to "000" under Safe Harbor.
#
# Source: US Census Bureau, derived from ZCTA population data.
# These prefixes are stable across census cycles. Last verified: 2020 Census.

_LOW_POPULATION_ZIP3: frozenset[str] = frozenset({
    "036",  # NH
    "059",  # MT
    "063",  # VT/NH
    "102",  # NY (small area)
    "203",  # DC (small overlap)
    "556",  # MN
    "692",  # NE
    "790",  # TX (small area)
    "821",  # WY
    "823",  # WY
    "830",  # WY
    "831",  # WY
    "878",  # NM
    "879",  # NM
    "884",  # NM
    "890",  # NV
    "893",  # NV
})


def hipaa_safe_harbor_zip3(value: Any) -> str:
    """
    HIPAA Safe Harbor ZIP code transformation.

    Retains only the first 3 digits of a ZIP code. If the 3-digit prefix
    has population < 20,000 (per Census data), returns "000" instead.

    Per 45 CFR 164.514(b)(2)(i)(B).

    Args:
        value: Original ZIP code (string or int)

    Returns:
        3-digit ZIP prefix, or "000" if low-population area
    """
    raw = str(value).strip()
    # Extract digits only (handles "12345-6789" format)
    digits = re.sub(r"[^0-9]", "", raw)
    if len(digits) < 3:
        return "000"

    prefix = digits[:3]
    if prefix in _LOW_POPULATION_ZIP3:
        return "000"
    return prefix


def year_only(value: Any) -> str:
    """
    HIPAA Safe Harbor date transformation.

    Extracts only the year from a date value. Per 45 CFR 164.514(b)(2)(i)(C),
    all date elements (except year) must be removed for dates directly related
    to an individual.

    Args:
        value: Original date value (date, datetime, string, or int)

    Returns:
        Year string (e.g., "1985")
    """
    if value is None:
        return ""

    # datetime/date objects
    if isinstance(value, (datetime.datetime, datetime.date)):
        return str(value.year)

    raw = str(value).strip()

    # ISO format: 2024-03-15 or 2024-03-15T10:30:00
    iso_match = re.match(r"(\d{4})-\d{2}-\d{2}", raw)
    if iso_match:
        return iso_match.group(1)

    # US format: 03/15/2024 or 03-15-2024
    us_match = re.match(r"\d{1,2}[/-]\d{1,2}[/-](\d{4})", raw)
    if us_match:
        return us_match.group(1)

    # Just a 4-digit year
    year_match = re.match(r"^(\d{4})$", raw)
    if year_match:
        return year_match.group(1)

    # Fallback: try to find any 4-digit year in the string
    any_year = re.search(r"\b(19|20)\d{2}\b", raw)
    if any_year:
        return any_year.group(0)

    return ""


def age_bucket(value: Any) -> str:
    """
    HIPAA Safe Harbor age bucketing.

    Per 45 CFR 164.514(b)(2)(i)(C), ages over 89 must be aggregated into
    a single category of "90 or over."

    Args:
        value: Age as integer or string

    Returns:
        Original age as string if <= 89, or "90+" if > 89
    """
    try:
        age = int(value)
    except (ValueError, TypeError):
        return str(value)

    if age > 89:
        return "90+"
    return str(age)


_FREETEXT_REDACTION_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    # Email
    (re.compile(r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b"), "[REDACTED_EMAIL]"),
    # SSN
    (re.compile(r"\b\d{3}-\d{2}-\d{4}\b"), "[REDACTED_SSN]"),
    # US Phone
    (re.compile(r"\b(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b"), "[REDACTED_PHONE]"),
    # Credit card (with separators)
    (re.compile(r"(?<!\d)(?:\d[\s-]?){13,19}(?!\d)"), "[REDACTED_PAN]"),
    # IPv4
    (re.compile(
        r"\b(?:(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\b"
    ), "[REDACTED_IP]"),
]


def redact_freetext(value: Any) -> str:
    """
    Inline PII redaction for free-text fields.

    Replaces detected PII patterns with placeholder tokens while preserving
    the surrounding text structure. This is for NOT NULL text columns where
    NULLing is not possible.

    Args:
        value: Original text value

    Returns:
        Text with PII patterns replaced by [REDACTED_*] placeholders
    """
    if value is None:
        return ""

    text = str(value)
    for pattern, replacement in _FREETEXT_REDACTION_PATTERNS:
        text = pattern.sub(replacement, text)
    return text


BINARY_SENTINEL = b"\x00"
"""Sentinel value for NOT NULL binary columns when compliance requires NULLing."""


CUSTOM_TRANSFORMERS: dict[str, Any] = {
    "hipaa_zip3": hipaa_safe_harbor_zip3,
    "year_only": year_only,
    "age_bucket": age_bucket,
    "redact_freetext": redact_freetext,
}
