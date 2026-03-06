from dataclasses import dataclass, field


@dataclass(frozen=True)
class ComplianceProfile:
    """A compliance profile defining anonymization requirements for a regulatory framework."""

    name: str
    """Profile identifier (e.g., 'gdpr', 'hipaa', 'pci-dss')."""

    display_name: str
    """Human-readable name (e.g., 'GDPR', 'HIPAA Safe Harbor')."""

    description: str
    """Brief description of what this profile covers."""

    required_column_patterns: dict[str, str] = field(default_factory=dict)
    """Column name substring -> Faker provider mappings that MUST be anonymized."""

    required_null_patterns: list[str] = field(default_factory=list)
    """Column name patterns that must be NULLed (security-sensitive data)."""

    value_scan_patterns: list[str] = field(default_factory=list)
    """Names of value-based PII scanner patterns to run (e.g., 'email', 'ssn', 'credit_card')."""

    warn_freetext_columns: list[str] = field(default_factory=list)
    """Column name patterns that may contain embedded PII in free text."""

    identifiers: list[str] = field(default_factory=list)
    """List of identifier categories this profile covers (for compliance reports)."""


GDPR_PROFILE = ComplianceProfile(
    name="gdpr",
    display_name="GDPR",
    description=(
        "EU General Data Protection Regulation. Covers direct identifiers and "
        "flags quasi-identifiers that could enable singling out or linkage attacks."
    ),
    required_column_patterns={
        # Direct identifiers
        "email": "email",
        "first_name": "first_name",
        "last_name": "last_name",
        "firstname": "first_name",
        "lastname": "last_name",
        "full_name": "name",
        "fullname": "name",
        "name": "name",
        "phone": "phone_number",
        "mobile": "phone_number",
        "fax": "phone_number",
        # Address / location
        "address": "address",
        "street": "street_address",
        "city": "city",
        "zip": "zipcode",
        "zipcode": "zipcode",
        "postal": "zipcode",
        # Identity documents
        "ssn": "ssn",
        "passport": "passport_number",
        "driver_license": "license_plate",
        # Financial
        "credit_card": "credit_card_number",
        "card_number": "credit_card_number",
        "iban": "iban",
        "bank_account": "bban",
        "account_number": "bban",
        # Network identifiers
        "ip_address": "ipv4",
        "ipaddress": "ipv4",
        "ip": "ipv4",
        "ipv6": "ipv6",
        "mac_address": "mac_address",
        # Online identifiers
        "username": "user_name",
        "user_name": "user_name",
        # Biographic
        "dob": "date_of_birth",
        "date_of_birth": "date_of_birth",
        "birthdate": "date_of_birth",
        "birth_date": "date_of_birth",
    },
    required_null_patterns=[
        "password",
        "passwd",
        "pwd",
        "hash",
        "salt",
        "token",
        "secret",
        "api_key",
        "apikey",
        "private_key",
        "public_key",
        "certificate",
        "session_id",
    ],
    value_scan_patterns=["email", "phone", "ipv4", "ipv6"],
    warn_freetext_columns=[
        "note",
        "notes",
        "comment",
        "comments",
        "description",
        "message",
        "body",
        "content",
        "text",
        "bio",
        "about",
        "reason",
        "feedback",
        "review",
    ],
    identifiers=[
        "Names",
        "Email addresses",
        "Phone numbers",
        "Physical addresses",
        "IP addresses",
        "Date of birth",
        "Identity documents (SSN, passport)",
        "Financial identifiers (credit card, IBAN)",
        "Online identifiers (username)",
        "Biometric identifiers (flagged via value scan)",
    ],
)

HIPAA_PROFILE = ComplianceProfile(
    name="hipaa",
    display_name="HIPAA Safe Harbor",
    description=(
        "HIPAA Safe Harbor de-identification method. Requires removal or masking "
        "of all 18 specified identifier types per 45 CFR 164.514(b)(2)."
    ),
    required_column_patterns={
        # 1. Names
        "name": "name",
        "first_name": "first_name",
        "last_name": "last_name",
        "firstname": "first_name",
        "lastname": "last_name",
        "full_name": "name",
        "fullname": "name",
        # 2. Geographic (smaller than state) — Safe Harbor requires ZIP3 with population check
        "address": "address",
        "street": "street_address",
        "city": "city",
        "zip": "hipaa_zip3",
        "zipcode": "hipaa_zip3",
        "postal": "hipaa_zip3",
        "county": "city",
        # 3. Dates (except year) — Safe Harbor requires year-only
        "dob": "year_only",
        "date_of_birth": "year_only",
        "birthdate": "year_only",
        "birth_date": "year_only",
        "admission_date": "year_only",
        "discharge_date": "year_only",
        "death_date": "year_only",
        "service_date": "year_only",
        "visit_date": "year_only",
        # 4. Phone numbers
        "phone": "phone_number",
        "mobile": "phone_number",
        "telephone": "phone_number",
        "cell": "phone_number",
        # 5. Fax numbers
        "fax": "phone_number",
        # 6. Email addresses
        "email": "email",
        # 7. SSN
        "ssn": "ssn",
        "social_security": "ssn",
        # 8. Medical record numbers
        "medical_record": "pystr",
        "mrn": "pystr",
        "patient_id": "pystr",
        # 9. Health plan beneficiary numbers
        "beneficiary": "pystr",
        "member_id": "pystr",
        "subscriber_id": "pystr",
        # 10. Account numbers
        "account_number": "bban",
        "bank_account": "bban",
        # 11. Certificate/license numbers
        "license_number": "license_plate",
        "certificate_number": "pystr",
        "driver_license": "license_plate",
        "passport": "passport_number",
        # 12. Vehicle identifiers
        "vin": "pystr",
        "vehicle_id": "pystr",
        "license_plate": "license_plate",
        # 13. Device identifiers
        "device_id": "pystr",
        "serial_number": "pystr",
        "device_serial": "pystr",
        # 14. Web URLs
        "url": "url",
        "website": "url",
        # 15. IP addresses
        "ip_address": "ipv4",
        "ipaddress": "ipv4",
        "ip": "ipv4",
        "ipv6": "ipv6",
        # 16. Biometric identifiers (column names are hints)
        "fingerprint": "pystr",
        "biometric": "pystr",
        "retina": "pystr",
        "voiceprint": "pystr",
        # 17. Full-face photographs (binary columns - flag as warning)
        # 18. Any other unique identifier
        "unique_id": "pystr",
    },
    required_null_patterns=[
        "password",
        "passwd",
        "pwd",
        "hash",
        "salt",
        "token",
        "secret",
        "api_key",
        "apikey",
        "private_key",
        "public_key",
        "certificate",
        "session_id",
    ],
    value_scan_patterns=["email", "ssn", "phone", "credit_card", "ipv4", "ipv6"],
    warn_freetext_columns=[
        "note",
        "notes",
        "comment",
        "comments",
        "description",
        "message",
        "body",
        "content",
        "text",
        "diagnosis",
        "treatment",
        "history",
        "narrative",
        "clinical_notes",
        "progress_notes",
        "discharge_summary",
    ],
    identifiers=[
        "1. Names",
        "2. Geographic data (smaller than state)",
        "3. Dates (except year)",
        "4. Phone numbers",
        "5. Fax numbers",
        "6. Email addresses",
        "7. Social Security numbers",
        "8. Medical record numbers",
        "9. Health plan beneficiary numbers",
        "10. Account numbers",
        "11. Certificate/license numbers",
        "12. Vehicle identifiers",
        "13. Device identifiers",
        "14. Web URLs",
        "15. IP addresses",
        "16. Biometric identifiers",
        "17. Full-face photographs (flag only)",
        "18. Any other unique identifying number",
    ],
)

PCI_DSS_PROFILE = ComplianceProfile(
    name="pci-dss",
    display_name="PCI-DSS v4.0",
    description=(
        "Payment Card Industry Data Security Standard v4.0. "
        "Real PANs are PROHIBITED in dev/test environments (Req 6.5.6). "
        "Cardholder data must be fully replaced with synthetic data."
    ),
    required_column_patterns={
        # Primary Account Number (PAN)
        "credit_card": "credit_card_number",
        "card_number": "credit_card_number",
        "card_num": "credit_card_number",
        "pan": "credit_card_number",
        "account_number": "bban",
        # Cardholder name
        "cardholder": "name",
        "card_holder": "name",
        "cardholder_name": "name",
        # Expiration
        "expiry": "credit_card_expire",
        "expiration": "credit_card_expire",
        "exp_date": "credit_card_expire",
        "card_expiry": "credit_card_expire",
        # Service code (3-4 digit)
        "service_code": "pystr",
        "cvv": "credit_card_security_code",
        "cvc": "credit_card_security_code",
        "cvv2": "credit_card_security_code",
    },
    required_null_patterns=[
        # Sensitive authentication data - MUST be removed post-authorization
        "pin",
        "pin_block",
        "pin_number",
        "cvv",
        "cvc",
        "cvv2",
        "cvc2",
        "magnetic_stripe",
        "track_data",
        "track1",
        "track2",
    ],
    value_scan_patterns=["credit_card"],
    warn_freetext_columns=[
        "note",
        "notes",
        "comment",
        "description",
        "transaction_detail",
        "memo",
    ],
    identifiers=[
        "Primary Account Number (PAN)",
        "Cardholder name",
        "Expiration date",
        "Service code",
        "Sensitive authentication data (CVV/PIN)",
    ],
)


_PROFILES: dict[str, ComplianceProfile] = {
    "gdpr": GDPR_PROFILE,
    "hipaa": HIPAA_PROFILE,
    "pci-dss": PCI_DSS_PROFILE,
}


def get_profile(name: str) -> ComplianceProfile:
    """
    Get a compliance profile by name.

    Args:
        name: Profile name (case-insensitive)

    Returns:
        ComplianceProfile

    Raises:
        ValueError: If profile not found
    """
    profile = _PROFILES.get(name.lower())
    if profile is None:
        available = ", ".join(sorted(_PROFILES.keys()))
        raise ValueError(f"Unknown compliance profile '{name}'. Available: {available}")
    return profile


def list_profiles() -> list[ComplianceProfile]:
    """Return all available compliance profiles."""
    return list(_PROFILES.values())
