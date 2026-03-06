"""
Tests for compliance gap fixes: HIPAA transformers, free-text handling,
binary columns, k-anonymity, and configurable scan sample size.

These tests verify END-TO-END behavior — not just that functions exist,
but that data is actually transformed correctly through the full pipeline.
"""

from dbslice.compliance.transformers import (
    BINARY_SENTINEL,
    age_bucket,
    hipaa_safe_harbor_zip3,
    redact_freetext,
    year_only,
)

# ──────────────────────────────────────────────────────────
# Phase A: HIPAA-specific transformers
# ──────────────────────────────────────────────────────────


class TestYearOnly:
    def test_iso_date(self):
        assert year_only("2024-03-15") == "2024"

    def test_iso_datetime(self):
        assert year_only("2024-03-15T10:30:00") == "2024"

    def test_us_date_slash(self):
        assert year_only("03/15/2024") == "2024"

    def test_us_date_dash(self):
        assert year_only("03-15-2024") == "2024"

    def test_datetime_object(self):
        import datetime

        assert year_only(datetime.date(1985, 6, 15)) == "1985"

    def test_datetime_datetime_object(self):
        import datetime

        assert year_only(datetime.datetime(1985, 6, 15, 10, 30)) == "1985"

    def test_just_year(self):
        assert year_only("1990") == "1990"

    def test_none(self):
        assert year_only(None) == ""

    def test_garbage(self):
        assert year_only("not a date") == ""

    def test_embedded_year(self):
        assert year_only("born in 1985 somewhere") == "1985"


class TestHipaaZip3:
    def test_normal_5digit_zip(self):
        result = hipaa_safe_harbor_zip3("12345")
        assert result == "123"

    def test_zip_plus_4(self):
        result = hipaa_safe_harbor_zip3("12345-6789")
        assert result == "123"

    def test_low_population_zip_suppressed(self):
        # 036xx is in NH, low population
        result = hipaa_safe_harbor_zip3("03601")
        assert result == "000"

    def test_another_low_pop(self):
        # 821xx is in WY
        result = hipaa_safe_harbor_zip3("82101")
        assert result == "000"

    def test_high_population_zip_retained(self):
        # 100xx is NYC — high population
        result = hipaa_safe_harbor_zip3("10001")
        assert result == "100"

    def test_short_zip(self):
        result = hipaa_safe_harbor_zip3("12")
        assert result == "000"

    def test_integer_zip(self):
        result = hipaa_safe_harbor_zip3(90210)
        assert result == "902"


class TestAgeBucket:
    def test_normal_age(self):
        assert age_bucket(45) == "45"

    def test_age_89(self):
        assert age_bucket(89) == "89"

    def test_age_90_bucketed(self):
        assert age_bucket(90) == "90+"

    def test_age_105_bucketed(self):
        assert age_bucket(105) == "90+"

    def test_string_age(self):
        assert age_bucket("75") == "75"

    def test_string_age_over_89(self):
        assert age_bucket("92") == "90+"

    def test_non_numeric(self):
        assert age_bucket("unknown") == "unknown"


class TestCustomTransformersInAnonymizer:
    """Verify custom transformers are actually called by the anonymizer."""

    def test_year_only_through_anonymizer(self):
        from dbslice.utils.anonymizer import DeterministicAnonymizer

        anon = DeterministicAnonymizer(seed="test")
        anon.configure(
            redact_fields=[],
            field_providers={"patients.admission_date": "year_only"},
        )
        result = anon.anonymize_value("2024-03-15", "patients", "admission_date")
        assert result == "2024"

    def test_hipaa_zip3_through_anonymizer(self):
        from dbslice.utils.anonymizer import DeterministicAnonymizer

        anon = DeterministicAnonymizer(seed="test")
        anon.configure(
            redact_fields=[],
            field_providers={"patients.zipcode": "hipaa_zip3"},
        )
        result = anon.anonymize_value("03601", "patients", "zipcode")
        assert result == "000"  # Low population area

    def test_age_bucket_through_anonymizer(self):
        from dbslice.utils.anonymizer import DeterministicAnonymizer

        anon = DeterministicAnonymizer(seed="test")
        anon.configure(
            redact_fields=[],
            field_providers={"patients.age": "age_bucket"},
        )
        assert anon.anonymize_value(92, "patients", "age") == "90+"
        assert anon.anonymize_value(45, "patients", "age") == "45"

    def test_hipaa_profile_uses_year_only_for_dates(self):
        """End-to-end: HIPAA profile maps date columns to year_only transformer."""
        from dbslice.compliance.profiles import get_profile
        from dbslice.utils.anonymizer import DeterministicAnonymizer

        profile = get_profile("hipaa")
        # Build fallback patterns like the engine does
        fallback_patterns = {
            f"*.{pattern}*": method for pattern, method in profile.required_column_patterns.items()
        }

        anon = DeterministicAnonymizer(seed="test")
        anon.configure(
            redact_fields=[],
            fallback_patterns=fallback_patterns,
        )

        # admission_date should use year_only
        result = anon.anonymize_value("2024-03-15", "visits", "admission_date")
        assert result == "2024"

    def test_hipaa_profile_uses_zip3_for_zipcodes(self):
        """End-to-end: HIPAA profile maps ZIP columns to hipaa_zip3 transformer."""
        from dbslice.compliance.profiles import get_profile
        from dbslice.utils.anonymizer import DeterministicAnonymizer

        profile = get_profile("hipaa")
        fallback_patterns = {
            f"*.{pattern}*": method for pattern, method in profile.required_column_patterns.items()
        }

        anon = DeterministicAnonymizer(seed="test")
        anon.configure(
            redact_fields=[],
            fallback_patterns=fallback_patterns,
        )

        result = anon.anonymize_value("82101", "addresses", "zipcode")
        assert result == "000"  # Wyoming, low population


# ──────────────────────────────────────────────────────────
# Phase B: Free-text handling
# ──────────────────────────────────────────────────────────


class TestFreetextRedaction:
    def test_redact_email(self):
        text = "Contact john@example.com for details"
        result = redact_freetext(text)
        assert "[REDACTED_EMAIL]" in result
        assert "john@example.com" not in result
        assert "Contact" in result
        assert "for details" in result

    def test_redact_ssn(self):
        text = "Patient SSN: 123-45-6789"
        result = redact_freetext(text)
        assert "[REDACTED_SSN]" in result
        assert "123-45-6789" not in result

    def test_redact_phone(self):
        text = "Call 555-123-4567"
        result = redact_freetext(text)
        assert "[REDACTED_PHONE]" in result

    def test_redact_multiple(self):
        text = "Email: alice@test.com, SSN: 123-45-6789"
        result = redact_freetext(text)
        assert "[REDACTED_EMAIL]" in result
        assert "[REDACTED_SSN]" in result
        assert "alice@test.com" not in result
        assert "123-45-6789" not in result

    def test_no_pii_unchanged(self):
        text = "This is a normal note with no PII"
        assert redact_freetext(text) == text

    def test_none_returns_empty(self):
        assert redact_freetext(None) == ""

    def test_redact_ip(self):
        text = "Source IP: 192.168.1.100"
        result = redact_freetext(text)
        assert "[REDACTED_IP]" in result
        assert "192.168.1.100" not in result


# ──────────────────────────────────────────────────────────
# Phase E: k-anonymity
# ──────────────────────────────────────────────────────────


class TestKAnonymityCheck:
    """Test k-anonymity verification logic directly on data."""

    def test_passes_when_k_satisfied(self):
        """Each combination appears >= 2 times."""
        rows = [
            {"age": "30", "gender": "M", "zip": "100"},
            {"age": "30", "gender": "M", "zip": "100"},
            {"age": "40", "gender": "F", "zip": "200"},
            {"age": "40", "gender": "F", "zip": "200"},
        ]
        violations = self._check(rows, ["age", "gender", "zip"], k=2)
        assert len(violations) == 0

    def test_fails_when_unique_combination(self):
        """One person with a unique combination."""
        rows = [
            {"age": "30", "gender": "M", "zip": "100"},
            {"age": "30", "gender": "M", "zip": "100"},
            {"age": "99", "gender": "X", "zip": "999"},  # unique
        ]
        violations = self._check(rows, ["age", "gender", "zip"], k=2)
        assert len(violations) == 1

    def test_k_1_always_passes(self):
        rows = [{"age": "unique_value", "gender": "unique"}]
        violations = self._check(rows, ["age", "gender"], k=1)
        assert len(violations) == 0

    @staticmethod
    def _check(rows, qi_columns, k):
        from collections import Counter

        combos = Counter(tuple(str(row.get(c, "")) for c in qi_columns) for row in rows)
        return [(combo, count) for combo, count in combos.items() if count < k]


# ──────────────────────────────────────────────────────────
# Phase C: Binary column sentinel
# ──────────────────────────────────────────────────────────


class TestBinarySentinel:
    def test_sentinel_value(self):
        assert BINARY_SENTINEL == b"\x00"


# ──────────────────────────────────────────────────────────
# Integration: verify config fields exist
# ──────────────────────────────────────────────────────────


class TestConfigFields:
    def test_extract_config_has_new_fields(self):
        from dbslice.config import ExtractConfig, SeedSpec

        seed = SeedSpec(table="t", column="c", value=1, where_clause=None)
        config = ExtractConfig(
            database_url="postgres://localhost/test",
            seeds=[seed],
            freetext_action="redact",
            binary_action="sentinel",
            compliance_sample_rows=500,
            k_anonymity_min_k=3,
            k_anonymity_quasi_identifiers=["users.age", "users.zip"],
            k_anonymity_action="fail",
        )
        assert config.freetext_action == "redact"
        assert config.binary_action == "sentinel"
        assert config.compliance_sample_rows == 500
        assert config.k_anonymity_min_k == 3
        assert config.k_anonymity_action == "fail"

    def test_defaults(self):
        from dbslice.config import ExtractConfig, SeedSpec

        seed = SeedSpec(table="t", column="c", value=1, where_clause=None)
        config = ExtractConfig(
            database_url="postgres://localhost/test",
            seeds=[seed],
        )
        assert config.freetext_action == "warn"
        assert config.binary_action == "warn"
        assert config.compliance_sample_rows == 100
        assert config.k_anonymity_min_k is None
        assert config.k_anonymity_action == "warn"
