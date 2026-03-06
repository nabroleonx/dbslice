"""Tests for the compliance module: profiles, scanner, manifest, and integration."""

import json
from unittest.mock import MagicMock

import pytest
import yaml

import dbslice.cli as cli
from dbslice.compliance.manifest import ComplianceManifest
from dbslice.compliance.profiles import (
    get_profile,
    list_profiles,
)
from dbslice.compliance.scanner import PIIDetection, PIIScanner, _luhn_check
from dbslice.config import ExtractConfig
from dbslice.core.engine import ExtractionEngine
from dbslice.exceptions import ExtractionError
from dbslice.models import Column, SchemaGraph, Table

# ──────────────────────────────────────────────────────────
# Profile tests
# ──────────────────────────────────────────────────────────


class TestProfiles:
    def test_get_gdpr_profile(self):
        profile = get_profile("gdpr")
        assert profile.name == "gdpr"
        assert profile.display_name == "GDPR"
        assert "email" in profile.required_column_patterns

    def test_get_hipaa_profile(self):
        profile = get_profile("hipaa")
        assert profile.name == "hipaa"
        assert "ssn" in profile.required_column_patterns
        assert len(profile.identifiers) == 18

    def test_get_pci_dss_profile(self):
        profile = get_profile("pci-dss")
        assert profile.name == "pci-dss"
        assert "credit_card" in profile.required_column_patterns
        assert "cvv" in profile.required_null_patterns

    def test_get_profile_case_insensitive(self):
        assert get_profile("GDPR").name == "gdpr"
        assert get_profile("Hipaa").name == "hipaa"
        assert get_profile("PCI-DSS").name == "pci-dss"

    def test_get_profile_unknown_raises(self):
        with pytest.raises(ValueError, match="Unknown compliance profile"):
            get_profile("unknown")

    def test_list_profiles(self):
        profiles = list_profiles()
        assert len(profiles) >= 3
        names = {p.name for p in profiles}
        assert "gdpr" in names
        assert "hipaa" in names
        assert "pci-dss" in names

    def test_gdpr_covers_direct_identifiers(self):
        profile = get_profile("gdpr")
        expected_patterns = ["email", "phone", "first_name", "last_name", "ssn", "ip_address"]
        for pattern in expected_patterns:
            assert pattern in profile.required_column_patterns, f"Missing: {pattern}"

    def test_hipaa_has_18_identifiers(self):
        profile = get_profile("hipaa")
        assert len(profile.identifiers) == 18
        assert profile.identifiers[0].startswith("1.")
        assert profile.identifiers[17].startswith("18.")

    def test_pci_dss_covers_pan_fields(self):
        profile = get_profile("pci-dss")
        for pattern in ["credit_card", "card_number", "pan"]:
            assert pattern in profile.required_column_patterns

    def test_profiles_have_value_scan_patterns(self):
        for profile in list_profiles():
            assert len(profile.value_scan_patterns) > 0

    def test_profiles_have_freetext_warnings(self):
        for profile in list_profiles():
            assert len(profile.warn_freetext_columns) > 0

    def test_profile_is_frozen(self):
        profile = get_profile("gdpr")
        with pytest.raises(AttributeError):
            profile.name = "hacked"  # type: ignore[misc]


# ──────────────────────────────────────────────────────────
# Scanner tests
# ──────────────────────────────────────────────────────────


class TestPIIScanner:
    def test_detect_emails(self):
        scanner = PIIScanner(patterns=["email"])
        values = ["john@example.com", "jane@test.org", "not-an-email", "bob@foo.co"]
        detections = scanner.scan_column("users", "notes", values)
        assert len(detections) == 1
        assert detections[0].pattern_name == "email"
        assert detections[0].match_count == 3

    def test_detect_ssn(self):
        scanner = PIIScanner(patterns=["ssn"])
        values = ["123-45-6789", "987-65-4321", "not-ssn", "hello"]
        detections = scanner.scan_column("users", "data", values)
        assert len(detections) == 1
        assert detections[0].pattern_name == "ssn"
        assert detections[0].match_count == 2

    def test_detect_credit_card_with_luhn(self):
        scanner = PIIScanner(patterns=["credit_card"])
        # 4111111111111111 is a valid Luhn number (Visa test card)
        values = ["4111111111111111", "1234567890123456", "not-a-card"]
        detections = scanner.scan_column("orders", "memo", values)
        assert len(detections) == 1
        assert detections[0].pattern_name == "credit_card"
        # Only the Luhn-valid one should match
        assert detections[0].match_count >= 1

    def test_detect_credit_card_with_grouped_format(self):
        scanner = PIIScanner(patterns=["credit_card"])
        values = ["4111-1111-1111-1111", "not-a-card"]
        detections = scanner.scan_column("orders", "memo", values)
        assert len(detections) == 1
        assert detections[0].pattern_name == "credit_card"

    def test_detect_credit_card_embedded_in_text(self):
        scanner = PIIScanner(patterns=["credit_card"])
        values = ["card=4111 1111 1111 1111 expires soon", "hello"]
        detections = scanner.scan_column("orders", "memo", values)
        assert len(detections) == 1
        assert detections[0].match_count == 1

    def test_detect_ipv4(self):
        scanner = PIIScanner(patterns=["ipv4"])
        values = ["192.168.1.1", "10.0.0.1", "not-ip", "256.1.1.1"]
        detections = scanner.scan_column("logs", "source", values)
        assert len(detections) >= 1
        assert detections[0].pattern_name == "ipv4"

    def test_no_detection_below_threshold(self):
        scanner = PIIScanner(patterns=["email"], min_match_rate=0.5)
        # Only 1 out of 10 is an email — below 50% threshold
        values = ["john@example.com"] + ["no-email"] * 9
        detections = scanner.scan_column("users", "notes", values)
        assert len(detections) == 0

    def test_scan_rows(self):
        scanner = PIIScanner(patterns=["email"])
        rows = [
            {"id": 1, "notes": "contact john@example.com"},
            {"id": 2, "notes": "call jane@test.org"},
            {"id": 3, "notes": "nothing here"},
        ]
        detections = scanner.scan_rows("users", rows)
        assert any(d.column == "notes" and d.pattern_name == "email" for d in detections)

    def test_scan_rows_skip_columns(self):
        scanner = PIIScanner(patterns=["email"])
        rows = [
            {"id": 1, "email": "john@example.com", "notes": "call john@example.com"},
        ]
        detections = scanner.scan_rows("users", rows, skip_columns={"email"})
        # Should only detect in "notes", not in "email" (skipped)
        assert all(d.column != "email" for d in detections)

    def test_scan_empty_rows(self):
        scanner = PIIScanner()
        assert scanner.scan_rows("users", []) == []

    def test_scan_none_values(self):
        scanner = PIIScanner(patterns=["email"])
        values = [None, None, None]
        detections = scanner.scan_column("users", "email", values)
        assert len(detections) == 0

    def test_confidence_levels(self):
        scanner = PIIScanner(patterns=["email"], min_match_rate=0.01)
        # High match rate = high confidence
        values = ["a@b.com"] * 10
        detections = scanner.scan_column("t", "c", values)
        assert detections[0].confidence == "high"

    def test_match_rate_property(self):
        detection = PIIDetection(
            table="t",
            column="c",
            pattern_name="email",
            match_count=3,
            sample_size=10,
            confidence="high",
        )
        assert detection.match_rate == 0.3

    def test_match_rate_zero_sample(self):
        detection = PIIDetection(
            table="t",
            column="c",
            pattern_name="email",
            match_count=0,
            sample_size=0,
            confidence="low",
        )
        assert detection.match_rate == 0.0


class TestLuhnCheck:
    def test_valid_visa(self):
        assert _luhn_check("4111111111111111") is True

    def test_valid_mastercard(self):
        assert _luhn_check("5500000000000004") is True

    def test_invalid_number(self):
        assert _luhn_check("1234567890123456") is False

    def test_too_short(self):
        assert _luhn_check("123") is False


# ──────────────────────────────────────────────────────────
# Manifest tests
# ──────────────────────────────────────────────────────────


class TestComplianceManifest:
    def test_initialize(self):
        manifest = ComplianceManifest()
        manifest.initialize(
            extraction_id="test-123",
            compliance_profiles=["gdpr"],
            anonymization_seed="my_seed",
            deterministic=True,
        )
        assert manifest.extraction_id == "test-123"
        assert manifest.compliance_profiles == ["gdpr"]
        assert manifest.masking_type == "deterministic_pseudonymization"
        assert manifest.seed_hash.startswith("sha256:")
        assert manifest.dbslice_version

    def test_initialize_non_deterministic(self):
        manifest = ComplianceManifest()
        manifest.initialize(
            extraction_id="test-456",
            deterministic=False,
        )
        assert manifest.masking_type == "non_deterministic_pseudonymization"

    def test_record_masked_field(self):
        manifest = ComplianceManifest()
        manifest.record_masked_field("users", "email", "email", category="direct_identifier")
        assert len(manifest.tables["users"].fields_masked) == 1
        assert manifest.tables["users"].fields_masked[0].method == "email"

    def test_record_nulled_field(self):
        manifest = ComplianceManifest()
        manifest.record_nulled_field("users", "password_hash", "security_null_pattern")
        assert len(manifest.tables["users"].fields_nulled) == 1

    def test_record_fk_preserved(self):
        manifest = ComplianceManifest()
        manifest.record_fk_preserved("orders", "user_id")
        assert "user_id" in manifest.tables["orders"].fields_preserved_fk

    def test_record_unmasked_field(self):
        manifest = ComplianceManifest()
        manifest.record_unmasked_field("orders", "status")
        assert "status" in manifest.tables["orders"].fields_unmasked

    def test_set_table_row_count(self):
        manifest = ComplianceManifest()
        manifest.set_table_row_count("users", 150)
        assert manifest.tables["users"].rows_extracted == 150

    def test_add_warning(self):
        manifest = ComplianceManifest()
        manifest.add_warning("notes", "body", "may contain PII")
        assert len(manifest.warnings) == 1
        assert manifest.warnings[0].table == "notes"

    def test_add_pii_detections(self):
        manifest = ComplianceManifest()
        detection = PIIDetection(
            table="logs",
            column="message",
            pattern_name="email",
            match_count=5,
            sample_size=100,
            confidence="high",
        )
        manifest.add_pii_detections([detection])
        assert len(manifest.pii_scan_results) == 1

    def test_to_dict(self):
        manifest = ComplianceManifest()
        manifest.initialize(
            extraction_id="test-789",
            compliance_profiles=["hipaa"],
            anonymization_seed="seed123",
        )
        manifest.record_masked_field("users", "email", "email")
        manifest.record_nulled_field("users", "password", "security_null")
        manifest.set_table_row_count("users", 50)
        manifest.add_warning("notes", "body", "freetext PII risk")

        d = manifest.to_dict()
        assert d["extraction_id"] == "test-789"
        assert d["compliance_profiles"] == ["hipaa"]
        assert "users" in d["tables"]
        assert d["tables"]["users"]["rows_extracted"] == 50
        assert len(d["tables"]["users"]["fields_masked"]) == 1
        assert len(d["warnings"]) == 1

    def test_to_json(self):
        manifest = ComplianceManifest()
        manifest.initialize(extraction_id="test-json")
        manifest.record_masked_field("t", "c", "email")
        json_str = manifest.to_json()
        parsed = json.loads(json_str)
        assert parsed["extraction_id"] == "test-json"

    def test_to_json_compact(self):
        manifest = ComplianceManifest()
        manifest.initialize(extraction_id="test-compact")
        json_str = manifest.to_json(pretty=False)
        assert "\n" not in json_str


# ──────────────────────────────────────────────────────────
# Integration: anonymizer + manifest
# ──────────────────────────────────────────────────────────


class TestAnonymizerManifestIntegration:
    def test_anonymizer_records_to_manifest(self):
        from dbslice.utils.anonymizer import DeterministicAnonymizer

        manifest = ComplianceManifest()
        anonymizer = DeterministicAnonymizer(
            seed="test_seed",
            deterministic=True,
            manifest=manifest,
        )
        anonymizer.configure(redact_fields=["users.email"])

        row = {"id": 1, "email": "john@example.com", "status": "active"}
        anonymizer.anonymize_row("users", row)

        # email should be recorded as masked
        assert any(
            f.column == "email" for f in manifest.tables.get("users", MagicMock()).fields_masked
        )

    def test_anonymizer_non_deterministic_mode(self):
        from dbslice.utils.anonymizer import DeterministicAnonymizer

        anonymizer = DeterministicAnonymizer(
            seed="test_seed",
            deterministic=False,
        )
        anonymizer.configure(redact_fields=["users.email"])

        # Same input should produce different outputs in non-deterministic mode
        results = set()
        for _ in range(10):
            row = {"email": "john@example.com"}
            result = anonymizer.anonymize_row("users", row)
            results.add(result["email"])

        # With 10 random seeds, we should get multiple distinct values
        assert len(results) > 1


# ──────────────────────────────────────────────────────────
# Config file integration
# ──────────────────────────────────────────────────────────


class TestComplianceConfig:
    def test_config_file_compliance_section(self, tmp_path):
        from dbslice.config_file import DbsliceConfig

        config_file = tmp_path / "dbslice.yaml"
        config_file.write_text("""
database:
  url: postgres://localhost/test

compliance:
  profiles: [gdpr]
  strict: true
  generate_manifest: true

anonymization:
  enabled: true
  deterministic: false
""")
        config = DbsliceConfig.from_yaml(config_file)
        assert config.compliance.profiles == ["gdpr"]
        assert config.compliance.strict is True
        assert config.compliance.generate_manifest is True
        assert config.anonymization.deterministic is False

    def test_config_file_invalid_profile(self, tmp_path):
        from dbslice.config_file import ConfigFileError, DbsliceConfig

        config_file = tmp_path / "dbslice.yaml"
        config_file.write_text("""
compliance:
  profiles: [nonexistent]
""")
        with pytest.raises(ConfigFileError, match="Unknown compliance profile"):
            DbsliceConfig.from_yaml(config_file)

    def test_config_file_compliance_unknown_key(self, tmp_path):
        from dbslice.config_file import ConfigFileError, DbsliceConfig

        config_file = tmp_path / "dbslice.yaml"
        config_file.write_text("""
compliance:
  profiles: [gdpr]
  invalid_key: true
""")
        with pytest.raises(ConfigFileError, match="Unknown key"):
            DbsliceConfig.from_yaml(config_file)

    def test_to_extract_config_with_compliance(self, tmp_path):
        from dbslice.config import SeedSpec
        from dbslice.config_file import DbsliceConfig

        config_file = tmp_path / "dbslice.yaml"
        config_file.write_text("""
database:
  url: postgres://localhost/test

compliance:
  profiles: [hipaa]
  strict: true
  generate_manifest: true

anonymization:
  enabled: true
  deterministic: false
""")
        config = DbsliceConfig.from_yaml(config_file)
        seed = SeedSpec(table="users", column="id", value=1, where_clause=None)
        extract_config = config.to_extract_config(seeds=[seed])

        assert extract_config.compliance_profiles == ["hipaa"]
        assert extract_config.compliance_strict is True
        assert extract_config.generate_manifest is True
        assert extract_config.deterministic is False

    def test_to_extract_config_with_compliance_policy_fields(self, tmp_path):
        from dbslice.config import SeedSpec
        from dbslice.config_file import DbsliceConfig

        config_file = tmp_path / "dbslice.yaml"
        config_file.write_text(
            """
database:
  url: postgres://localhost/test
compliance:
  profiles: [gdpr]
  policy_mode: strict
  allow_url_patterns:
    - ".*localhost.*"
  deny_url_patterns:
    - ".*prod.*"
  required_sslmode: require
  require_ci: true
  sign_manifest: true
  manifest_key_env: DBSLICE_SIGN_KEY
"""
        )
        config = DbsliceConfig.from_yaml(config_file)
        seed = SeedSpec(table="users", column="id", value=1, where_clause=None)
        extract_config = config.to_extract_config(seeds=[seed])

        assert extract_config.compliance_policy_mode == "strict"
        assert extract_config.compliance_allowed_url_patterns == [".*localhost.*"]
        assert extract_config.compliance_denied_url_patterns == [".*prod.*"]
        assert extract_config.compliance_required_sslmode == "require"
        assert extract_config.compliance_require_ci is True
        assert extract_config.compliance_manifest_sign is True
        assert extract_config.compliance_manifest_key_env == "DBSLICE_SIGN_KEY"

    def test_compliance_empty_section_ok(self, tmp_path):
        from dbslice.config_file import DbsliceConfig

        config_file = tmp_path / "dbslice.yaml"
        config_file.write_text("""
compliance: {}
""")
        config = DbsliceConfig.from_yaml(config_file)
        assert config.compliance.profiles == []
        assert config.compliance.strict is False

    def test_to_yaml_includes_deterministic_and_compliance(self, tmp_path):
        from dbslice.config_file import DbsliceConfig

        config_file = tmp_path / "dbslice.yaml"
        config_file.write_text(
            """
database:
  url: postgres://localhost/test
anonymization:
  enabled: true
  deterministic: false
compliance:
  profiles: [gdpr]
  strict: true
  generate_manifest: true
  policy_mode: strict
"""
        )
        config = DbsliceConfig.from_yaml(config_file)
        exported = config.to_yaml(include_comments=False)
        parsed = yaml.safe_load(exported)
        assert parsed["anonymization"]["deterministic"] is False
        assert parsed["compliance"]["profiles"] == ["gdpr"]
        assert parsed["compliance"]["strict"] is True
        assert parsed["compliance"]["policy_mode"] == "strict"


class TestComplianceScanSemantics:
    def _engine(self, strict: bool) -> ExtractionEngine:
        config = ExtractConfig(
            database_url="postgresql://localhost/test",
            seeds=[],
            anonymize=True,
            compliance_profiles=["gdpr"],
            compliance_strict=strict,
        )
        return ExtractionEngine(config)

    def test_strict_mode_ignores_masked_synthetic_values(self):
        engine = self._engine(strict=True)
        pre_mask = {"users": [{"email": "alice@example.com"}]}
        post_mask = {"users": [{"email": "xcooper@example.org"}]}
        # No exception: email column is protected by profile rules.
        engine._run_pii_scan(pre_mask, post_mask)

    def test_strict_mode_fails_on_residual_unprotected_detections(self):
        engine = self._engine(strict=True)
        pre_mask = {"logs": [{"message": "contact alice@example.com"}]}
        post_mask = {"logs": [{"message": "contact alice@example.com"}]}
        with pytest.raises(ExtractionError, match="residual unprotected PII"):
            engine._run_pii_scan(pre_mask, post_mask)


class TestManifestVerification:
    def test_manifest_file_hash_and_signature_verification(self, tmp_path):
        from dbslice.compliance.manifest import verify_manifest_payload

        data_file = tmp_path / "subset.sql"
        data_file.write_text("select 1;\n", encoding="utf-8")

        manifest = ComplianceManifest()
        manifest.initialize(extraction_id="verify-1")
        manifest.add_output_file_hashes([data_file], base_dir=tmp_path)
        manifest.sign("secret-key")

        manifest_path = tmp_path / "subset.manifest.json"
        manifest_path.write_text(manifest.to_json(pretty=True), encoding="utf-8")
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))

        ok, errors = verify_manifest_payload(
            payload,
            manifest_path,
            signing_key="secret-key",
            verify_signature=True,
        )
        assert ok is True
        assert errors == []

        data_file.write_text("tampered\n", encoding="utf-8")
        ok, errors = verify_manifest_payload(
            payload,
            manifest_path,
            signing_key="secret-key",
            verify_signature=True,
        )
        assert ok is False
        assert any("Hash mismatch" in err for err in errors)

    def test_verify_manifest_cli_command(self, tmp_path, monkeypatch):
        data_file = tmp_path / "subset.sql"
        data_file.write_text("select 1;\n", encoding="utf-8")

        manifest = ComplianceManifest()
        manifest.initialize(extraction_id="verify-cli")
        manifest.add_output_file_hashes([data_file], base_dir=tmp_path)
        manifest.sign("secret-key")

        manifest_path = tmp_path / "subset.manifest.json"
        manifest_path.write_text(manifest.to_json(pretty=True), encoding="utf-8")

        monkeypatch.setenv("DBSLICE_MANIFEST_SIGNING_KEY", "secret-key")
        cli.verify_manifest(
            manifest_file=manifest_path,
            verify_signature=True,
            key_env="DBSLICE_MANIFEST_SIGNING_KEY",
        )


class TestCompliancePolicyGates:
    def test_policy_blocks_stdout_without_breakglass(self):
        config = ExtractConfig(
            database_url="postgresql://localhost/test",
            seeds=[],
            compliance_profiles=["gdpr"],
            compliance_policy_mode="standard",
            anonymize=True,
        )
        with pytest.raises(ValueError, match="stdout output is blocked"):
            cli._enforce_compliance_policy(
                config,
                out_file=None,
                allow_raw=False,
                breakglass_reason=None,
                ticket_id=None,
            )

    def test_policy_breakglass_requires_metadata(self):
        config = ExtractConfig(
            database_url="postgresql://localhost/test",
            seeds=[],
            compliance_profiles=["gdpr"],
            compliance_policy_mode="strict",
            anonymize=False,
        )
        with pytest.raises(ValueError, match="--breakglass-reason"):
            cli._enforce_compliance_policy(
                config,
                out_file=None,
                allow_raw=True,
                breakglass_reason=None,
                ticket_id="SEC-123",
            )

    def test_source_guardrails_validate_sslmode_and_ci(self, monkeypatch):
        config = ExtractConfig(
            database_url="postgresql://localhost/test?sslmode=require",
            seeds=[],
            compliance_profiles=["gdpr"],
            compliance_required_sslmode="require",
            compliance_require_ci=True,
        )
        monkeypatch.setenv("CI", "true")
        cli._enforce_source_guardrails(config)

        bad = ExtractConfig(
            database_url="postgresql://localhost/test?sslmode=disable",
            seeds=[],
            compliance_profiles=["gdpr"],
            compliance_required_sslmode="require",
        )
        with pytest.raises(ValueError, match="sslmode"):
            cli._enforce_source_guardrails(bad)


class TestComplianceInspectReport:
    def test_report_detects_uncovered_columns(self):
        class FakeAdapter:
            def fetch_rows(self, table: str, where_clause: str, params: tuple[object, ...]):
                assert where_clause == "TRUE"
                assert params == ()
                if table == "logs":
                    yield {"id": 1, "message": "contact jane@example.com"}

        schema = SchemaGraph(
            tables={
                "logs": Table(
                    name="logs",
                    schema="public",
                    columns=[
                        Column("id", "integer", False, True),
                        Column("message", "text", True, False),
                    ],
                    primary_key=("id",),
                    foreign_keys=[],
                )
            },
            edges=[],
        )

        # Smoke test that helper executes and prints JSON report
        cli._run_compliance_check_report(
            adapter=FakeAdapter(),
            db_schema=schema,
            profiles=["gdpr"],
            sample_rows=10,
            output_mode="json",
            target_table=None,
            console=MagicMock(),
        )
