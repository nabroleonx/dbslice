import hashlib
import hmac
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dbslice import __version__
from dbslice.compliance.scanner import PIIDetection


@dataclass
class ManifestFieldEntry:
    """Record of anonymization applied to a single field."""

    table: str
    column: str
    method: str
    category: str = ""  # e.g., "direct_identifier", "hipaa_identifier_7"


@dataclass
class ManifestNullEntry:
    """Record of a field forced to NULL."""

    table: str
    column: str
    reason: str  # e.g., "security_null_pattern"


@dataclass
class ManifestWarning:
    """A compliance warning."""

    table: str
    column: str
    reason: str
    severity: str = "warning"  # "warning" or "error"


@dataclass
class ManifestTableEntry:
    """Per-table manifest data."""

    rows_extracted: int = 0
    fields_masked: list[ManifestFieldEntry] = field(default_factory=list)
    fields_nulled: list[ManifestNullEntry] = field(default_factory=list)
    fields_preserved_fk: list[str] = field(default_factory=list)
    fields_unmasked: list[str] = field(default_factory=list)


@dataclass
class ManifestEntry:
    """A single entry in the compliance manifest (for external use)."""

    table: str
    column: str
    action: str  # "masked", "nulled", "preserved_fk", "unmasked"
    method: str = ""
    reason: str = ""


@dataclass
class ComplianceManifest:
    """
    Full compliance audit manifest.

    Generated alongside extraction output to document what anonymization
    was applied and provide evidence for compliance audits.
    """

    extraction_id: str = ""
    timestamp: str = ""
    dbslice_version: str = ""
    masking_type: str = "deterministic_pseudonymization"
    compliance_profiles: list[str] = field(default_factory=list)
    tables: dict[str, ManifestTableEntry] = field(default_factory=dict)
    pii_scan_results: list[PIIDetection] = field(default_factory=list)
    warnings: list[ManifestWarning] = field(default_factory=list)
    seed_hash: str = ""
    output_file_hashes: dict[str, str] = field(default_factory=dict)
    breakglass: dict[str, str] = field(default_factory=dict)
    signature_algorithm: str = ""
    signature: str = ""

    def initialize(
        self,
        extraction_id: str,
        compliance_profiles: list[str] | None = None,
        anonymization_seed: str | None = None,
        deterministic: bool = True,
    ) -> None:
        """
        Initialize manifest metadata.

        Args:
            extraction_id: Unique ID for this extraction
            compliance_profiles: Names of active compliance profiles
            anonymization_seed: The anonymization seed (hashed, not stored raw)
            deterministic: Whether deterministic mode is used
        """
        self.extraction_id = extraction_id
        self.timestamp = datetime.now(timezone.utc).isoformat()
        self.dbslice_version = __version__
        self.compliance_profiles = compliance_profiles or []
        self.masking_type = (
            "deterministic_pseudonymization"
            if deterministic
            else "non_deterministic_pseudonymization"
        )
        if anonymization_seed:
            self.seed_hash = (
                f"sha256:{hashlib.sha256(anonymization_seed.encode()).hexdigest()[:16]}"
            )

    def record_masked_field(
        self,
        table: str,
        column: str,
        method: str,
        category: str = "",
    ) -> None:
        """Record that a field was masked/anonymized."""
        entry = self.tables.setdefault(table, ManifestTableEntry())
        entry.fields_masked.append(
            ManifestFieldEntry(table=table, column=column, method=method, category=category)
        )

    def record_nulled_field(self, table: str, column: str, reason: str) -> None:
        """Record that a field was set to NULL."""
        entry = self.tables.setdefault(table, ManifestTableEntry())
        entry.fields_nulled.append(ManifestNullEntry(table=table, column=column, reason=reason))

    def record_fk_preserved(self, table: str, column: str) -> None:
        """Record that a FK column was preserved (not anonymized)."""
        entry = self.tables.setdefault(table, ManifestTableEntry())
        if column not in entry.fields_preserved_fk:
            entry.fields_preserved_fk.append(column)

    def record_unmasked_field(self, table: str, column: str) -> None:
        """Record that a field was not masked."""
        entry = self.tables.setdefault(table, ManifestTableEntry())
        if column not in entry.fields_unmasked:
            entry.fields_unmasked.append(column)

    def set_table_row_count(self, table: str, count: int) -> None:
        """Set the extracted row count for a table."""
        entry = self.tables.setdefault(table, ManifestTableEntry())
        entry.rows_extracted = count

    def add_warning(
        self,
        table: str,
        column: str,
        reason: str,
        severity: str = "warning",
    ) -> None:
        """Add a compliance warning."""
        self.warnings.append(
            ManifestWarning(table=table, column=column, reason=reason, severity=severity)
        )

    def add_pii_detections(self, detections: list[PIIDetection]) -> None:
        """Add PII scan results."""
        self.pii_scan_results.extend(detections)

    def set_breakglass(self, reason: str, ticket_id: str) -> None:
        """Record breakglass metadata for raw/unsafe extraction exceptions."""
        self.breakglass = {
            "reason": reason,
            "ticket_id": ticket_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def add_output_file_hashes(
        self, output_files: list[Path], base_dir: Path | None = None
    ) -> None:
        """Record deterministic SHA256 hashes for generated output files."""
        root = (base_dir or Path.cwd()).resolve()
        hashes: dict[str, str] = {}

        for file_path in sorted((Path(p).resolve() for p in output_files), key=lambda p: str(p)):
            if not file_path.exists() or not file_path.is_file():
                continue
            digest = _sha256_file(file_path)
            key: str
            try:
                key = str(file_path.relative_to(root))
            except ValueError:
                key = str(file_path)
            hashes[key] = f"sha256:{digest}"

        self.output_file_hashes = hashes

    def sign(self, signing_key: str) -> None:
        """Sign manifest payload using HMAC-SHA256."""
        payload = self._signable_dict()
        digest = _manifest_hmac(payload, signing_key)
        self.signature_algorithm = "hmac-sha256"
        self.signature = f"hmac-sha256:{digest}"

    def to_dict(self) -> dict[str, Any]:
        """Convert to a JSON-serializable dictionary."""
        tables_dict: dict[str, Any] = {}
        for table_name, table_entry in self.tables.items():
            tables_dict[table_name] = {
                "rows_extracted": table_entry.rows_extracted,
                "fields_masked": [
                    {"column": f.column, "method": f.method, "category": f.category}
                    for f in table_entry.fields_masked
                ],
                "fields_nulled": [
                    {"column": f.column, "reason": f.reason} for f in table_entry.fields_nulled
                ],
                "fields_preserved_fk": table_entry.fields_preserved_fk,
                "fields_unmasked": table_entry.fields_unmasked,
            }

        pii_results = [
            {
                "table": d.table,
                "column": d.column,
                "pattern": d.pattern_name,
                "match_count": d.match_count,
                "sample_size": d.sample_size,
                "confidence": d.confidence,
            }
            for d in self.pii_scan_results
        ]

        warnings = [asdict(w) for w in self.warnings]

        return {
            "extraction_id": self.extraction_id,
            "timestamp": self.timestamp,
            "dbslice_version": self.dbslice_version,
            "masking_type": self.masking_type,
            "compliance_profiles": self.compliance_profiles,
            "seed_hash": self.seed_hash,
            "tables": tables_dict,
            "pii_scan_results": pii_results,
            "warnings": warnings,
            "output_file_hashes": self.output_file_hashes,
            "breakglass": self.breakglass,
            "signature_algorithm": self.signature_algorithm,
            "signature": self.signature,
        }

    def to_json(self, pretty: bool = True) -> str:
        """Serialize manifest to JSON string."""
        return json.dumps(self.to_dict(), indent=2 if pretty else None, default=str)

    def _signable_dict(self) -> dict[str, Any]:
        payload = self.to_dict()
        payload.pop("signature_algorithm", None)
        payload.pop("signature", None)
        return payload


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _manifest_hmac(payload: dict[str, Any], signing_key: str) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hmac.new(signing_key.encode("utf-8"), canonical.encode("utf-8"), hashlib.sha256).hexdigest()


def verify_manifest_payload(
    payload: dict[str, Any],
    manifest_path: Path,
    signing_key: str | None = None,
    verify_signature: bool = True,
) -> tuple[bool, list[str]]:
    """Verify output file hashes and optional HMAC signature for a manifest payload."""
    errors: list[str] = []
    manifest_dir = manifest_path.parent.resolve()

    file_hashes = payload.get("output_file_hashes", {})
    if not isinstance(file_hashes, dict):
        errors.append("'output_file_hashes' must be an object")
        return False, errors

    for rel_path, expected_hash in file_hashes.items():
        if not isinstance(rel_path, str) or not isinstance(expected_hash, str):
            errors.append("Invalid output_file_hashes entry")
            continue
        target = (manifest_dir / rel_path).resolve()
        if not target.exists():
            errors.append(f"Missing output file: {rel_path}")
            continue
        actual = f"sha256:{_sha256_file(target)}"
        if actual != expected_hash:
            errors.append(
                f"Hash mismatch for {rel_path}: expected {expected_hash}, got {actual}"
            )

    if verify_signature:
        signature = payload.get("signature")
        signature_algorithm = payload.get("signature_algorithm")
        if signature:
            if signature_algorithm != "hmac-sha256":
                errors.append("Unsupported signature_algorithm (expected hmac-sha256)")
            elif signing_key is None:
                errors.append("Manifest is signed but no signing key was provided")
            else:
                signable = dict(payload)
                signable.pop("signature", None)
                signable.pop("signature_algorithm", None)
                expected = f"hmac-sha256:{_manifest_hmac(signable, signing_key)}"
                if signature != expected:
                    errors.append("Manifest signature verification failed")

    return len(errors) == 0, errors
