from __future__ import annotations

import inspect
import json
import secrets
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from dbslice.logging import get_logger
from dbslice.models import SchemaGraph

logger = get_logger(__name__)


class MappingServer:
    """Local mapping UI HTTP server."""

    def __init__(
        self,
        port: int = 9473,
        database_url: str | None = None,
        schema: str | None = None,
    ):
        self.port = port
        self.database_url = database_url
        self.schema_name = schema
        self.token = secrets.token_urlsafe(32)
        self._server: HTTPServer | None = None
        self._cached_schema: SchemaGraph | None = None
        self._cached_adapter: Any = None

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}?token={self.token}"

    def start(self, open_browser: bool = True) -> None:
        """Start the server and optionally open a browser."""
        handler = _make_handler(self)
        self._server = HTTPServer(("127.0.0.1", self.port), handler)

        if open_browser:
            import webbrowser

            threading.Timer(0.5, webbrowser.open, args=[self.url]).start()

        logger.info("Mapping UI server starting", url=self.url)
        try:
            self._server.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            self._server.server_close()
            if self._cached_adapter:
                try:
                    self._cached_adapter.close()
                except Exception:
                    pass

    def _introspect(self, database_url: str, schema: str | None, detect_sensitive: bool) -> dict:
        """Connect to database and introspect schema."""
        from dbslice.adapters.postgresql import PostgreSQLAdapter
        from dbslice.compliance.profiles import list_profiles
        from dbslice.input_validators import validate_database_url
        from dbslice.utils.anonymizer import (
            _SECURITY_NULL_PATTERNS,
        )
        from dbslice.utils.connection import parse_database_url

        validate_database_url(database_url)
        db_config = parse_database_url(database_url)

        if self._cached_adapter:
            try:
                self._cached_adapter.close()
            except Exception:
                pass

        adapter = PostgreSQLAdapter(schema=schema)
        adapter.connect(database_url)
        self._cached_adapter = adapter

        db_schema = adapter.get_schema()
        self._cached_schema = db_schema

        tables = []
        sensitive_suggestions: dict[str, str] = {}

        if detect_sensitive:
            sensitive_patterns = {
                "email": "email",
                "e_mail": "email",
                "email_address": "email",
                "phone": "phone_number",
                "telephone": "phone_number",
                "mobile": "phone_number",
                "cell": "phone_number",
                "first_name": "first_name",
                "firstname": "first_name",
                "last_name": "last_name",
                "lastname": "last_name",
                "full_name": "name",
                "fullname": "name",
                "address": "address",
                "street": "street_address",
                "city": "city",
                "postal_code": "postcode",
                "zipcode": "postcode",
                "ssn": "ssn",
                "social_security": "ssn",
                "passport": "passport_number",
                "driver_license": "license_plate",
                "credit_card": "credit_card_number",
                "card_number": "credit_card_number",
                "ip_address": "ipv4",
                "ip": "ipv4",
                "ipv4": "ipv4",
                "ipv6": "ipv6",
                "dob": "date_of_birth",
                "date_of_birth": "date_of_birth",
                "username": "user_name",
            }
            for table_name, table in db_schema.tables.items():
                for column in table.columns:
                    col_lower = column.name.lower()
                    if col_lower in sensitive_patterns:
                        sensitive_suggestions[f"{table_name}.{column.name}"] = sensitive_patterns[
                            col_lower
                        ]
                    else:
                        for pattern, provider in sensitive_patterns.items():
                            if pattern in col_lower:
                                sensitive_suggestions[f"{table_name}.{column.name}"] = provider
                                break

        fk_columns: set[tuple[str, str]] = set()
        for fk in db_schema.edges:
            for col in fk.source_columns:
                fk_columns.add((fk.source_table, col))

        null_columns: set[str] = set()
        for tbl_name, tbl in db_schema.tables.items():
            for col_obj in tbl.columns:
                col_lower = col_obj.name.lower()
                for pat in _SECURITY_NULL_PATTERNS:
                    if pat in col_lower:
                        null_columns.add(f"{tbl_name}.{col_obj.name}")
                        break

        from dbslice.models import Column as ColumnModel

        for table_name in sorted(db_schema.tables.keys()):
            table_info = db_schema.tables[table_name]
            columns: list[dict[str, Any]] = []
            col_obj2: ColumnModel
            for col_obj2 in table_info.columns:
                full_name = f"{table_name}.{col_obj2.name}"
                is_fk = (table_name, col_obj2.name) in fk_columns
                suggested = sensitive_suggestions.get(full_name)
                is_null_target = full_name in null_columns

                action = "keep"
                provider = ""
                if is_fk:
                    action = "locked_fk"
                elif col_obj2.is_primary_key:
                    action = "locked_pk"
                elif is_null_target:
                    action = "null"
                elif suggested:
                    action = "anonymize"
                    provider = suggested

                columns.append(
                    {
                        "name": col_obj2.name,
                        "data_type": col_obj2.data_type,
                        "nullable": col_obj2.nullable,
                        "is_pk": col_obj2.is_primary_key,
                        "is_fk": is_fk,
                        "suggested_action": action,
                        "suggested_provider": provider,
                    }
                )

            tables.append(
                {
                    "name": table_name,
                    "primary_key": list(table_info.primary_key),
                    "columns": columns,
                }
            )

        profiles = [
            {"name": p.name, "display_name": p.display_name, "description": p.description}
            for p in list_profiles()
        ]

        common_providers = [
            "email",
            "phone_number",
            "first_name",
            "last_name",
            "name",
            "address",
            "street_address",
            "city",
            "zipcode",
            "ssn",
            "credit_card_number",
            "ipv4",
            "ipv6",
            "company",
            "url",
            "date_of_birth",
            "user_name",
            "passport_number",
            "iban",
            "pystr",
            "random_int",
            "year_only",
            "hipaa_zip3",
            "age_bucket",
            "redact_freetext",
        ]

        return {
            "database": db_config.database,
            "table_count": len(tables),
            "tables": tables,
            "sensitive_suggestions": sensitive_suggestions,
            "compliance_profiles": profiles,
            "common_providers": common_providers,
        }

    def _apply_profile(self, profile_name: str, current_mappings: dict) -> dict:
        """Apply a compliance profile's patterns to the current schema."""
        from dbslice.compliance.profiles import get_profile

        profile = get_profile(profile_name)
        if not self._cached_schema:
            return {"error": "No schema loaded. Run introspection first."}

        additions: dict[str, str] = {}
        null_additions: list[str] = []

        for table_name, table in self._cached_schema.tables.items():
            for column in table.columns:
                full_name = f"{table_name}.{column.name}"
                if full_name in current_mappings:
                    continue

                col_lower = column.name.lower()

                for pat in profile.required_null_patterns:
                    if pat in col_lower:
                        null_additions.append(full_name)
                        break
                else:
                    for pat, method in profile.required_column_patterns.items():
                        if pat in col_lower:
                            additions[full_name] = method
                            break

        return {
            "profile": profile_name,
            "display_name": profile.display_name,
            "field_additions": additions,
            "null_additions": null_additions,
            "identifiers_covered": profile.identifiers,
        }

    @staticmethod
    def _generate_config(mappings: dict) -> dict:
        """Generate YAML config from column mappings."""
        fields: dict[str, str] = {}
        null_fields: list[str] = []

        for full_name, action_data in mappings.items():
            action = action_data.get("action", "keep")
            if action == "anonymize":
                provider = action_data.get("provider", "pystr")
                fields[full_name] = provider
            elif action == "null":
                null_fields.append(full_name)

        lines = [
            "# Generated by dbslice map",
            "",
            "database:",
            "  url: ${DATABASE_URL}",
            "",
            "anonymization:",
            "  enabled: true",
        ]

        if fields:
            lines.append("  fields:")
            for field_name, provider in sorted(fields.items()):
                lines.append(f"    {field_name}: {provider}")

        if null_fields:
            lines.append("  security_null_fields:")
            for field_name in sorted(null_fields):
                lines.append(f"    - {field_name}")

        lines.extend(
            [
                "",
                "extraction:",
                "  default_depth: 3",
                "  direction: both",
                "  validate: true",
                "",
                "output:",
                "  format: sql",
                "  include_transaction: true",
            ]
        )

        yaml_content = "\n".join(lines) + "\n"

        cmd = 'dbslice extract --config dbslice.yaml --seed "<table.column=value>"'

        return {
            "yaml": yaml_content,
            "command_template": cmd,
            "field_count": len(fields),
            "null_count": len(null_fields),
        }

    @staticmethod
    def _validate_provider(provider: str) -> dict:
        """Validate a Faker provider name."""
        from dbslice.compliance.transformers import CUSTOM_TRANSFORMERS

        if provider in CUSTOM_TRANSFORMERS:
            return {"valid": True, "provider": provider, "source": "custom_transformer"}

        try:
            from faker import Faker
        except ImportError:
            return {"valid": False, "error": "Faker not installed"}

        fake = Faker()
        method = getattr(fake, provider, None)
        if method is None or not callable(method):
            return {"valid": False, "error": f"Unknown provider '{provider}'"}

        try:
            sig = inspect.signature(method)
            for param in sig.parameters.values():
                if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
                    continue
                if param.default is inspect.Parameter.empty:
                    return {
                        "valid": False,
                        "error": f"Provider '{provider}' requires argument '{param.name}'",
                    }
        except (TypeError, ValueError):
            pass

        return {"valid": True, "provider": provider, "source": "faker"}


def _make_handler(server: MappingServer):
    """Create a request handler class bound to the server instance."""

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            pass

        def _check_token(self) -> bool:
            token = self.headers.get("X-DBSLICE-Token")
            if token != server.token:
                self._json_error(403, "Invalid or missing session token")
                return False
            return True

        def _json_response(self, data: dict, status: int = 200) -> None:
            body = json.dumps(data, default=str).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _json_error(self, status: int, message: str) -> None:
            self._json_response({"error": message}, status)

        def _read_json_body(self) -> dict[str, Any] | None:
            length = int(self.headers.get("Content-Length", 0))
            if length == 0:
                self._json_error(400, "Empty request body")
                return None
            try:
                result: dict[str, Any] = json.loads(self.rfile.read(length))
                return result
            except json.JSONDecodeError:
                self._json_error(400, "Invalid JSON")
                return None

        def do_GET(self) -> None:
            parsed = urlparse(self.path)

            if parsed.path == "/" or parsed.path == "":
                query = parse_qs(parsed.query)
                url_token = query.get("token", [None])[0]
                if url_token != server.token:
                    self.send_response(403)
                    self.send_header("Content-Type", "text/plain")
                    self.end_headers()
                    self.wfile.write(b"Invalid session token")
                    return

                from dbslice.mapping.ui import get_ui_html

                html = get_ui_html(server.token, server.database_url or "")
                body = html.encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                self._json_error(404, "Not found")

        def do_POST(self) -> None:
            parsed = urlparse(self.path)

            if not self._check_token():
                return

            if parsed.path == "/api/introspect":
                body = self._read_json_body()
                if body is None:
                    return
                try:
                    result = server._introspect(
                        database_url=body.get("database_url", ""),
                        schema=body.get("schema"),
                        detect_sensitive=body.get("detect_sensitive", True),
                    )
                    self._json_response(result)
                except Exception as e:
                    self._json_error(400, str(e))

            elif parsed.path == "/api/apply-profile":
                body = self._read_json_body()
                if body is None:
                    return
                try:
                    result = server._apply_profile(
                        profile_name=body.get("profile", ""),
                        current_mappings=body.get("current_mappings", {}),
                    )
                    self._json_response(result)
                except Exception as e:
                    self._json_error(400, str(e))

            elif parsed.path == "/api/generate-config":
                body = self._read_json_body()
                if body is None:
                    return
                try:
                    result = server._generate_config(
                        mappings=body.get("mappings", {}),
                    )
                    self._json_response(result)
                except Exception as e:
                    self._json_error(400, str(e))

            elif parsed.path == "/api/validate-provider":
                body = self._read_json_body()
                if body is None:
                    return
                try:
                    result = server._validate_provider(
                        provider=body.get("provider", ""),
                    )
                    self._json_response(result)
                except Exception as e:
                    self._json_error(400, str(e))

            else:
                self._json_error(404, "Not found")

    return Handler
