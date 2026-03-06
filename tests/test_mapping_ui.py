"""Tests for the column mapping UI server and API."""

import json
import threading
import time
from http.client import HTTPConnection

import pytest

from dbslice.mapping.server import MappingServer


@pytest.fixture()
def server():
    """Start a mapping UI server on a random-ish port for testing."""
    srv = MappingServer(port=19473, database_url="", schema=None)
    thread = threading.Thread(target=srv.start, kwargs={"open_browser": False}, daemon=True)
    thread.start()
    time.sleep(0.3)  # Wait for server to bind
    yield srv
    if srv._server:
        srv._server.shutdown()


def _conn():
    return HTTPConnection("127.0.0.1", 19473, timeout=5)


def _post(conn, path, body, token):
    conn.request(
        "POST",
        path,
        json.dumps(body).encode(),
        {"Content-Type": "application/json", "X-DBSLICE-Token": token},
    )
    return conn.getresponse()


class TestTokenSecurity:
    def test_get_without_token_fails(self, server):
        conn = _conn()
        conn.request("GET", "/")
        resp = conn.getresponse()
        assert resp.status == 403

    def test_get_with_wrong_token_fails(self, server):
        conn = _conn()
        conn.request("GET", "/?token=wrong")
        resp = conn.getresponse()
        assert resp.status == 403

    def test_get_with_valid_token_serves_html(self, server):
        conn = _conn()
        conn.request("GET", f"/?token={server.token}")
        resp = conn.getresponse()
        assert resp.status == 200
        body = resp.read().decode()
        assert "dbslice" in body
        assert "Column Mapping" in body

    def test_post_without_token_fails(self, server):
        conn = _conn()
        conn.request(
            "POST",
            "/api/validate-provider",
            json.dumps({"provider": "email"}).encode(),
            {"Content-Type": "application/json"},
        )
        resp = conn.getresponse()
        assert resp.status == 403

    def test_post_with_wrong_token_fails(self, server):
        conn = _conn()
        resp = _post(conn, "/api/validate-provider", {"provider": "email"}, "bad-token")
        assert resp.status == 403


class TestValidateProviderAPI:
    def test_valid_faker_provider(self, server):
        conn = _conn()
        resp = _post(conn, "/api/validate-provider", {"provider": "email"}, server.token)
        assert resp.status == 200
        data = json.loads(resp.read())
        assert data["valid"] is True
        assert data["source"] == "faker"

    def test_valid_custom_transformer(self, server):
        conn = _conn()
        resp = _post(conn, "/api/validate-provider", {"provider": "year_only"}, server.token)
        assert resp.status == 200
        data = json.loads(resp.read())
        assert data["valid"] is True
        assert data["source"] == "custom_transformer"

    def test_invalid_provider(self, server):
        conn = _conn()
        resp = _post(conn, "/api/validate-provider", {"provider": "not_a_real_provider_xyz"}, server.token)
        assert resp.status == 200
        data = json.loads(resp.read())
        assert data["valid"] is False

    def test_hipaa_zip3_is_valid(self, server):
        conn = _conn()
        resp = _post(conn, "/api/validate-provider", {"provider": "hipaa_zip3"}, server.token)
        data = json.loads(resp.read())
        assert data["valid"] is True


class TestGenerateConfigAPI:
    def test_generate_basic_config(self, server):
        conn = _conn()
        mappings = {
            "users.email": {"action": "anonymize", "provider": "email"},
            "users.password_hash": {"action": "null", "provider": ""},
        }
        resp = _post(conn, "/api/generate-config", {"mappings": mappings}, server.token)
        assert resp.status == 200
        data = json.loads(resp.read())
        assert "yaml" in data
        assert "users.email: email" in data["yaml"]
        assert "users.password_hash" in data["yaml"]
        assert data["field_count"] == 1
        assert data["null_count"] == 1
        assert "command_template" in data
        assert "dbslice extract" in data["command_template"]

    def test_generate_empty_config(self, server):
        conn = _conn()
        resp = _post(conn, "/api/generate-config", {"mappings": {}}, server.token)
        assert resp.status == 200
        data = json.loads(resp.read())
        assert data["field_count"] == 0
        assert data["null_count"] == 0

    def test_keep_action_excluded(self, server):
        conn = _conn()
        mappings = {
            "users.email": {"action": "keep", "provider": ""},
        }
        resp = _post(conn, "/api/generate-config", {"mappings": mappings}, server.token)
        data = json.loads(resp.read())
        assert "users.email" not in data["yaml"]

    def test_generated_yaml_is_valid(self, server):
        """The generated YAML should parse without errors."""
        import yaml

        conn = _conn()
        mappings = {
            "users.email": {"action": "anonymize", "provider": "email"},
            "users.ssn": {"action": "anonymize", "provider": "ssn"},
            "users.token": {"action": "null", "provider": ""},
        }
        resp = _post(conn, "/api/generate-config", {"mappings": mappings}, server.token)
        data = json.loads(resp.read())
        parsed = yaml.safe_load(data["yaml"])
        assert parsed["anonymization"]["enabled"] is True
        assert "users.email" in parsed["anonymization"]["fields"]
        assert "users.token" in parsed["anonymization"]["security_null_fields"]


class TestNotFoundRoutes:
    def test_unknown_get(self, server):
        conn = _conn()
        conn.request("GET", f"/unknown?token={server.token}")
        resp = conn.getresponse()
        assert resp.status == 404

    def test_unknown_post(self, server):
        conn = _conn()
        resp = _post(conn, "/api/unknown", {}, server.token)
        assert resp.status == 404


class TestUIContent:
    def test_html_contains_token(self, server):
        conn = _conn()
        conn.request("GET", f"/?token={server.token}")
        resp = conn.getresponse()
        body = resp.read().decode()
        assert server.token in body

    def test_html_loads_from_static_file(self, server):
        """UI should load from the static HTML file, not inline string."""
        conn = _conn()
        conn.request("GET", f"/?token={server.token}")
        resp = conn.getresponse()
        body = resp.read().decode()
        # Should contain Tailwind CDN (intentional external resource)
        assert "tailwindcss" in body
        # Should contain key UI elements
        assert "Column Mapping" in body
        assert "Introspect Schema" in body
        assert "Compliance Profiles" in body

    def test_html_has_proper_structure(self, server):
        """UI should have accessibility and structural elements."""
        conn = _conn()
        conn.request("GET", f"/?token={server.token}")
        resp = conn.getresponse()
        body = resp.read().decode()
        assert 'lang="en"' in body
        assert 'aria-label' in body
        assert 'aria-live="polite"' in body
