from pathlib import Path

_STATIC_DIR = Path(__file__).parent / "static"
_TEMPLATE_CACHE: str | None = None


def get_ui_html(token: str, initial_url: str) -> str:
    """Return the complete HTML page with token and initial URL embedded."""
    global _TEMPLATE_CACHE  # noqa: PLW0603
    if _TEMPLATE_CACHE is None:
        template_path = _STATIC_DIR / "index.html"
        _TEMPLATE_CACHE = template_path.read_text(encoding="utf-8")

    return (
        _TEMPLATE_CACHE
        .replace("{{TOKEN}}", token)
        .replace("{{INITIAL_URL}}", initial_url)
    )
