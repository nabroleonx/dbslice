"""
dbslice - Extract minimal, referentially-intact database subsets.

A CLI tool for extracting database subsets that maintain referential integrity,
useful for local development, debugging, and creating test fixtures.
"""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("dbslice")
except PackageNotFoundError:
    # Fallback for local source execution without installed metadata.
    __version__ = "0.2.0"

__all__ = ["__version__"]
