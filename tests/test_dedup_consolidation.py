"""
Tests for deduplication consolidation safety guards.

These tests exercise the execute_consolidation method using an in-memory
SQLite-compatible schema simulation (via mocked psycopg) so that they run
without a real database while still validating the guard logic.
"""

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch, call

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from similarity_deduplication import ProductDeduplicator


class _FakeCursor:
    """Minimal cursor stub that supports execute/fetchone/rowcount."""

    def __init__(self, rows_by_query: dict):
        self._rows = rows_by_query  # maps param_value → row_or_None
        self.rowcount = 0
        self._last_params: tuple = ()

    def execute(self, query: str, params: tuple = ()):
        self._last_query = query
        self._last_params = params
        # Rowcount for UPDATE/DELETE stubs
        self.rowcount = 1

    def fetchone(self):
        # Return a truthy result if the first param is in the allowed set
        if self._last_params and self._last_params[0] in self._rows:
            return self._rows[self._last_params[0]]
        return None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        pass


class ConsolidationGuardTests(unittest.TestCase):
    """execute_consolidation must raise ValueError for missing keys."""

    def _make_dedup(self):
        dedup = ProductDeduplicator.__new__(ProductDeduplicator)
        dedup.db_url = "postgresql://fake/db"
        dedup.model = None
        dedup.cache = None
        dedup.consolidations = []
        return dedup

    def test_raises_when_canonical_key_missing(self):
        """Migrating to a non-existent canonical key must be blocked."""
        dedup = self._make_dedup()

        # Only the source key exists; canonical is absent.
        cursor = _FakeCursor({"source_key_exists": (1,)})

        def _mock_connect(url):
            conn = MagicMock()
            conn.__enter__ = lambda s: s
            conn.__exit__ = MagicMock(return_value=False)
            conn.cursor = MagicMock(return_value=cursor)
            return conn

        with patch("similarity_deduplication.psycopg.connect", side_effect=_mock_connect):
            with self.assertRaises(ValueError) as ctx:
                dedup.execute_consolidation(
                    source_key="source_key_exists",
                    canonical_key="canonical_key_missing",
                    similarity=0.97,
                )

        self.assertIn("canonical_key_missing", str(ctx.exception))
        self.assertIn("not found", str(ctx.exception))

    def test_raises_when_source_key_missing(self):
        """Consolidating an already-deleted source key must be blocked."""
        dedup = self._make_dedup()

        # Only the canonical key exists; source is absent.
        cursor = _FakeCursor({"canonical_key_exists": (1,)})

        def _mock_connect(url):
            conn = MagicMock()
            conn.__enter__ = lambda s: s
            conn.__exit__ = MagicMock(return_value=False)
            conn.cursor = MagicMock(return_value=cursor)
            return conn

        with patch("similarity_deduplication.psycopg.connect", side_effect=_mock_connect):
            with self.assertRaises(ValueError) as ctx:
                dedup.execute_consolidation(
                    source_key="source_key_missing",
                    canonical_key="canonical_key_exists",
                    similarity=0.97,
                )

        self.assertIn("source_key_missing", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
