"""Database layer: SQLite storage with Postgres-ready schema design."""

import logging
import sqlite3

from macro_tracker.config import get_sqlite_path
from macro_tracker.schema import DataPoint

logger = logging.getLogger(__name__)

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS observations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    indicator_id TEXT NOT NULL,
    value REAL NOT NULL,
    unit TEXT NOT NULL,
    frequency TEXT NOT NULL,
    source TEXT NOT NULL,
    source_code TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(timestamp, indicator_id)
);
"""

CREATE_INDEX = """
CREATE INDEX IF NOT EXISTS idx_obs_indicator_ts
ON observations (indicator_id, timestamp);
"""


class Database:
    """SQLite-backed storage with upsert semantics."""

    def __init__(self, db_path: str | None = None):
        self.db_path = db_path or get_sqlite_path()
        self._init_schema()

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_schema(self) -> None:
        with self._get_conn() as conn:
            conn.execute(CREATE_TABLE)
            conn.execute(CREATE_INDEX)

    def upsert_datapoints(self, points: list[DataPoint]) -> int:
        """Insert or update data points. Returns count of rows affected."""
        if not points:
            return 0

        sql = """
        INSERT INTO observations
            (timestamp, indicator_id, value, unit, frequency, source, source_code)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(timestamp, indicator_id) DO UPDATE SET
            value = excluded.value,
            unit = excluded.unit,
            frequency = excluded.frequency,
            source = excluded.source,
            source_code = excluded.source_code
        """
        rows = [
            (
                p.timestamp.isoformat(),
                p.indicator_id,
                p.value,
                p.unit,
                p.frequency.value,
                p.source.value,
                p.source_code,
            )
            for p in points
        ]
        with self._get_conn() as conn:
            conn.executemany(sql, rows)
            count = len(rows)
        logger.info("Upserted %d data points", count)
        return count

    def get_timeseries(
        self,
        indicator_id: str,
        start: str | None = None,
        end: str | None = None,
        limit: int = 5000,
    ) -> list[dict]:
        """Fetch timeseries for an indicator, optionally filtered by date range."""
        sql = "SELECT timestamp, value FROM observations WHERE indicator_id = ?"
        params: list = [indicator_id]

        if start:
            sql += " AND timestamp >= ?"
            params.append(start)
        if end:
            sql += " AND timestamp <= ?"
            params.append(end)

        sql += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)

        with self._get_conn() as conn:
            rows = conn.execute(sql, params).fetchall()

        return [{"timestamp": r["timestamp"], "value": r["value"]} for r in reversed(rows)]

    def get_latest(self, indicator_id: str) -> dict | None:
        """Get the most recent observation for an indicator."""
        sql = """
        SELECT timestamp, value, unit, source
        FROM observations
        WHERE indicator_id = ?
        ORDER BY timestamp DESC
        LIMIT 1
        """
        with self._get_conn() as conn:
            row = conn.execute(sql, (indicator_id,)).fetchone()
        if row:
            return dict(row)
        return None

    def get_all_latest(self) -> list[dict]:
        """Get the most recent observation for every indicator."""
        sql = """
        SELECT indicator_id, timestamp, value, unit, source
        FROM observations o1
        WHERE timestamp = (
            SELECT MAX(timestamp) FROM observations o2
            WHERE o2.indicator_id = o1.indicator_id
        )
        ORDER BY indicator_id
        """
        with self._get_conn() as conn:
            rows = conn.execute(sql).fetchall()
        return [dict(r) for r in rows]

    def get_freshness(self) -> list[dict]:
        """Get the latest timestamp per indicator for freshness monitoring."""
        sql = """
        SELECT indicator_id, MAX(timestamp) as latest_timestamp, COUNT(*) as total_points
        FROM observations
        GROUP BY indicator_id
        ORDER BY indicator_id
        """
        with self._get_conn() as conn:
            rows = conn.execute(sql).fetchall()
        return [dict(r) for r in rows]

    def get_indicator_ids(self) -> list[str]:
        """List all indicator IDs that have data."""
        sql = "SELECT DISTINCT indicator_id FROM observations ORDER BY indicator_id"
        with self._get_conn() as conn:
            rows = conn.execute(sql).fetchall()
        return [r["indicator_id"] for r in rows]
