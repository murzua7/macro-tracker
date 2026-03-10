"""SQLite persistence layer for the macro tracker."""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Iterable

import pandas as pd


LOGGER = logging.getLogger(__name__)


class MacroDataStore:
    """Persist transformed macro data into SQLite with upsert semantics."""

    def __init__(self, database_path: Path) -> None:
        self.database_path = Path(database_path)

    def get_last_loaded_date(self) -> pd.Timestamp | None:
        """Return the max loaded date from the SQLite fact table."""
        if not self.database_path.exists():
            return None

        with sqlite3.connect(self.database_path) as connection:
            try:
                cursor = connection.execute("SELECT MAX(date) FROM macro_daily;")
                row = cursor.fetchone()
            except sqlite3.OperationalError:
                return None

        if not row or row[0] is None:
            return None
        return pd.Timestamp(row[0])

    def ensure_table(self, columns: Iterable[str]) -> None:
        """Create the macro_daily table if it does not already exist."""
        metric_columns = [
            f'"{column}" REAL' for column in columns if column != "date"
        ]
        ddl = """
        CREATE TABLE IF NOT EXISTS macro_daily (
            date TEXT PRIMARY KEY,
            {metric_columns}
        );
        """.format(metric_columns=", ".join(metric_columns))

        with sqlite3.connect(self.database_path) as connection:
            connection.execute(ddl)
            existing = {
                row[1] for row in connection.execute("PRAGMA table_info(macro_daily);")
            }
            for column in columns:
                if column != "date" and column not in existing:
                    connection.execute(
                        f'ALTER TABLE macro_daily ADD COLUMN "{column}" REAL;'
                    )
            connection.commit()

    def upsert_daily_frame(self, frame: pd.DataFrame) -> int:
        """Upsert transformed rows into SQLite."""
        if frame.empty:
            LOGGER.warning("No transformed rows available for database load.")
            return 0

        payload = frame.copy()
        payload["date"] = pd.to_datetime(payload["date"]).dt.strftime("%Y-%m-%d")
        columns = list(payload.columns)
        self.ensure_table(columns)

        placeholders = ", ".join("?" for _ in columns)
        quoted_columns = ", ".join(f'"{column}"' for column in columns)
        update_clause = ", ".join(
            f'"{column}" = excluded."{column}"' for column in columns if column != "date"
        )
        statement = f"""
            INSERT INTO macro_daily ({quoted_columns})
            VALUES ({placeholders})
            ON CONFLICT(date) DO UPDATE SET
            {update_clause};
        """

        rows = []
        for row in payload.itertuples(index=False, name=None):
            normalized_row = []
            for column, value in zip(columns, row):
                if column == "date":
                    normalized_row.append(value)
                elif pd.isna(value):
                    normalized_row.append(None)
                else:
                    normalized_row.append(float(value))
            rows.append(tuple(normalized_row))

        with sqlite3.connect(self.database_path) as connection:
            connection.executemany(statement, rows)
            connection.commit()

        LOGGER.info("Upserted %s rows into %s", len(rows), self.database_path)
        return len(rows)
