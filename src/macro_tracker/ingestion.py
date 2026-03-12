"""Ingestion orchestrator: reads registry, dispatches to connectors, stores data."""

import logging
from datetime import date

from macro_tracker.connectors.base import BaseConnector
from macro_tracker.connectors.fred import FredConnector
from macro_tracker.connectors.yfinance_conn import YFinanceConnector
from macro_tracker.db import Database
from macro_tracker.registry import load_registry

logger = logging.getLogger(__name__)


def _build_connectors() -> dict[str, BaseConnector]:
    connectors: dict[str, BaseConnector] = {"yfinance": YFinanceConnector()}
    try:
        connectors["fred"] = FredConnector()
    except ValueError:
        logger.warning("FRED_API_KEY not set — skipping FRED indicators")
    return connectors


def ingest_all(
    start_date: str | None = None,
    end_date: str | None = None,
    registry_path: str | None = None,
    db: Database | None = None,
) -> int:
    """Run full ingestion for all registered indicators.

    Returns total number of data points upserted.
    """
    from macro_tracker.config import START_DATE

    if start_date is None:
        start_date = START_DATE
    if end_date is None:
        end_date = date.today().isoformat()

    indicators = load_registry(registry_path)
    connectors = _build_connectors()
    db = db or Database()

    total = 0
    for spec in indicators:
        connector = connectors.get(spec.source.value)
        if connector is None:
            logger.warning("No connector for source %s (indicator %s)", spec.source, spec.id)
            continue

        points = connector.fetch_safe(spec, start_date, end_date)
        if points:
            count = db.upsert_datapoints(points)
            total += count
            logger.info("Stored %d points for %s", count, spec.id)
        else:
            logger.info("No data for %s", spec.id)

    logger.info("Ingestion complete. Total points upserted: %d", total)
    return total


def ingest_indicator(
    indicator_id: str,
    start_date: str | None = None,
    end_date: str | None = None,
    registry_path: str | None = None,
    db: Database | None = None,
) -> int:
    """Ingest a single indicator by ID."""
    from macro_tracker.config import START_DATE

    if start_date is None:
        start_date = START_DATE
    if end_date is None:
        end_date = date.today().isoformat()

    indicators = load_registry(registry_path)
    spec = next((i for i in indicators if i.id == indicator_id), None)
    if spec is None:
        raise ValueError(f"Indicator '{indicator_id}' not found in registry")

    connectors = _build_connectors()
    connector = connectors.get(spec.source.value)
    if connector is None:
        raise ValueError(f"No connector for source: {spec.source}")

    db = db or Database()
    points = connector.fetch_safe(spec, start_date, end_date)
    if points:
        return db.upsert_datapoints(points)
    return 0
