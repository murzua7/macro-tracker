"""Main ETL orchestration for daily US macroeconomic tracking."""

from __future__ import annotations

import logging
from datetime import date, timedelta

import pandas as pd

from .config import IndicatorDefinition, MacroTrackerConfig
from .extract import MacroDataExtractor
from .storage import MacroDataStore
from .transform import build_daily_macro_frame


LOGGER = logging.getLogger(__name__)


class MacroTracker:
    """Run the end-to-end macroeconomic ETL pipeline."""

    def __init__(self, config: MacroTrackerConfig | None = None) -> None:
        self.config = config or MacroTrackerConfig()
        self.extractor = MacroDataExtractor(self.config.fred_api_key)
        self.store = MacroDataStore(self.config.database_path)

    def _compute_fetch_window(self) -> tuple[date, date]:
        """Calculate the incremental fetch window with a lookback buffer."""
        last_loaded = self.store.get_last_loaded_date()
        if last_loaded is None:
            start = pd.Timestamp(self.config.start_date).date()
        else:
            start = (
                last_loaded - pd.Timedelta(days=self.config.daily_lookback_days)
            ).date()
        end = date.today()
        return start, end

    def _fetch_indicator(
        self,
        definition: IndicatorDefinition,
        start_date: date,
        end_date: date,
    ) -> pd.Series:
        """Dispatch extraction based on provider."""
        if definition.source == "fred":
            return self.extractor.fetch_fred_series(definition, start_date, end_date)
        if definition.source == "yfinance":
            return self.extractor.fetch_yfinance_series(definition, start_date, end_date)
        raise ValueError(f"Unsupported source: {definition.source}")

    def run_daily_update(self) -> pd.DataFrame:
        """Execute extract, transform, and load for all tracked indicators."""
        start_date, end_date = self._compute_fetch_window()
        LOGGER.info("Starting macro tracker run from %s to %s", start_date, end_date)

        raw_series: dict[str, pd.Series] = {}
        for definition in self.config.indicators.values():
            series = self._fetch_indicator(definition, start_date, end_date)
            if not series.empty:
                raw_series[definition.key] = series
            else:
                LOGGER.warning("Skipping empty series for %s", definition.key)

        transformed = build_daily_macro_frame(
            raw_series=raw_series,
            definitions=self.config.indicators.values(),
        )
        if transformed.empty:
            LOGGER.warning("Transformation produced no rows.")
            return transformed

        rows_upserted = self.store.upsert_daily_frame(transformed)
        LOGGER.info("Macro tracker run complete. Rows upserted: %s", rows_upserted)
        return transformed
