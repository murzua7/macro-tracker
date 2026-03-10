"""Extraction utilities for FRED and Yahoo Finance data."""

from __future__ import annotations

import logging
from datetime import date

import pandas as pd
import yfinance as yf
from fredapi import Fred

from .config import IndicatorDefinition


LOGGER = logging.getLogger(__name__)


class MacroDataExtractor:
    """Fetch raw macroeconomic series from remote providers."""

    def __init__(self, fred_api_key: str) -> None:
        self._fred_client = Fred(api_key=fred_api_key) if fred_api_key else None

    def fetch_fred_series(
        self,
        indicator: IndicatorDefinition,
        start_date: date,
        end_date: date,
    ) -> pd.Series:
        """Fetch a single FRED series and return a normalized series."""
        if self._fred_client is None:
            raise ValueError("FRED_API_KEY is required to fetch FRED series.")

        try:
            LOGGER.info(
                "Fetching FRED series %s (%s) from %s to %s",
                indicator.key,
                indicator.source_id,
                start_date,
                end_date,
            )
            series = self._fred_client.get_series(
                indicator.source_id,
                observation_start=start_date,
                observation_end=end_date,
            )
        except Exception as exc:
            LOGGER.exception(
                "FRED request failed for %s (%s): %s",
                indicator.key,
                indicator.source_id,
                exc,
            )
            return pd.Series(name=indicator.key, dtype="float64")

        if series.empty:
            LOGGER.warning("FRED series %s returned no rows.", indicator.key)
            return pd.Series(name=indicator.key, dtype="float64")

        normalized = pd.Series(series, name=indicator.key, dtype="float64")
        normalized.index = pd.to_datetime(normalized.index).tz_localize(None)
        normalized = normalized.sort_index()
        return normalized

    def fetch_yfinance_series(
        self,
        indicator: IndicatorDefinition,
        start_date: date,
        end_date: date,
    ) -> pd.Series:
        """Fetch daily adjusted close data from Yahoo Finance."""
        try:
            LOGGER.info(
                "Fetching yfinance series %s (%s) from %s to %s",
                indicator.key,
                indicator.source_id,
                start_date,
                end_date,
            )
            frame = yf.download(
                indicator.source_id,
                start=start_date,
                end=end_date + pd.Timedelta(days=1),
                progress=False,
                auto_adjust=False,
                threads=False,
            )
        except Exception as exc:
            LOGGER.exception(
                "yfinance request failed for %s (%s): %s",
                indicator.key,
                indicator.source_id,
                exc,
            )
            return pd.Series(name=indicator.key, dtype="float64")

        if frame.empty or "Adj Close" not in frame.columns:
            LOGGER.warning("yfinance series %s returned no usable rows.", indicator.key)
            return pd.Series(name=indicator.key, dtype="float64")

        series = frame["Adj Close"]
        if isinstance(series, pd.DataFrame):
            series = series.iloc[:, 0]

        series = series.astype("float64").rename(indicator.key)
        series.index = pd.to_datetime(series.index).tz_localize(None)
        return series.sort_index()
