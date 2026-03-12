"""FRED data connector using fredapi."""

import logging

import pandas as pd
from fredapi import Fred

from macro_tracker.config import FRED_API_KEY
from macro_tracker.connectors.base import BaseConnector, network_retry
from macro_tracker.schema import DataPoint, Frequency, IndicatorSpec, Source

logger = logging.getLogger(__name__)


class FredConnector(BaseConnector):
    source_name = "fred"

    def __init__(self, api_key: str | None = None):
        self._api_key = api_key or FRED_API_KEY
        if not self._api_key:
            raise ValueError("FRED_API_KEY is required. Set it in .env or pass directly.")
        self._client = Fred(api_key=self._api_key)

    @network_retry
    def fetch(
        self, spec: IndicatorSpec, start_date: str, end_date: str
    ) -> list[DataPoint]:
        logger.info("Fetching FRED series %s (%s)", spec.id, spec.code)

        series: pd.Series = self._client.get_series(
            spec.code,
            observation_start=start_date,
            observation_end=end_date,
        )

        if series is None or series.empty:
            logger.warning("No data returned for %s", spec.code)
            return []

        # Drop NaN values
        series = series.dropna()

        points = []
        for ts, value in series.items():
            dt = pd.Timestamp(ts).to_pydatetime()
            if dt.tzinfo is not None:
                dt = dt.replace(tzinfo=None)
            points.append(
                DataPoint(
                    timestamp=dt,
                    indicator_id=spec.id,
                    value=float(value),
                    unit=spec.unit,
                    frequency=Frequency(spec.frequency.value),
                    source=Source.FRED,
                    source_code=spec.code,
                )
            )

        logger.info("Fetched %d points for %s", len(points), spec.id)
        return points
