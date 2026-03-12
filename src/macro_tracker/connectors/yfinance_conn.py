"""Yahoo Finance data connector using yfinance."""

import logging

import pandas as pd
import yfinance as yf

from macro_tracker.connectors.base import BaseConnector, network_retry
from macro_tracker.schema import DataPoint, Frequency, IndicatorSpec, Source

logger = logging.getLogger(__name__)


class YFinanceConnector(BaseConnector):
    source_name = "yfinance"

    @network_retry
    def fetch(
        self, spec: IndicatorSpec, start_date: str, end_date: str
    ) -> list[DataPoint]:
        logger.info("Fetching YFinance ticker %s (%s)", spec.id, spec.code)

        ticker = yf.Ticker(spec.code)
        df: pd.DataFrame = ticker.history(start=start_date, end=end_date, auto_adjust=True)

        if df is None or df.empty:
            logger.warning("No data returned for %s", spec.code)
            return []

        points = []
        for ts, row in df.iterrows():
            dt = pd.Timestamp(ts).to_pydatetime()
            if dt.tzinfo is not None:
                dt = dt.replace(tzinfo=None)
            close_val = row.get("Close")
            if close_val is None or pd.isna(close_val):
                continue
            points.append(
                DataPoint(
                    timestamp=dt,
                    indicator_id=spec.id,
                    value=float(close_val),
                    unit=spec.unit,
                    frequency=Frequency(spec.frequency.value),
                    source=Source.YFINANCE,
                    source_code=spec.code,
                )
            )

        logger.info("Fetched %d points for %s", len(points), spec.id)
        return points
