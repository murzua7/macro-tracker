"""Tests for data connectors (FRED, YFinance, Placeholder)."""

from unittest.mock import MagicMock, patch

import pandas as pd

from macro_tracker.schema import (
    Category,
    DataPoint,
    Frequency,
    IndicatorSpec,
    Source,
)


def _make_spec(source="fred", code="TEST", **kwargs) -> IndicatorSpec:
    defaults = dict(
        id="test_ind",
        name="Test",
        category=Category.MACRO_LABOR,
        source=source,
        frequency=Frequency.MONTHLY,
        code=code,
        unit="percent",
    )
    defaults.update(kwargs)
    return IndicatorSpec(**defaults)


class TestFredConnector:
    @patch("macro_tracker.connectors.fred.Fred")
    def test_fetch_returns_datapoints(self, MockFred):
        mock_client = MagicMock()
        MockFred.return_value = mock_client

        # Simulate FRED returning a pandas Series
        dates = pd.date_range("2024-01-01", periods=3, freq="MS")
        series = pd.Series([3.1, 3.2, 3.3], index=dates)
        mock_client.get_series.return_value = series

        from macro_tracker.connectors.fred import FredConnector

        connector = FredConnector(api_key="fake_key")
        spec = _make_spec(source="fred", code="CPIAUCSL")

        points = connector.fetch(spec, "2024-01-01", "2024-03-31")

        assert len(points) == 3
        assert all(isinstance(p, DataPoint) for p in points)
        assert points[0].value == 3.1
        assert points[0].source == Source.FRED
        assert points[0].indicator_id == "test_ind"

    @patch("macro_tracker.connectors.fred.Fred")
    def test_fetch_empty_series(self, MockFred):
        mock_client = MagicMock()
        MockFred.return_value = mock_client
        mock_client.get_series.return_value = pd.Series(dtype=float)

        from macro_tracker.connectors.fred import FredConnector

        connector = FredConnector(api_key="fake_key")
        spec = _make_spec(source="fred", code="EMPTY")

        points = connector.fetch(spec, "2024-01-01", "2024-03-31")
        assert points == []


class TestYFinanceConnector:
    @patch("macro_tracker.connectors.yfinance_conn.yf.Ticker")
    def test_fetch_returns_datapoints(self, MockTicker):
        mock_ticker = MagicMock()
        MockTicker.return_value = mock_ticker

        dates = pd.date_range("2024-01-02", periods=3, freq="B")
        df = pd.DataFrame(
            {"Close": [150.0, 151.5, 152.0], "Volume": [1e6, 1.1e6, 9e5]},
            index=dates,
        )
        mock_ticker.history.return_value = df

        from macro_tracker.connectors.yfinance_conn import YFinanceConnector

        connector = YFinanceConnector()
        spec = _make_spec(source="yfinance", code="SPY", frequency=Frequency.DAILY, unit="usd")

        points = connector.fetch(spec, "2024-01-01", "2024-01-05")

        assert len(points) == 3
        assert all(isinstance(p, DataPoint) for p in points)
        assert points[0].value == 150.0
        assert points[0].source == Source.YFINANCE

    @patch("macro_tracker.connectors.yfinance_conn.yf.Ticker")
    def test_fetch_empty_dataframe(self, MockTicker):
        mock_ticker = MagicMock()
        MockTicker.return_value = mock_ticker
        mock_ticker.history.return_value = pd.DataFrame()

        from macro_tracker.connectors.yfinance_conn import YFinanceConnector

        connector = YFinanceConnector()
        spec = _make_spec(source="yfinance", code="EMPTY", frequency=Frequency.DAILY, unit="usd")

        points = connector.fetch(spec, "2024-01-01", "2024-01-05")
        assert points == []


class TestPlaceholderConnector:
    def test_returns_empty(self):
        from macro_tracker.connectors.placeholder import PlaceholderConnector

        connector = PlaceholderConnector()
        spec = _make_spec(source="placeholder", code="SPX_OPT_VOL", category=Category.DERIVATIVES)
        points = connector.fetch(spec, "2024-01-01", "2024-12-31")
        assert points == []
