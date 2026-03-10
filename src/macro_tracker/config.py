"""Configuration objects and indicator metadata for the macro tracker."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class IndicatorDefinition:
    """Metadata for an individual time series."""

    key: str
    source: str
    source_id: str
    frequency: str
    compute_mom: bool = False
    compute_yoy: bool = False


@dataclass(frozen=True)
class MacroTrackerConfig:
    """Runtime configuration for the ETL pipeline."""

    fred_api_key: str = field(default_factory=lambda: os.getenv("FRED_API_KEY", ""))
    database_path: Path = field(
        default_factory=lambda: Path(
            os.getenv("MACRO_DB_PATH", "macro_data.db")
        ).expanduser()
    )
    log_path: Path = field(
        default_factory=lambda: Path(
            os.getenv("MACRO_LOG_PATH", "macro_tracker.log")
        ).expanduser()
    )
    start_date: str = os.getenv("MACRO_START_DATE", "2000-01-01")
    daily_lookback_days: int = int(os.getenv("MACRO_LOOKBACK_DAYS", "400"))

    @property
    def indicators(self) -> dict[str, IndicatorDefinition]:
        """Return all tracked indicators."""
        return {
            "cpi": IndicatorDefinition(
                key="cpi",
                source="fred",
                source_id="CPIAUCSL",
                frequency="monthly",
                compute_mom=True,
                compute_yoy=True,
            ),
            "core_pce": IndicatorDefinition(
                key="core_pce",
                source="fred",
                source_id="PCEPILFE",
                frequency="monthly",
                compute_mom=True,
                compute_yoy=True,
            ),
            "non_farm_payrolls": IndicatorDefinition(
                key="non_farm_payrolls",
                source="fred",
                source_id="PAYEMS",
                frequency="monthly",
                compute_mom=True,
                compute_yoy=True,
            ),
            "unemployment_rate": IndicatorDefinition(
                key="unemployment_rate",
                source="fred",
                source_id="UNRATE",
                frequency="monthly",
            ),
            "effective_fed_funds_rate": IndicatorDefinition(
                key="effective_fed_funds_rate",
                source="fred",
                source_id="DFF",
                frequency="daily",
            ),
            "ten_year_two_year_spread": IndicatorDefinition(
                key="ten_year_two_year_spread",
                source="fred",
                source_id="T10Y2Y",
                frequency="daily",
            ),
            "real_gdp": IndicatorDefinition(
                key="real_gdp",
                source="fred",
                source_id="GDPC1",
                frequency="quarterly",
                compute_yoy=True,
            ),
            "sp500_proxy": IndicatorDefinition(
                key="sp500_proxy",
                source="yfinance",
                source_id="^GSPC",
                frequency="daily",
            ),
            "dxy_proxy": IndicatorDefinition(
                key="dxy_proxy",
                source="yfinance",
                source_id="DX-Y.NYB",
                frequency="daily",
            ),
            "ten_year_yield_proxy": IndicatorDefinition(
                key="ten_year_yield_proxy",
                source="yfinance",
                source_id="^TNX",
                frequency="daily",
            ),
        }
