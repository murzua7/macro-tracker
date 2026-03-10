"""Analytics helpers for querying and summarizing stored macro data."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class IndicatorView:
    """Metadata used by the dashboard to organize indicators."""

    label: str
    column: str
    category: str
    yoy_column: str | None = None
    mom_column: str | None = None


INDICATOR_VIEWS: tuple[IndicatorView, ...] = (
    IndicatorView(
        label="CPI",
        column="cpi",
        category="Inflation",
        yoy_column="cpi_yoy_pct",
        mom_column="cpi_mom_pct",
    ),
    IndicatorView(
        label="Core PCE",
        column="core_pce",
        category="Inflation",
        yoy_column="core_pce_yoy_pct",
        mom_column="core_pce_mom_pct",
    ),
    IndicatorView(
        label="Non-Farm Payrolls",
        column="non_farm_payrolls",
        category="Employment",
        yoy_column="non_farm_payrolls_yoy_pct",
        mom_column="non_farm_payrolls_mom_pct",
    ),
    IndicatorView(
        label="Unemployment Rate",
        column="unemployment_rate",
        category="Employment",
    ),
    IndicatorView(
        label="Effective Fed Funds Rate",
        column="effective_fed_funds_rate",
        category="Rates",
    ),
    IndicatorView(
        label="10Y-2Y Treasury Spread",
        column="ten_year_two_year_spread",
        category="Rates",
    ),
    IndicatorView(
        label="Real GDP",
        column="real_gdp",
        category="Growth",
        yoy_column="real_gdp_yoy_pct",
    ),
    IndicatorView(
        label="S&P 500 Proxy",
        column="sp500_proxy",
        category="Market Proxies",
    ),
    IndicatorView(
        label="DXY Proxy",
        column="dxy_proxy",
        category="Market Proxies",
    ),
    IndicatorView(
        label="10Y Treasury Yield Proxy",
        column="ten_year_yield_proxy",
        category="Market Proxies",
    ),
)


class MacroAnalytics:
    """Read macro data from SQLite and prepare dashboard-ready views."""

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = Path(database_path)

    def load_daily_frame(self) -> pd.DataFrame:
        """Load the daily macro fact table from SQLite."""
        if not self.database_path.exists():
            return pd.DataFrame()

        with sqlite3.connect(self.database_path) as connection:
            frame = pd.read_sql_query(
                "SELECT * FROM macro_daily ORDER BY date ASC;",
                connection,
                parse_dates=["date"],
            )

        return frame

    def latest_snapshot(self, frame: pd.DataFrame) -> pd.DataFrame:
        """Return the latest non-null reading for each tracked indicator."""
        rows = []
        for indicator in INDICATOR_VIEWS:
            if indicator.column not in frame.columns:
                continue

            subset = frame[["date", indicator.column]].dropna()
            if subset.empty:
                continue

            latest = subset.iloc[-1]
            rows.append(
                {
                    "label": indicator.label,
                    "category": indicator.category,
                    "date": latest["date"],
                    "value": latest[indicator.column],
                    "column": indicator.column,
                    "yoy_column": indicator.yoy_column,
                    "mom_column": indicator.mom_column,
                }
            )

        return pd.DataFrame(rows)

    def build_category_frame(self, frame: pd.DataFrame, category: str) -> pd.DataFrame:
        """Return the subset of indicators belonging to one category."""
        columns = ["date"]
        for indicator in INDICATOR_VIEWS:
            if indicator.category == category and indicator.column in frame.columns:
                columns.append(indicator.column)
        return frame[columns].copy() if len(columns) > 1 else pd.DataFrame()
