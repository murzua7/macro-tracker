"""Canonical data models for the macrofinance tracker."""

from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field


class Source(str, Enum):
    FRED = "fred"
    YFINANCE = "yfinance"
    PLACEHOLDER = "placeholder"


class Frequency(str, Enum):
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"


class Category(str, Enum):
    MACRO_ACTIVITY = "macro_activity"
    LABOR = "labor"
    INFLATION_PRICES = "inflation_prices"
    RATES_CURVE = "rates_curve"
    CREDIT_CONDITIONS = "credit_conditions"
    HOUSING = "housing"
    LEADING_COMPOSITE = "leading_composite"
    MARKETS = "markets"


class IndicatorSpec(BaseModel):
    """Definition of an indicator from the registry."""

    id: str = Field(..., pattern=r"^[a-z][a-z0-9_]*$")
    name: str
    category: Category
    source: Source
    frequency: Frequency
    code: str
    unit: str
    description: str = ""


class DataPoint(BaseModel):
    """A single normalized observation."""

    timestamp: datetime
    indicator_id: str
    value: float
    unit: str
    frequency: Frequency
    source: Source
    source_code: str


class IndicatorInfo(BaseModel):
    """Public-facing indicator metadata returned by the API."""

    id: str
    name: str
    category: str
    source: str
    frequency: str
    unit: str


class TimeseriesResponse(BaseModel):
    """API response for timeseries data."""

    indicator_id: str
    indicator_name: str
    unit: str
    data: list[dict]


class SnapshotEntry(BaseModel):
    """Latest reading for an indicator."""

    indicator_id: str
    indicator_name: str
    category: str
    latest_value: float | None
    latest_timestamp: str | None
    unit: str
