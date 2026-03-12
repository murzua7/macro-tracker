"""Tests for schema validation."""

from datetime import datetime

import pytest
from pydantic import ValidationError

from macro_tracker.schema import (
    Category,
    DataPoint,
    Frequency,
    IndicatorSpec,
    Source,
)


def test_indicator_spec_valid():
    spec = IndicatorSpec(
        id="test_indicator",
        name="Test Indicator",
        category=Category.MACRO_LABOR,
        source=Source.FRED,
        frequency=Frequency.MONTHLY,
        code="TEST123",
        unit="percent",
    )
    assert spec.id == "test_indicator"
    assert spec.source == Source.FRED


def test_indicator_spec_invalid_id():
    with pytest.raises(ValidationError):
        IndicatorSpec(
            id="Invalid-ID!",
            name="Bad",
            category="macro_labor",
            source="fred",
            frequency="monthly",
            code="X",
            unit="percent",
        )


def test_indicator_spec_invalid_source():
    with pytest.raises(ValidationError):
        IndicatorSpec(
            id="test",
            name="Test",
            category="macro_labor",
            source="invalid_source",
            frequency="monthly",
            code="X",
            unit="percent",
        )


def test_datapoint_valid():
    dp = DataPoint(
        timestamp=datetime(2024, 1, 1),
        indicator_id="cpi_yoy",
        value=3.5,
        unit="index",
        frequency=Frequency.MONTHLY,
        source=Source.FRED,
        source_code="CPIAUCSL",
    )
    assert dp.value == 3.5
    assert dp.source == Source.FRED


def test_datapoint_requires_all_fields():
    with pytest.raises(ValidationError):
        DataPoint(
            timestamp=datetime(2024, 1, 1),
            indicator_id="test",
            # missing value and other fields
        )


def test_all_categories():
    assert len(Category) == 5
    values = {c.value for c in Category}
    assert "macro_labor" in values
    assert "derivatives" in values


def test_all_frequencies():
    assert len(Frequency) == 4
    values = {f.value for f in Frequency}
    assert values == {"daily", "weekly", "monthly", "quarterly"}
