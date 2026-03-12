"""Tests for registry loading and validation."""

import tempfile
from pathlib import Path

import pytest
import yaml

from macro_tracker.registry import load_registry
from macro_tracker.schema import IndicatorSpec


def test_load_registry_from_project():
    """Test loading the actual registry.yaml from the project root."""
    registry_path = Path(__file__).resolve().parent.parent / "registry.yaml"
    indicators = load_registry(registry_path)

    assert len(indicators) >= 60, f"Expected at least 60 indicators, got {len(indicators)}"

    ids = [i.id for i in indicators]
    assert len(ids) == len(set(ids)), "Duplicate indicator IDs found"

    for ind in indicators:
        assert isinstance(ind, IndicatorSpec)
        assert ind.id
        assert ind.name
        assert ind.code
        assert ind.source
        assert ind.category


def test_load_registry_categories():
    """Ensure all 8 required categories are represented."""
    registry_path = Path(__file__).resolve().parent.parent / "registry.yaml"
    indicators = load_registry(registry_path)

    categories = {i.category.value for i in indicators}
    expected = {
        "macro_activity", "labor", "inflation_prices", "rates_curve",
        "credit_conditions", "housing", "leading_composite", "markets",
    }
    assert expected == categories, f"Missing categories: {expected - categories}"


def test_load_registry_sources():
    """Ensure fred and yfinance sources are represented."""
    registry_path = Path(__file__).resolve().parent.parent / "registry.yaml"
    indicators = load_registry(registry_path)

    sources = {i.source.value for i in indicators}
    assert "fred" in sources
    assert "yfinance" in sources


def test_load_registry_custom_file():
    """Test loading from a custom YAML file."""
    data = {
        "indicators": [
            {
                "id": "test_ind",
                "name": "Test Indicator",
                "category": "macro_activity",
                "source": "fred",
                "frequency": "monthly",
                "code": "TEST123",
                "unit": "percent",
            }
        ]
    }
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(data, f)
        f.flush()
        indicators = load_registry(f.name)

    assert len(indicators) == 1
    assert indicators[0].id == "test_ind"
    assert indicators[0].code == "TEST123"


def test_load_registry_invalid_missing_key():
    """Test that missing 'indicators' key raises ValueError."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump({"something_else": []}, f)
        f.flush()
        with pytest.raises(ValueError, match="missing 'indicators' key"):
            load_registry(f.name)


def test_load_registry_duplicate_ids():
    """Test that duplicate IDs raise ValueError."""
    data = {
        "indicators": [
            {
                "id": "dup",
                "name": "Dup 1",
                "category": "macro_activity",
                "source": "fred",
                "frequency": "monthly",
                "code": "D1",
                "unit": "percent",
            },
            {
                "id": "dup",
                "name": "Dup 2",
                "category": "macro_activity",
                "source": "fred",
                "frequency": "monthly",
                "code": "D2",
                "unit": "percent",
            },
        ]
    }
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(data, f)
        f.flush()
        with pytest.raises(ValueError, match="Duplicate indicator id"):
            load_registry(f.name)
