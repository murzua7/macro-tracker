"""Tests for FastAPI endpoints."""

from datetime import datetime

import pytest
from fastapi.testclient import TestClient

from macro_tracker.db import Database
from macro_tracker.schema import DataPoint, Frequency, Source


@pytest.fixture
def db_with_data(tmp_path):
    """Create a temporary database with some test data."""
    db_path = str(tmp_path / "test.db")
    db = Database(db_path=db_path)

    points = [
        DataPoint(
            timestamp=datetime(2024, 1, 1),
            indicator_id="cpi_yoy",
            value=3.1,
            unit="index",
            frequency=Frequency.MONTHLY,
            source=Source.FRED,
            source_code="CPIAUCSL",
        ),
        DataPoint(
            timestamp=datetime(2024, 2, 1),
            indicator_id="cpi_yoy",
            value=3.2,
            unit="index",
            frequency=Frequency.MONTHLY,
            source=Source.FRED,
            source_code="CPIAUCSL",
        ),
        DataPoint(
            timestamp=datetime(2024, 1, 2),
            indicator_id="sp500",
            value=470.5,
            unit="usd",
            frequency=Frequency.DAILY,
            source=Source.YFINANCE,
            source_code="SPY",
        ),
    ]
    db.upsert_datapoints(points)
    return db


@pytest.fixture
def client(db_with_data, monkeypatch):
    """Create a test client with mocked database."""
    from macro_tracker.api import main as api_module

    monkeypatch.setattr(api_module, "_get_db", lambda: db_with_data)

    client = TestClient(api_module.app)
    return client


def test_health(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_list_indicators(client):
    response = client.get("/indicators")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) >= 30
    # Check structure
    first = data[0]
    assert "id" in first
    assert "name" in first
    assert "category" in first
    assert "source" in first


def test_list_indicators_filter_category(client):
    response = client.get("/indicators", params={"category": "macro_labor"})
    assert response.status_code == 200
    data = response.json()
    assert all(i["category"] == "macro_labor" for i in data)


def test_list_indicators_filter_source(client):
    response = client.get("/indicators", params={"source": "fred"})
    assert response.status_code == 200
    data = response.json()
    assert all(i["source"] == "fred" for i in data)


def test_get_timeseries(client):
    response = client.get("/timeseries/cpi_yoy")
    assert response.status_code == 200
    data = response.json()
    assert data["indicator_id"] == "cpi_yoy"
    assert len(data["data"]) == 2


def test_get_timeseries_not_found(client):
    response = client.get("/timeseries/nonexistent")
    assert response.status_code == 404


def test_get_snapshot(client):
    response = client.get("/snapshot")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    # Should have entries for indicators that have data
    ids = {e["indicator_id"] for e in data}
    assert "cpi_yoy" in ids
    assert "sp500" in ids


def test_get_freshness(client):
    response = client.get("/freshness")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) >= 2
