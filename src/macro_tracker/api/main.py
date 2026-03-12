"""FastAPI backend for the macrofinance tracker."""

from fastapi import FastAPI, HTTPException, Query

from macro_tracker.db import Database
from macro_tracker.registry import load_registry
from macro_tracker.schema import IndicatorInfo, SnapshotEntry, TimeseriesResponse

app = FastAPI(
    title="US Macrofinance Tracker API",
    description="API for US macroeconomic and financial indicators",
    version="0.1.0",
)


def _get_db() -> Database:
    return Database()


def _get_registry_map() -> dict:
    indicators = load_registry()
    return {i.id: i for i in indicators}


@app.get("/health")
def health_check():
    return {"status": "ok"}


@app.get("/indicators", response_model=list[IndicatorInfo])
def list_indicators(
    category: str | None = Query(None, description="Filter by category"),
    source: str | None = Query(None, description="Filter by source"),
):
    """List all registered indicators with optional filtering."""
    indicators = load_registry()

    if category:
        indicators = [i for i in indicators if i.category.value == category]
    if source:
        indicators = [i for i in indicators if i.source.value == source]

    return [
        IndicatorInfo(
            id=i.id,
            name=i.name,
            category=i.category.value,
            source=i.source.value,
            frequency=i.frequency.value,
            unit=i.unit,
        )
        for i in indicators
    ]


@app.get("/timeseries/{indicator_id}", response_model=TimeseriesResponse)
def get_timeseries(
    indicator_id: str,
    start: str | None = Query(None, description="Start date (YYYY-MM-DD)"),
    end: str | None = Query(None, description="End date (YYYY-MM-DD)"),
    limit: int = Query(5000, ge=1, le=50000),
):
    """Fetch timeseries data for a specific indicator."""
    registry = _get_registry_map()
    spec = registry.get(indicator_id)
    if not spec:
        raise HTTPException(status_code=404, detail=f"Indicator '{indicator_id}' not found")

    db = _get_db()
    data = db.get_timeseries(indicator_id, start=start, end=end, limit=limit)

    return TimeseriesResponse(
        indicator_id=indicator_id,
        indicator_name=spec.name,
        unit=spec.unit,
        data=data,
    )


@app.get("/snapshot", response_model=list[SnapshotEntry])
def get_snapshot(
    category: str | None = Query(None, description="Filter by category"),
):
    """Get latest reading for all indicators."""
    db = _get_db()
    registry = _get_registry_map()
    latest_rows = db.get_all_latest()

    results = []
    for row in latest_rows:
        ind_id = row["indicator_id"]
        spec = registry.get(ind_id)
        if not spec:
            continue
        if category and spec.category.value != category:
            continue

        results.append(
            SnapshotEntry(
                indicator_id=ind_id,
                indicator_name=spec.name,
                category=spec.category.value,
                latest_value=row["value"],
                latest_timestamp=row["timestamp"],
                unit=spec.unit,
            )
        )

    return results


@app.get("/freshness")
def get_freshness():
    """Get data freshness info per indicator."""
    db = _get_db()
    return db.get_freshness()
