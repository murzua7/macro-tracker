"""FastAPI backend for the macrofinance tracker."""

import math

from fastapi import FastAPI, HTTPException, Query

from macro_tracker.analytics import (
    classify_lead_lag,
    composite_recession_risk,
    compute_composite_leading_index,
    compute_diffusion_index,
    compute_financial_stress_index,
    compute_indicator_summary,
    cross_correlation,
    detect_regime,
    get_indicator_series,
    get_sparkline_data,
    historical_percentile_table,
    rolling_correlation,
    sahm_rule_status,
    yield_curve_recession_prob,
)
from macro_tracker.db import Database
from macro_tracker.registry import load_registry
from macro_tracker.schema import IndicatorInfo, SnapshotEntry, TimeseriesResponse

app = FastAPI(
    title="US Macrofinance Tracker API",
    description="API for US macroeconomic and financial indicators with analytics",
    version="0.3.0",
)


def _get_db() -> Database:
    return Database()


def _get_registry_map() -> dict:
    indicators = load_registry()
    return {i.id: i for i in indicators}


def _clean_for_json(obj):
    """Recursively replace NaN/Inf with None for JSON serialization."""
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    if isinstance(obj, dict):
        return {k: _clean_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_clean_for_json(item) for item in obj]
    return obj


@app.get("/health")
def health_check():
    return {"status": "ok"}


@app.get("/indicators", response_model=list[IndicatorInfo])
def list_indicators(
    category: str | None = Query(None),
    source: str | None = Query(None),
):
    indicators = load_registry()
    if category:
        indicators = [i for i in indicators if i.category.value == category]
    if source:
        indicators = [i for i in indicators if i.source.value == source]
    return [
        IndicatorInfo(id=i.id, name=i.name, category=i.category.value,
                      source=i.source.value, frequency=i.frequency.value, unit=i.unit)
        for i in indicators
    ]


@app.get("/timeseries/{indicator_id}", response_model=TimeseriesResponse)
def get_timeseries(
    indicator_id: str,
    start: str | None = Query(None),
    end: str | None = Query(None),
    limit: int = Query(5000, ge=1, le=50000),
):
    registry = _get_registry_map()
    spec = registry.get(indicator_id)
    if not spec:
        raise HTTPException(status_code=404, detail=f"Indicator '{indicator_id}' not found")
    db = _get_db()
    data = db.get_timeseries(indicator_id, start=start, end=end, limit=limit)
    return TimeseriesResponse(
        indicator_id=indicator_id, indicator_name=spec.name,
        unit=spec.unit, data=data,
    )


@app.get("/snapshot", response_model=list[SnapshotEntry])
def get_snapshot(category: str | None = Query(None)):
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
        results.append(SnapshotEntry(
            indicator_id=ind_id, indicator_name=spec.name, category=spec.category.value,
            latest_value=row["value"], latest_timestamp=row["timestamp"], unit=spec.unit,
        ))
    return results


@app.get("/freshness")
def get_freshness():
    return _get_db().get_freshness()


# ── Analytics ──────────────────────────────────────────────────────────

@app.get("/analytics/summary")
def get_analytics_summary():
    return compute_indicator_summary(_get_db())


@app.get("/analytics/recession")
def get_recession_risk():
    db = _get_db()
    spread = get_indicator_series(db, "spread_10y_3m")
    yc_prob = float(yield_curve_recession_prob(spread).iloc[-1]) if not spread.empty else None
    sahm = get_indicator_series(db, "sahm_rule")
    sahm_val = float(sahm.iloc[-1]) if not sahm.empty else None
    nfci = get_indicator_series(db, "nfci")
    nfci_val = float(nfci.iloc[-1]) if not nfci.empty else None
    hy = get_indicator_series(db, "hy_oas")
    hy_val = float(hy.iloc[-1]) if not hy.empty else None

    return _clean_for_json({
        "yield_curve_prob": round(yc_prob, 1) if yc_prob is not None else None,
        "sahm_rule": sahm_rule_status(sahm_val),
        "nfci": round(nfci_val, 3) if nfci_val is not None else None,
        "hy_oas": round(hy_val, 2) if hy_val is not None else None,
        "composite": composite_recession_risk(yc_prob, sahm_val, nfci_val, hy_val),
    })


@app.get("/analytics/crosscorr")
def get_cross_correlation(
    ind1: str = Query(...), ind2: str = Query(...), max_lag: int = Query(24, ge=1, le=60),
):
    db = _get_db()
    s1 = get_indicator_series(db, ind1)
    s2 = get_indicator_series(db, ind2)
    if s1.empty or s2.empty:
        raise HTTPException(status_code=404, detail="One or both indicators have no data")
    ccf = cross_correlation(s1, s2, max_lag=max_lag)
    return _clean_for_json({
        "indicator_1": ind1, "indicator_2": ind2,
        "ccf": ccf.to_dict(orient="records"),
        "classification": classify_lead_lag(ccf),
    })


@app.get("/analytics/regime")
def get_regime():
    return _clean_for_json(detect_regime(_get_db()))


@app.get("/analytics/heatmap")
def get_historical_heatmap():
    return _clean_for_json(historical_percentile_table(_get_db()))


@app.get("/analytics/sparkline/{indicator_id}")
def get_sparkline(indicator_id: str, n: int = Query(30, ge=5, le=200)):
    return get_sparkline_data(_get_db(), indicator_id, n_points=n)


@app.get("/analytics/rolling_corr")
def get_rolling_corr(
    ind1: str = Query(...), ind2: str = Query(...), window: int = Query(60, ge=10, le=252),
):
    db = _get_db()
    s1 = get_indicator_series(db, ind1)
    s2 = get_indicator_series(db, ind2)
    if s1.empty or s2.empty:
        raise HTTPException(status_code=404, detail="One or both indicators have no data")
    df = rolling_correlation(s1, s2, window=window)
    records = df.to_dict(orient="records")
    for r in records:
        r["timestamp"] = r["timestamp"].isoformat()
    return _clean_for_json(records)


@app.get("/analytics/composite_leading")
def get_composite_leading():
    df = compute_composite_leading_index(_get_db())
    if df.empty:
        return {"data": [], "description": "Insufficient data for composite leading index"}
    records = df.to_dict(orient="records")
    for r in records:
        if hasattr(r["timestamp"], "isoformat"):
            r["timestamp"] = r["timestamp"].isoformat()
    return _clean_for_json({"data": records})


@app.get("/analytics/stress_index")
def get_stress_index():
    result = compute_financial_stress_index(_get_db())
    if isinstance(result, tuple):
        df, weights, explained_var = result
    else:
        return {"data": [], "weights": {}, "explained_variance": None}
    if df.empty:
        return {"data": [], "weights": weights, "explained_variance": explained_var}
    records = df.to_dict(orient="records")
    for r in records:
        if hasattr(r.get("timestamp"), "isoformat"):
            r["timestamp"] = r["timestamp"].isoformat()
    return _clean_for_json({
        "data": records,
        "weights": weights,
        "explained_variance": round(explained_var, 1) if explained_var else None,
    })


@app.get("/analytics/diffusion")
def get_diffusion():
    df = compute_diffusion_index(_get_db())
    if df.empty:
        return {"data": []}
    records = df.to_dict(orient="records")
    for r in records:
        if hasattr(r.get("timestamp"), "isoformat"):
            r["timestamp"] = r["timestamp"].isoformat()
    return _clean_for_json({"data": records})
