"""Transformation helpers for normalizing macroeconomic time series."""

from __future__ import annotations

from typing import Iterable

import pandas as pd

from .config import IndicatorDefinition


def _periods_for_change(frequency: str, metric: str) -> int:
    """Return the correct lag count for a given change metric."""
    if frequency == "monthly":
        return 1 if metric == "mom" else 12
    if frequency == "quarterly":
        return 1 if metric == "qoq" else 4
    if frequency == "daily":
        return 30 if metric == "mom" else 365
    raise ValueError(f"Unsupported frequency for change calculation: {frequency}")


def build_daily_macro_frame(
    raw_series: dict[str, pd.Series],
    definitions: Iterable[IndicatorDefinition],
) -> pd.DataFrame:
    """Align raw series to a daily index and compute derivative metrics."""
    cleaned = {
        key: series.dropna().sort_index()
        for key, series in raw_series.items()
        if not series.empty
    }
    if not cleaned:
        return pd.DataFrame()

    min_date = min(series.index.min() for series in cleaned.values())
    max_date = max(series.index.max() for series in cleaned.values())
    daily_index = pd.date_range(min_date, max_date, freq="D", name="date")

    daily_frame = pd.DataFrame(index=daily_index)
    definition_map = {definition.key: definition for definition in definitions}

    for key, series in cleaned.items():
        definition = definition_map[key]
        aligned = series.reindex(daily_index)
        if definition.frequency in {"monthly", "quarterly"}:
            aligned = aligned.ffill()
        daily_frame[key] = aligned

        if definition.compute_mom:
            raw_change = series.pct_change(
                periods=_periods_for_change(definition.frequency, "mom")
            ) * 100.0
            daily_frame[f"{key}_mom_pct"] = raw_change.reindex(daily_index).ffill()
        if definition.compute_yoy:
            raw_change = series.pct_change(
                periods=_periods_for_change(definition.frequency, "yoy")
            ) * 100.0
            daily_frame[f"{key}_yoy_pct"] = raw_change.reindex(daily_index).ffill()

    return daily_frame.reset_index()
