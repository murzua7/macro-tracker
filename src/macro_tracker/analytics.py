"""Analytical computations: percentiles, z-scores, rate-of-change, recession probability,
cross-correlation, regime detection, composite indices, diffusion index.

All functions operate on pandas DataFrames/Series retrieved from the database.
Expanding-window statistics are used throughout to avoid look-ahead bias.
"""

import numpy as np
import pandas as pd

from macro_tracker.db import Database


def get_indicator_series(db: Database, indicator_id: str) -> pd.Series:
    """Fetch a single indicator as a datetime-indexed Series."""
    rows = db.get_timeseries(indicator_id, limit=50000)
    if not rows:
        return pd.Series(dtype=float)
    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.set_index("timestamp").sort_index()
    return df["value"]


def get_all_series(db: Database, indicator_ids: list[str] | None = None) -> dict[str, pd.Series]:
    """Fetch multiple indicators as a dict of Series."""
    if indicator_ids is None:
        indicator_ids = db.get_indicator_ids()
    return {iid: get_indicator_series(db, iid) for iid in indicator_ids}


# ── Phase 2a: Historical Percentile Context ────────────────────────────

def expanding_percentile(s: pd.Series) -> pd.Series:
    """Compute expanding-window percentile rank (0-100) for each observation."""
    if s.empty:
        return pd.Series(dtype=float)
    return s.expanding(min_periods=1).apply(
        lambda w: (w.iloc[:-1] <= w.iloc[-1]).sum() / max(len(w) - 1, 1) * 100,
        raw=False,
    )


def current_percentile(s: pd.Series) -> float | None:
    """Get the percentile rank of the latest value against all history."""
    if s.empty or len(s) < 2:
        return None
    latest = s.iloc[-1]
    return float((s.iloc[:-1] <= latest).sum() / (len(s) - 1) * 100)


def percentile_color(pct: float | None) -> str:
    """Map percentile to traffic-light color."""
    if pct is None:
        return "gray"
    if pct <= 10 or pct >= 90:
        return "red"
    if pct <= 25 or pct >= 75:
        return "orange"
    return "green"


# ── Phase 2b: Rate of Change ───────────────────────────────────────────

def rate_of_change(s: pd.Series, periods: int = 1) -> pd.Series:
    """Compute percentage change over `periods` observations."""
    if s.empty:
        return pd.Series(dtype=float)
    return s.pct_change(periods=periods) * 100


def acceleration(s: pd.Series, periods: int = 1) -> pd.Series:
    """Second derivative: change in the rate of change."""
    roc = rate_of_change(s, periods)
    return roc.diff()


def trend_direction(s: pd.Series, lookback: int = 3) -> str:
    """Classify recent trend: 'improving', 'deteriorating', or 'stable'."""
    if len(s) < lookback + 1:
        return "stable"
    recent = s.iloc[-lookback:]
    slope = np.polyfit(range(len(recent)), recent.values, 1)[0]
    if abs(slope) < 1e-8:
        return "stable"
    return "improving" if slope > 0 else "deteriorating"


def trend_arrow(direction: str) -> str:
    return {"improving": "^", "deteriorating": "v", "stable": "-"}.get(direction, "-")


# ── Phase 2c: Z-Score Normalization ────────────────────────────────────

def expanding_zscore(s: pd.Series) -> pd.Series:
    """Compute expanding-window z-score for each observation."""
    if s.empty or len(s) < 2:
        return pd.Series(dtype=float, index=s.index)
    mean = s.expanding(min_periods=2).mean()
    std = s.expanding(min_periods=2).std()
    return (s - mean) / std.replace(0, np.nan)


def current_zscore(s: pd.Series) -> float | None:
    """Z-score of the latest observation against all history."""
    if s.empty or len(s) < 3:
        return None
    mean = s.iloc[:-1].mean()
    std = s.iloc[:-1].std()
    if std == 0 or np.isnan(std):
        return None
    return float((s.iloc[-1] - mean) / std)


def zscore_color(z: float | None) -> str:
    if z is None:
        return "gray"
    az = abs(z)
    if az > 2:
        return "red"
    if az > 1:
        return "orange"
    return "green"


# ── Phase 2d: Recession Probability ────────────────────────────────────

def yield_curve_recession_prob(spread_series: pd.Series, horizon_months: int = 12) -> pd.Series:
    """Estimate recession probability from the 10Y-3M spread (Estrella-Mishkin 1998)."""
    from scipy.stats import norm
    if spread_series.empty:
        return pd.Series(dtype=float)
    prob = spread_series.apply(lambda s: norm.cdf(-0.5333 - 0.6330 * s) * 100)
    return prob


def sahm_rule_status(sahm_value: float | None) -> dict:
    if sahm_value is None:
        return {"status": "unknown", "color": "gray", "label": "No Data"}
    if sahm_value >= 0.50:
        return {"status": "triggered", "color": "red", "label": f"TRIGGERED ({sahm_value:.2f})"}
    if sahm_value >= 0.30:
        return {"status": "warning", "color": "orange", "label": f"Warning ({sahm_value:.2f})"}
    return {"status": "normal", "color": "green", "label": f"Normal ({sahm_value:.2f})"}


def composite_recession_risk(
    yield_curve_prob: float | None,
    sahm_value: float | None,
    nfci_value: float | None,
    hy_oas_value: float | None,
) -> dict:
    components = {}
    scores = []

    if yield_curve_prob is not None:
        components["yield_curve"] = min(yield_curve_prob, 100)
        scores.append(components["yield_curve"])
    if sahm_value is not None:
        components["sahm_rule"] = min(sahm_value / 0.5 * 100, 100)
        scores.append(components["sahm_rule"])
    if nfci_value is not None:
        components["nfci"] = max(0, min((nfci_value + 0.5) / 1.5 * 100, 100))
        scores.append(components["nfci"])
    if hy_oas_value is not None:
        components["hy_spread"] = max(0, min((hy_oas_value - 3.0) / 4.0 * 100, 100))
        scores.append(components["hy_spread"])

    if not scores:
        return {"level": "unknown", "score": None, "components": components}

    avg_score = sum(scores) / len(scores)
    if avg_score >= 60:
        level = "high"
    elif avg_score >= 35:
        level = "elevated"
    elif avg_score >= 15:
        level = "moderate"
    else:
        level = "low"

    return {"level": level, "score": round(avg_score, 1), "components": components}


# ── Phase 2e: Cross-Correlation ────────────────────────────────────────

def cross_correlation(s1: pd.Series, s2: pd.Series, max_lag: int = 24) -> pd.DataFrame:
    """Compute cross-correlation between two series at lags -max_lag to +max_lag."""
    s1m = s1.resample("ME").last().dropna()
    s2m = s2.resample("ME").last().dropna()
    common = s1m.index.intersection(s2m.index)
    if len(common) < max_lag + 5:
        return pd.DataFrame(columns=["lag", "correlation"])
    s1m = s1m.loc[common]
    s2m = s2m.loc[common]

    results = []
    for lag in range(-max_lag, max_lag + 1):
        if lag >= 0:
            a = s1m.iloc[:len(s1m) - lag].values
            b = s2m.iloc[lag:].values[:len(a)]
        else:
            a = s1m.iloc[-lag:].values
            b = s2m.iloc[:len(s2m) + lag].values[:len(a)]
        if len(a) < 5:
            corr = None
        else:
            c = np.corrcoef(a, b)[0, 1]
            corr = float(c) if not np.isnan(c) else None
        results.append({"lag": lag, "correlation": corr})
    return pd.DataFrame(results)


def classify_lead_lag(ccf: pd.DataFrame) -> dict:
    if ccf.empty:
        return {"best_lag": 0, "best_corr": 0, "classification": "insufficient_data"}
    valid = ccf.dropna(subset=["correlation"])
    if valid.empty:
        return {"best_lag": 0, "best_corr": 0, "classification": "insufficient_data"}
    idx = valid["correlation"].abs().idxmax()
    best_lag = int(valid.loc[idx, "lag"])
    best_corr = float(valid.loc[idx, "correlation"])
    if abs(best_lag) <= 1:
        classification = "coincident"
    elif best_lag > 0:
        classification = f"s1 leads by {best_lag} months"
    else:
        classification = f"s2 leads by {-best_lag} months"
    return {"best_lag": best_lag, "best_corr": round(best_corr, 3), "classification": classification}


# ══════════════════════════════════════════════════════════════════════
# Phase 3b: Regime Detection
# ══════════════════════════════════════════════════════════════════════

def detect_regime(db: Database) -> dict:
    """Determine the current economic regime using multiple signals.

    Uses: recession_prob, sahm_rule, nfci, spread_10y_3m, lei.
    Returns: regime label, color, description, component signals.
    """
    signals = {}
    score = 0
    n_signals = 0

    # 1. Smoothed recession probability (Chauvet Markov-switching model)
    rec_prob = get_indicator_series(db, "recession_prob")
    if not rec_prob.empty:
        val = float(rec_prob.iloc[-1])
        signals["recession_prob"] = {"value": round(val, 1), "unit": "%"}
        if val > 50:
            score += 2
        elif val > 25:
            score += 1
        n_signals += 1

    # 2. Sahm Rule
    sahm = get_indicator_series(db, "sahm_rule")
    if not sahm.empty:
        val = float(sahm.iloc[-1])
        signals["sahm_rule"] = {"value": round(val, 2), "unit": "pp"}
        if val >= 0.50:
            score += 2
        elif val >= 0.30:
            score += 1
        n_signals += 1

    # 3. NFCI (positive = tighter than average)
    nfci = get_indicator_series(db, "nfci")
    if not nfci.empty:
        val = float(nfci.iloc[-1])
        signals["nfci"] = {"value": round(val, 3), "unit": "index"}
        if val > 0.5:
            score += 2
        elif val > 0:
            score += 1
        n_signals += 1

    # 4. Yield curve (10Y-3M)
    spread = get_indicator_series(db, "spread_10y_3m")
    if not spread.empty:
        val = float(spread.iloc[-1])
        signals["yield_curve"] = {"value": round(val, 2), "unit": "%"}
        if val < -0.5:
            score += 2  # deeply inverted
        elif val < 0:
            score += 1  # inverted
        n_signals += 1

    # 5. LEI trend (3-month change)
    lei = get_indicator_series(db, "lei")
    if not lei.empty and len(lei) >= 4:
        change_3m = float(lei.iloc[-1] - lei.iloc[-4])
        signals["lei_3m_change"] = {"value": round(change_3m, 2), "unit": "pts"}
        if change_3m < -1.5:
            score += 2
        elif change_3m < 0:
            score += 1
        n_signals += 1

    # 6. HY OAS
    hy = get_indicator_series(db, "hy_oas")
    if not hy.empty:
        val = float(hy.iloc[-1])
        signals["hy_oas"] = {"value": round(val, 2), "unit": "%"}
        if val > 6.0:
            score += 2
        elif val > 4.5:
            score += 1
        n_signals += 1

    # Determine regime
    if n_signals == 0:
        return {"regime": "UNKNOWN", "color": "#999999", "description": "Insufficient data", "signals": signals, "score": 0, "max_score": 0}

    max_score = n_signals * 2
    pct = score / max_score * 100

    if pct >= 60:
        regime = "CONTRACTION"
        color = "#dc3545"
        desc = "Multiple indicators signal economic contraction"
    elif pct >= 30:
        regime = "CAUTION"
        color = "#fd7e14"
        desc = "Mixed signals — elevated risk of slowdown"
    else:
        regime = "EXPANSION"
        color = "#28a745"
        desc = "Broad indicators consistent with economic expansion"

    return {
        "regime": regime,
        "color": color,
        "description": desc,
        "signals": signals,
        "score": score,
        "max_score": max_score,
    }


# ══════════════════════════════════════════════════════════════════════
# Phase 3a: Historical Heatmap (values at different time horizons)
# ══════════════════════════════════════════════════════════════════════

def historical_percentile_table(db: Database, indicator_ids: list[str] | None = None) -> list[dict]:
    """For each indicator, compute percentile at current, 1M, 3M, 6M, 1Y ago."""
    series_map = get_all_series(db, indicator_ids)
    rows = []
    for iid, s in series_map.items():
        if s.empty or len(s) < 10:
            continue
        s_monthly = s.resample("ME").last().dropna()
        if len(s_monthly) < 6:
            continue

        def _pct_at_offset(offset: int) -> float | None:
            if len(s_monthly) <= abs(offset):
                return None
            val = s_monthly.iloc[offset]
            history = s_monthly.iloc[:len(s_monthly) + offset] if offset < 0 else s_monthly
            if len(history) < 2:
                return None
            return float((history.iloc[:-1] <= val).sum() / (len(history) - 1) * 100)

        rows.append({
            "indicator_id": iid,
            "current": _pct_at_offset(-1),
            "1m_ago": _pct_at_offset(-2),
            "3m_ago": _pct_at_offset(-4),
            "6m_ago": _pct_at_offset(-7),
            "1y_ago": _pct_at_offset(-13) if len(s_monthly) > 13 else None,
        })
    return rows


# ══════════════════════════════════════════════════════════════════════
# Phase 3d: Rolling Correlation
# ══════════════════════════════════════════════════════════════════════

def rolling_correlation(s1: pd.Series, s2: pd.Series, window: int = 60) -> pd.DataFrame:
    """Compute rolling correlation between two daily/weekly series.

    Returns DataFrame with timestamp and rolling_corr columns.
    """
    s1d = s1.resample("B").last().ffill()
    s2d = s2.resample("B").last().ffill()
    common = s1d.index.intersection(s2d.index)
    if len(common) < window + 10:
        return pd.DataFrame(columns=["timestamp", "rolling_corr"])

    df = pd.DataFrame({"s1": s1d.loc[common], "s2": s2d.loc[common]})
    df["rolling_corr"] = df["s1"].rolling(window).corr(df["s2"])
    result = df[["rolling_corr"]].dropna().reset_index()
    result.columns = ["timestamp", "rolling_corr"]
    return result


# ══════════════════════════════════════════════════════════════════════
# Phase 4a: Composite Leading Index (OECD-style)
# ══════════════════════════════════════════════════════════════════════

# Indicators that are known to lead the business cycle
LEADING_INDICATOR_IDS = [
    "spread_10y_3m",    # yield curve
    "initial_claims",   # weekly unemployment claims (inverted)
    "building_permits", # housing permits
    "sp500",            # equity prices
    "consumer_sentiment",  # Michigan
    "ism_mfg_pmi",      # ISM new orders
    "avg_weekly_hours_mfg",  # manufacturing hours
]

# Indicators to invert (higher = worse for economy)
INVERT_INDICATORS = {"initial_claims"}


def compute_composite_leading_index(db: Database) -> pd.DataFrame:
    """Build a composite leading index from selected indicators.

    Methodology (OECD-style):
    1. Resample all to monthly
    2. Compute expanding-window z-scores (no look-ahead)
    3. Invert indicators where higher = worse
    4. Equal-weight average of z-scores
    5. Scale to mean=100, std=10 for interpretability

    Returns DataFrame with columns: timestamp, composite_index, n_components, component z-scores.
    """
    series = {}
    for iid in LEADING_INDICATOR_IDS:
        s = get_indicator_series(db, iid)
        if not s.empty and len(s) >= 12:
            s_monthly = s.resample("ME").last().dropna()
            if len(s_monthly) >= 6:
                series[iid] = s_monthly

    if len(series) < 2:
        return pd.DataFrame(columns=["timestamp", "composite_index", "n_components"])

    # Compute expanding z-scores
    zscores = {}
    for iid, s in series.items():
        z = expanding_zscore(s)
        if iid in INVERT_INDICATORS:
            z = -z
        zscores[iid] = z

    # Align on common dates and compute equal-weight average
    df_z = pd.DataFrame(zscores)
    df_z = df_z.dropna(how="all")

    composite = df_z.mean(axis=1)  # equal weight
    n_components = df_z.notna().sum(axis=1)

    # Scale to index=100, std=10
    c_mean = composite.expanding(min_periods=6).mean()
    c_std = composite.expanding(min_periods=6).std()
    scaled = 100 + (composite - c_mean) / c_std.replace(0, np.nan) * 10

    result = pd.DataFrame({
        "timestamp": scaled.index,
        "composite_index": scaled.values,
        "n_components": n_components.values,
    })

    # Add component z-scores
    for iid in zscores:
        vals = df_z[iid].reindex(scaled.index)
        result[f"z_{iid}"] = vals.values

    return result.dropna(subset=["composite_index"])


# ══════════════════════════════════════════════════════════════════════
# Phase 4b: Financial Stress Composite (OFR FSI methodology)
# ══════════════════════════════════════════════════════════════════════

STRESS_INDICATOR_IDS = [
    "hy_oas",       # HY credit spread
    "ig_oas",       # IG credit spread
    "baa_spread",   # BAA-10Y spread
    "vix",          # equity volatility
    "nfci",         # Chicago Fed NFCI
    "stlfsi",       # St. Louis FSI
    "spread_10y_3m",  # yield curve (inverted — flatter = more stress)
]

STRESS_INVERT = {"spread_10y_3m"}  # invert: lower spread = more stress


def compute_financial_stress_index(db: Database) -> pd.DataFrame:
    """Build a financial stress composite following OFR FSI principles.

    Methodology:
    1. Resample to weekly/monthly
    2. Rolling z-score normalization (expanding window, no look-ahead)
    3. Invert indicators where necessary
    4. First principal component extraction (or equal-weight if <3 components)
    5. Decompose by subcategory

    Returns DataFrame with timestamp, stress_index, and component contributions.
    """
    series = {}
    for iid in STRESS_INDICATOR_IDS:
        s = get_indicator_series(db, iid)
        if not s.empty and len(s) >= 12:
            s_monthly = s.resample("ME").last().dropna()
            if len(s_monthly) >= 6:
                series[iid] = s_monthly

    if len(series) < 2:
        return pd.DataFrame(columns=["timestamp", "stress_index", "n_components"])

    # Compute expanding z-scores
    zscores = {}
    for iid, s in series.items():
        z = expanding_zscore(s)
        if iid in STRESS_INVERT:
            z = -z
        zscores[iid] = z

    df_z = pd.DataFrame(zscores).dropna(how="all")

    # Try PCA if enough components, else equal-weight
    n_comp = df_z.shape[1]
    df_valid = df_z.dropna()

    if n_comp >= 3 and len(df_valid) >= 12:
        # PCA: extract first principal component
        from numpy.linalg import eigh
        data = df_valid.values
        cov = np.cov(data, rowvar=False)
        eigenvalues, eigenvectors = eigh(cov)
        # Last eigenvector = largest eigenvalue
        pc1_weights = eigenvectors[:, -1]
        # Ensure positive direction (stress should be positive when spreads are high)
        if pc1_weights.sum() < 0:
            pc1_weights = -pc1_weights
        # Normalize weights to sum to 1
        pc1_weights = pc1_weights / pc1_weights.sum()
        explained_var = float(eigenvalues[-1] / eigenvalues.sum() * 100)

        stress = df_z.fillna(0).values @ pc1_weights
        weight_map = {col: float(w) for col, w in zip(df_z.columns, pc1_weights)}
    else:
        stress = df_z.mean(axis=1).values
        weight_map = {col: 1.0 / n_comp for col in df_z.columns}
        explained_var = None

    n_components = df_z.notna().sum(axis=1)

    result = pd.DataFrame({
        "timestamp": df_z.index,
        "stress_index": stress,
        "n_components": n_components.values,
    })

    # Add component z-scores
    for iid in zscores:
        vals = df_z[iid].reindex(df_z.index) if iid in df_z.columns else pd.Series(dtype=float)
        result[f"z_{iid}"] = vals.values if len(vals) == len(result) else np.nan

    result = result.dropna(subset=["stress_index"])

    # Clean NaN for JSON
    result = result.where(result.notna(), None)

    return result, weight_map, explained_var


# ══════════════════════════════════════════════════════════════════════
# Phase 4c: Diffusion Index
# ══════════════════════════════════════════════════════════════════════

def compute_diffusion_index(db: Database, indicator_ids: list[str] | None = None) -> pd.DataFrame:
    """Compute the fraction of indicators improving month-over-month.

    Analogous to CFNAI Diffusion Index: when it drops below 0.5, the
    economy is deteriorating on a broad basis.

    Returns DataFrame with timestamp, diffusion (0-1), n_indicators, improving, deteriorating.
    """
    series_map = get_all_series(db, indicator_ids)

    # Resample all to monthly
    monthly = {}
    for iid, s in series_map.items():
        if s.empty or len(s) < 3:
            continue
        sm = s.resample("ME").last().dropna()
        if len(sm) >= 3:
            monthly[iid] = sm

    if len(monthly) < 5:
        return pd.DataFrame(columns=["timestamp", "diffusion", "n_indicators"])

    # Compute MoM changes
    changes = pd.DataFrame({iid: s.diff() for iid, s in monthly.items()})
    changes = changes.dropna(how="all")

    improving = (changes > 0).sum(axis=1)
    deteriorating = (changes < 0).sum(axis=1)
    total = changes.notna().sum(axis=1)
    diffusion = improving / total.replace(0, np.nan)

    result = pd.DataFrame({
        "timestamp": changes.index,
        "diffusion": diffusion.values,
        "n_indicators": total.values.astype(int),
        "improving": improving.values.astype(int),
        "deteriorating": deteriorating.values.astype(int),
    })

    return result.dropna(subset=["diffusion"])


# ══════════════════════════════════════════════════════════════════════
# Phase 3c: Sparkline Data (mini time series for category pages)
# ══════════════════════════════════════════════════════════════════════

def get_sparkline_data(db: Database, indicator_id: str, n_points: int = 30) -> list[float]:
    """Get the last n_points values for sparkline rendering."""
    rows = db.get_timeseries(indicator_id, limit=n_points)
    if not rows:
        return []
    return [r["value"] for r in rows]


# ── Summary: Compute All Analytics for Dashboard ───────────────────────

def compute_indicator_summary(db: Database, indicator_ids: list[str] | None = None) -> list[dict]:
    """Compute percentile, z-score, trend, and rate-of-change for all indicators."""
    series_map = get_all_series(db, indicator_ids)
    summaries = []

    for iid, s in series_map.items():
        if s.empty:
            continue

        pct = current_percentile(s)
        z = current_zscore(s)
        direction = trend_direction(s, lookback=3)
        roc_val = None
        if len(s) >= 2:
            roc = rate_of_change(s, periods=1)
            roc_val = float(roc.iloc[-1]) if not np.isnan(roc.iloc[-1]) else None

        accel_val = None
        if len(s) >= 3:
            acc = acceleration(s, periods=1)
            accel_val = float(acc.iloc[-1]) if not np.isnan(acc.iloc[-1]) else None

        summaries.append({
            "indicator_id": iid,
            "latest_value": float(s.iloc[-1]),
            "latest_date": s.index[-1].isoformat(),
            "percentile": round(pct, 1) if pct is not None else None,
            "percentile_color": percentile_color(pct),
            "zscore": round(z, 2) if z is not None else None,
            "zscore_color": zscore_color(z),
            "trend": direction,
            "trend_arrow": trend_arrow(direction),
            "roc_pct": round(roc_val, 2) if roc_val is not None else None,
            "acceleration": round(accel_val, 3) if accel_val is not None else None,
            "n_observations": len(s),
        })

    return summaries
