"""Streamlit dashboard — US Macrofinance Tracker v0.4 (direct DB, deployable)."""

import os
import subprocess
import sys
from pathlib import Path

# Ensure src/ is on Python path (needed for Streamlit Cloud where package isn't installed)
_src_dir = str(Path(__file__).resolve().parent.parent.parent)
if _src_dir not in sys.path:
    sys.path.insert(0, _src_dir)

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

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
from macro_tracker.schema import IndicatorInfo

st.set_page_config(page_title="US Macrofinance Tracker", layout="wide")

CATEGORIES = {
    "All": None,
    "Macro Activity": "macro_activity",
    "Labor": "labor",
    "Inflation & Prices": "inflation_prices",
    "Rates & Yield Curve": "rates_curve",
    "Credit Conditions": "credit_conditions",
    "Housing": "housing",
    "Leading & Composite": "leading_composite",
    "Markets": "markets",
}
CATEGORY_LABELS = {v: k for k, v in CATEGORIES.items() if v is not None}


# ── Data layer (direct Python, no API needed) ─────────────────────────

def _clean(obj):
    """Recursively replace NaN/Inf with None."""
    import math
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    if isinstance(obj, dict):
        return {k: _clean(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_clean(item) for item in obj]
    return obj


@st.cache_resource
def get_db():
    return Database()


@st.cache_data(ttl=300)
def get_registry():
    indicators = load_registry()
    return [
        {"id": i.id, "name": i.name, "category": i.category.value,
         "source": i.source.value, "frequency": i.frequency.value, "unit": i.unit,
         "description": i.description}
        for i in indicators
    ]


@st.cache_data(ttl=300)
def get_description_map():
    indicators = load_registry()
    return {i.id: i.description for i in indicators if i.description}


@st.cache_data(ttl=300)
def get_registry_map():
    indicators = load_registry()
    return {i.id: i for i in indicators}


@st.cache_data(ttl=300)
def fetch_indicators(category=None):
    indicators = get_registry()
    if category:
        indicators = [i for i in indicators if i["category"] == category]
    return indicators


@st.cache_data(ttl=300)
def fetch_snapshot(category=None):
    db = get_db()
    registry = get_registry_map()
    latest_rows = db.get_all_latest()
    results = []
    for row in latest_rows:
        ind_id = row["indicator_id"]
        spec = registry.get(ind_id)
        if not spec:
            continue
        if category and spec.category.value != category:
            continue
        results.append({
            "indicator_id": ind_id, "indicator_name": spec.name,
            "category": spec.category.value, "latest_value": row["value"],
            "latest_timestamp": row["timestamp"], "unit": spec.unit,
        })
    return results


@st.cache_data(ttl=300)
def fetch_timeseries(indicator_id, start=None, end=None):
    db = get_db()
    registry = get_registry_map()
    spec = registry.get(indicator_id)
    if not spec:
        return None
    data = db.get_timeseries(indicator_id, start=start, end=end)
    return {"indicator_id": indicator_id, "indicator_name": spec.name,
            "unit": spec.unit, "data": data}


@st.cache_data(ttl=300)
def fetch_summary():
    return _clean(compute_indicator_summary(get_db()))


@st.cache_data(ttl=300)
def fetch_recession():
    db = get_db()
    spread = get_indicator_series(db, "spread_10y_3m")
    yc_prob = float(yield_curve_recession_prob(spread).iloc[-1]) if not spread.empty else None
    sahm = get_indicator_series(db, "sahm_rule")
    sahm_val = float(sahm.iloc[-1]) if not sahm.empty else None
    nfci = get_indicator_series(db, "nfci")
    nfci_val = float(nfci.iloc[-1]) if not nfci.empty else None
    hy = get_indicator_series(db, "hy_oas")
    hy_val = float(hy.iloc[-1]) if not hy.empty else None
    return _clean({
        "yield_curve_prob": round(yc_prob, 1) if yc_prob is not None else None,
        "sahm_rule": sahm_rule_status(sahm_val),
        "nfci": round(nfci_val, 3) if nfci_val is not None else None,
        "hy_oas": round(hy_val, 2) if hy_val is not None else None,
        "composite": composite_recession_risk(yc_prob, sahm_val, nfci_val, hy_val),
    })


@st.cache_data(ttl=300)
def fetch_regime():
    return _clean(detect_regime(get_db()))


@st.cache_data(ttl=300)
def fetch_heatmap():
    return _clean(historical_percentile_table(get_db()))


@st.cache_data(ttl=300)
def fetch_composite_leading():
    df = compute_composite_leading_index(get_db())
    if df.empty:
        return {"data": [], "description": "Insufficient data"}
    records = df.to_dict(orient="records")
    for r in records:
        if hasattr(r["timestamp"], "isoformat"):
            r["timestamp"] = r["timestamp"].isoformat()
    return _clean({"data": records})


@st.cache_data(ttl=300)
def fetch_stress_index():
    result = compute_financial_stress_index(get_db())
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
    return _clean({
        "data": records, "weights": weights,
        "explained_variance": round(explained_var, 1) if explained_var else None,
    })


@st.cache_data(ttl=300)
def fetch_diffusion():
    df = compute_diffusion_index(get_db())
    if df.empty:
        return {"data": []}
    records = df.to_dict(orient="records")
    for r in records:
        if hasattr(r.get("timestamp"), "isoformat"):
            r["timestamp"] = r["timestamp"].isoformat()
    return _clean({"data": records})


@st.cache_data(ttl=300)
def fetch_freshness():
    return get_db().get_freshness()


@st.cache_data(ttl=300)
def fetch_crosscorr(ind1, ind2, max_lag):
    db = get_db()
    s1 = get_indicator_series(db, ind1)
    s2 = get_indicator_series(db, ind2)
    if s1.empty or s2.empty:
        return None
    ccf = cross_correlation(s1, s2, max_lag=max_lag)
    return _clean({
        "indicator_1": ind1, "indicator_2": ind2,
        "ccf": ccf.to_dict(orient="records"),
        "classification": classify_lead_lag(ccf),
    })


@st.cache_data(ttl=300)
def fetch_rolling_corr(ind1, ind2, window):
    db = get_db()
    s1 = get_indicator_series(db, ind1)
    s2 = get_indicator_series(db, ind2)
    if s1.empty or s2.empty:
        return None
    df = rolling_correlation(s1, s2, window=window)
    records = df.to_dict(orient="records")
    for r in records:
        r["timestamp"] = r["timestamp"].isoformat()
    return _clean(records)


@st.cache_data(ttl=300)
def fetch_sparkline(indicator_id, n=60):
    return get_sparkline_data(get_db(), indicator_id, n_points=n)


# ── Sidebar ─────────────────────────────────────────────────────────────

st.sidebar.header("Settings")
st.sidebar.subheader("FRED API Key")
fred_key = st.sidebar.text_input(
    "API Key", type="password", placeholder="32-char FRED API key",
    help="Get one at https://fred.stlouisfed.org/docs/api/api_key.html",
)

if st.sidebar.button("Ingest Data", type="primary"):
    with st.sidebar.status("Running ingestion...", expanded=True) as status:
        env_path = Path(__file__).resolve().parent.parent.parent.parent / ".env"
        env_lines = env_path.read_text().splitlines() if env_path.exists() else []
        if fred_key:
            new_lines, found = [], False
            for line in env_lines:
                if line.startswith("FRED_API_KEY="):
                    new_lines.append(f"FRED_API_KEY={fred_key}")
                    found = True
                else:
                    new_lines.append(line)
            if not found:
                new_lines.append(f"FRED_API_KEY={fred_key}")
            env_path.write_text("\n".join(new_lines) + "\n")

        scripts_dir = Path(__file__).resolve().parent.parent.parent.parent / "scripts"
        src_dir = Path(__file__).resolve().parent.parent.parent
        env = {**os.environ, "PYTHONPATH": str(src_dir)}
        if fred_key:
            env["FRED_API_KEY"] = fred_key
        result = subprocess.run(
            [sys.executable, str(scripts_dir / "ingest.py")],
            capture_output=True, text=True, timeout=300, env=env,
        )
        if result.returncode == 0:
            st.write(result.stdout.strip().split("\n")[-1])
            status.update(label="Ingestion complete!", state="complete")
            st.cache_data.clear()
        else:
            st.error("\n".join(result.stderr.strip().split("\n")[-5:]))
            status.update(label="Ingestion failed", state="error")

st.sidebar.divider()
st.sidebar.header("Filters")
selected_cat_label = st.sidebar.selectbox("Category", list(CATEGORIES.keys()))
selected_category = CATEGORIES[selected_cat_label]


# ═══════════════════════════════════════════════════════════════════════
# REGIME BANNER
# ═══════════════════════════════════════════════════════════════════════

regime_data = fetch_regime()

st.title("US Macrofinance Tracker")

if regime_data and regime_data.get("regime") != "UNKNOWN":
    regime = regime_data["regime"]
    color = regime_data["color"]
    desc = regime_data["description"]
    score = regime_data.get("score", 0)
    max_score = regime_data.get("max_score", 1)

    st.markdown(
        f'<div style="background-color:{color};color:white;padding:12px 20px;'
        f'border-radius:8px;margin-bottom:16px;font-size:18px;">'
        f'<strong>Current Regime: {regime}</strong> &nbsp;&mdash;&nbsp; {desc}'
        f' &nbsp;({score}/{max_score} stress signals)'
        f'</div>',
        unsafe_allow_html=True,
    )


# ═══════════════════════════════════════════════════════════════════════
# TABS
# ═══════════════════════════════════════════════════════════════════════

tabs = st.tabs([
    "Overview", "Recession Risk", "Heatmap", "Composites",
    "Charts", "Compare", "Cross-Correlation", "Freshness",
])


# ── Tab 1: Overview ─────────────────────────────────────────────────────

with tabs[0]:
    st.header("Latest Readings")

    snapshot = fetch_snapshot(category=selected_category)
    summary_data = fetch_summary() or []
    summary_map = {s["indicator_id"]: s for s in summary_data}
    desc_map = get_description_map()

    if snapshot:
        cats_seen = {}
        for entry in snapshot:
            cats_seen.setdefault(entry["category"], []).append(entry)

        for cat, entries in cats_seen.items():
            cat_label = CATEGORY_LABELS.get(cat, cat)
            st.subheader(cat_label)
            cols = st.columns(min(len(entries), 4))
            for i, entry in enumerate(entries):
                col = cols[i % len(cols)]
                val = entry["latest_value"]
                display_val = f"{val:,.2f}" if val is not None else "N/A"
                analytics = summary_map.get(entry["indicator_id"], {})
                pct = analytics.get("percentile")
                arrow = analytics.get("trend_arrow", "")
                roc = analytics.get("roc_pct")
                delta_str = f"{roc:+.1f}%" if roc is not None else None

                ind_desc = desc_map.get(entry["indicator_id"], "")
                help_text = f"{ind_desc}\n\n" if ind_desc else ""
                help_text += f"{entry['indicator_id']} | {entry['unit']} | {entry['latest_timestamp'] or 'N/A'}"
                if pct is not None:
                    help_text += f" | Percentile: {pct:.0f}th"

                col.metric(
                    label=f"{entry['indicator_name']} {arrow}",
                    value=display_val, delta=delta_str, help=help_text,
                )
    else:
        st.info("No data. Enter your FRED API key in the sidebar and click **Ingest Data**.")


# ── Tab 2: Recession Risk ───────────────────────────────────────────────

with tabs[1]:
    st.header("Recession Risk Assessment")
    recession_data = fetch_recession()

    if recession_data:
        composite = recession_data.get("composite", {})
        score = composite.get("score")
        level = composite.get("level", "unknown")
        level_labels = {"low": "LOW", "moderate": "MODERATE", "elevated": "ELEVATED", "high": "HIGH"}

        col1, col2, col3 = st.columns([2, 1, 1])
        with col1:
            st.markdown(f"### Composite Risk: **{level_labels.get(level, 'N/A')}**")
            if score is not None:
                st.progress(min(score / 100, 1.0))
                st.caption(f"Score: {score}/100")
        with col2:
            yc_prob = recession_data.get("yield_curve_prob")
            st.metric("Yield Curve Prob.", f"{yc_prob:.1f}%" if yc_prob else "N/A",
                       help="12-month recession probability derived from the 10Y-3M Treasury spread using the Estrella-Mishkin probit model. Above 30% = elevated; above 50% = high risk.")
        with col3:
            sahm = recession_data.get("sahm_rule", {})
            st.metric("Sahm Rule", sahm.get("label", "N/A"))

        st.divider()
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Yield Curve", f"{yc_prob:.1f}%" if yc_prob else "N/A")
        c2.metric("Sahm Rule", sahm.get("label", "N/A"))
        nfci_val = recession_data.get("nfci")
        c3.metric("NFCI", f"{nfci_val:.3f}" if nfci_val is not None else "N/A",
                   help="Chicago Fed National Financial Conditions Index. Weighted average of 105 indicators. Positive = tighter than average (restrictive); negative = looser (accommodative).")
        hy_val = recession_data.get("hy_oas")
        c4.metric("HY OAS", f"{hy_val:.2f}%" if hy_val is not None else "N/A",
                   help="ICE BofA High Yield Option-Adjusted Spread. Measures credit risk premium on junk bonds. Above 500bps = distress; above 800bps = crisis conditions.")

        # Yield curve recession probability time series
        ts = fetch_timeseries("spread_10y_3m")
        if ts and ts.get("data"):
            from scipy.stats import norm
            df = pd.DataFrame(ts["data"])
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            df["recession_prob"] = df["value"].apply(lambda s: norm.cdf(-0.5333 - 0.6330 * s) * 100)
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=df["timestamp"], y=df["recession_prob"],
                                      mode="lines", name="Recession Prob.", line=dict(color="crimson")))
            fig.add_hline(y=30, line_dash="dash", line_color="orange", annotation_text="30%")
            fig.add_hline(y=50, line_dash="dash", line_color="red", annotation_text="50%")
            fig.update_layout(title="Yield Curve Recession Probability (12-month horizon)",
                              yaxis_title="%", yaxis=dict(range=[0, 100]))
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Ingest FRED data first.")


# ── Tab 3: Heatmap ──────────────────────────────────────────────────────

with tabs[2]:
    st.header("Indicator Heatmap")

    view_mode = st.radio("View", ["Current Analytics", "Historical Percentiles"], horizontal=True)

    if view_mode == "Current Analytics":
        summary_data = fetch_summary() or []
        if summary_data:
            registry_items = fetch_indicators()
            registry_map = {ind["id"]: ind for ind in registry_items}
            rows = []
            for s in summary_data:
                info = registry_map.get(s["indicator_id"], {})
                cat = CATEGORY_LABELS.get(info.get("category", ""), info.get("category", ""))
                if selected_category and info.get("category") != selected_category:
                    continue
                rows.append({
                    "Indicator": info.get("name", s["indicator_id"]),
                    "Category": cat,
                    "Value": s["latest_value"],
                    "Pctile": s["percentile"],
                    "Z": s["zscore"],
                    "Trend": s["trend_arrow"],
                    "RoC%": s["roc_pct"],
                    "Accel": s["acceleration"],
                })
            df_heat = pd.DataFrame(rows)
            desc_map_heat = get_description_map()
            if not df_heat.empty:
                def _color_pct(val):
                    if val is None or pd.isna(val):
                        return "background-color: #ddd"
                    if val <= 10 or val >= 90:
                        return "background-color: #ff6b6b; color: white"
                    if val <= 25 or val >= 75:
                        return "background-color: #ffa94d"
                    return "background-color: #51cf66"

                def _color_z(val):
                    if val is None or pd.isna(val):
                        return "background-color: #ddd"
                    if abs(val) > 2:
                        return "background-color: #ff6b6b; color: white"
                    if abs(val) > 1:
                        return "background-color: #ffa94d"
                    return "background-color: #51cf66"

                styled = (df_heat.style
                          .applymap(_color_pct, subset=["Pctile"])
                          .applymap(_color_z, subset=["Z"])
                          .format({"Value": "{:,.2f}", "Pctile": "{:.0f}", "Z": "{:.2f}",
                                   "RoC%": "{:+.1f}", "Accel": "{:+.3f}"}, na_rep="--"))
                st.dataframe(styled, use_container_width=True, height=600)

                with st.expander("Indicator Guide"):
                    for s in summary_data:
                        ind_id = s["indicator_id"]
                        desc = desc_map_heat.get(ind_id, "")
                        info = registry_map.get(ind_id, {})
                        if desc and (not selected_category or info.get("category") == selected_category):
                            st.markdown(f"**{info.get('name', ind_id)}** — {desc}")

                # Momentum bar chart
                st.subheader("Momentum (Rate of Change)")
                df_roc = df_heat[df_heat["RoC%"].notna()].sort_values("RoC%", ascending=False)
                if not df_roc.empty:
                    fig = px.bar(df_roc, x="Indicator", y="RoC%", color="RoC%",
                                 color_continuous_scale=["red", "gray", "green"],
                                 color_continuous_midpoint=0)
                    fig.update_layout(xaxis_tickangle=-45, height=400)
                    st.plotly_chart(fig, use_container_width=True)

    else:  # Historical Percentiles
        heatmap_data = fetch_heatmap() or []
        if heatmap_data:
            registry_items = fetch_indicators()
            registry_map = {ind["id"]: ind for ind in registry_items}
            rows = []
            for h in heatmap_data:
                info = registry_map.get(h["indicator_id"], {})
                if selected_category and info.get("category") != selected_category:
                    continue
                rows.append({
                    "Indicator": info.get("name", h["indicator_id"]),
                    "Category": CATEGORY_LABELS.get(info.get("category", ""), ""),
                    "Current": h.get("current"),
                    "1M Ago": h.get("1m_ago"),
                    "3M Ago": h.get("3m_ago"),
                    "6M Ago": h.get("6m_ago"),
                    "1Y Ago": h.get("1y_ago"),
                })
            df_hist = pd.DataFrame(rows)
            if not df_hist.empty:
                pct_cols = ["Current", "1M Ago", "3M Ago", "6M Ago", "1Y Ago"]

                def _color_pct(val):
                    if val is None or pd.isna(val):
                        return "background-color: #ddd"
                    if val <= 10 or val >= 90:
                        return "background-color: #ff6b6b; color: white"
                    if val <= 25 or val >= 75:
                        return "background-color: #ffa94d"
                    return "background-color: #51cf66"

                styled = (df_hist.style
                          .applymap(_color_pct, subset=pct_cols)
                          .format({c: "{:.0f}" for c in pct_cols}, na_rep="--"))
                st.dataframe(styled, use_container_width=True, height=600)
                st.caption("Values are percentile ranks (0-100). Green = normal, Orange = notable, Red = extreme.")

                desc_map_hist = get_description_map()
                with st.expander("Indicator Guide"):
                    for h in heatmap_data:
                        ind_id = h["indicator_id"]
                        desc = desc_map_hist.get(ind_id, "")
                        info = registry_map.get(ind_id, {})
                        if desc and (not selected_category or info.get("category") == selected_category):
                            st.markdown(f"**{info.get('name', ind_id)}** — {desc}")
        else:
            st.info("No data for historical heatmap.")


# ── Tab 4: Composite Indices ─────────────────────────────────────────────

with tabs[3]:
    st.header("Composite Indices")
    sub_tab = st.radio("Index", ["Leading Index", "Financial Stress", "Diffusion"], horizontal=True)

    if sub_tab == "Leading Index":
        st.subheader("Composite Leading Economic Index (OECD-style)")
        st.caption("Equal-weight z-score average of: yield curve, initial claims (inv.), "
                   "building permits, S&P 500, consumer sentiment, ISM new orders, mfg hours.")

        data = fetch_composite_leading()
        if data and data.get("data"):
            df = pd.DataFrame(data["data"])
            df["timestamp"] = pd.to_datetime(df["timestamp"])

            fig = go.Figure()
            fig.add_trace(go.Scatter(x=df["timestamp"], y=df["composite_index"],
                                      mode="lines", name="Composite Leading Index",
                                      line=dict(color="royalblue", width=2)))
            fig.add_hline(y=100, line_dash="dash", line_color="gray", annotation_text="Neutral (100)")
            fig.update_layout(title="Composite Leading Index (mean=100, std=10)",
                              yaxis_title="Index", height=450)
            st.plotly_chart(fig, use_container_width=True)

            # Component decomposition
            z_cols = [c for c in df.columns if c.startswith("z_")]
            if z_cols:
                st.subheader("Component Z-Scores")
                df_components = df[["timestamp"] + z_cols].melt(
                    id_vars="timestamp", var_name="component", value_name="zscore")
                df_components["component"] = df_components["component"].str.replace("z_", "")
                fig2 = px.line(df_components, x="timestamp", y="zscore", color="component",
                               title="Individual Component Z-Scores")
                fig2.add_hline(y=0, line_dash="dash", line_color="gray")
                st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("Insufficient data. Need at least 2 leading indicators with 12+ months of data.")

    elif sub_tab == "Financial Stress":
        st.subheader("Financial Stress Index (OFR-style)")
        st.caption("PCA-based composite of: HY OAS, IG OAS, BAA spread, VIX, NFCI, StL FSI, yield curve (inv.)")

        data = fetch_stress_index()
        if data and data.get("data"):
            df = pd.DataFrame(data["data"])
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            weights = data.get("weights", {})
            explained = data.get("explained_variance")

            fig = go.Figure()
            fig.add_trace(go.Scatter(x=df["timestamp"], y=df["stress_index"],
                                      mode="lines", name="Stress Index",
                                      line=dict(color="crimson", width=2),
                                      fill="tozeroy", fillcolor="rgba(220,53,69,0.15)"))
            fig.add_hline(y=0, line_dash="dash", line_color="gray", annotation_text="Average")
            fig.add_hline(y=1, line_dash="dot", line_color="orange", annotation_text="+1 SD")
            fig.add_hline(y=2, line_dash="dot", line_color="red", annotation_text="+2 SD")
            fig.update_layout(title="Financial Stress Index", yaxis_title="Stress (z-score units)", height=450)
            st.plotly_chart(fig, use_container_width=True)

            if weights:
                w1, w2 = st.columns(2)
                with w1:
                    st.subheader("PCA Weights")
                    wdf = pd.DataFrame([{"Component": k, "Weight": f"{v:.3f}"} for k, v in weights.items()])
                    st.dataframe(wdf, use_container_width=True, hide_index=True)
                with w2:
                    if explained:
                        st.metric("Variance Explained (PC1)", f"{explained:.1f}%")
                    st.metric("Components", len(weights))

            z_cols = [c for c in df.columns if c.startswith("z_")]
            if z_cols:
                st.subheader("Component Z-Scores")
                df_c = df[["timestamp"] + z_cols].melt(
                    id_vars="timestamp", var_name="component", value_name="zscore")
                df_c["component"] = df_c["component"].str.replace("z_", "")
                fig2 = px.line(df_c, x="timestamp", y="zscore", color="component")
                fig2.add_hline(y=0, line_dash="dash", line_color="gray")
                st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("Insufficient data for financial stress index.")

    else:  # Diffusion
        st.subheader("Diffusion Index")
        st.caption("Fraction of indicators improving month-over-month. "
                   "Below 0.5 = broad-based deterioration (analogous to CFNAI Diffusion Index).")

        data = fetch_diffusion()
        if data and data.get("data"):
            df = pd.DataFrame(data["data"])
            df["timestamp"] = pd.to_datetime(df["timestamp"])

            fig = go.Figure()
            fig.add_trace(go.Scatter(x=df["timestamp"], y=df["diffusion"],
                                      mode="lines", name="Diffusion",
                                      line=dict(color="teal", width=2)))
            fig.add_hline(y=0.5, line_dash="dash", line_color="red",
                          annotation_text="50% threshold (broad deterioration below)")
            fig.update_layout(title="Economic Diffusion Index",
                              yaxis_title="Fraction Improving", yaxis=dict(range=[0, 1]), height=400)
            st.plotly_chart(fig, use_container_width=True)

            latest = df.iloc[-1]
            c1, c2, c3 = st.columns(3)
            c1.metric("Current Diffusion", f"{latest['diffusion']:.1%}")
            c2.metric("Improving", f"{int(latest['improving'])} / {int(latest['n_indicators'])}")
            c3.metric("Deteriorating", f"{int(latest['deteriorating'])} / {int(latest['n_indicators'])}")
        else:
            st.info("Insufficient data for diffusion index.")


# ── Tab 5: Charts ────────────────────────────────────────────────────────

with tabs[4]:
    st.header("Timeseries Charts")

    indicators = fetch_indicators(category=selected_category)
    if indicators:
        # Category sub-pages with sparklines
        if selected_category:
            st.subheader(f"Category: {CATEGORY_LABELS.get(selected_category, selected_category)}")
            spark_cols = st.columns(min(len(indicators), 3))
            for i, ind in enumerate(indicators):
                with spark_cols[i % len(spark_cols)]:
                    spark = fetch_sparkline(ind["id"], n=60)
                    if spark:
                        fig = go.Figure(go.Scatter(y=spark, mode="lines",
                                                    line=dict(color="steelblue", width=1.5)))
                        fig.update_layout(height=120, margin=dict(l=0, r=0, t=25, b=0),
                                          title=dict(text=ind["name"], font=dict(size=11)),
                                          xaxis=dict(visible=False), yaxis=dict(visible=False))
                        st.plotly_chart(fig, use_container_width=True, key=f"spark_{ind['id']}")
            st.divider()

        indicator_map = {f"{ind['name']} ({ind['id']})": ind['id'] for ind in indicators}
        selected_label = st.selectbox("Select indicator for detail", list(indicator_map.keys()), key="chart_ind")
        selected_id = indicator_map[selected_label]
        sel_desc = get_description_map().get(selected_id, "")
        if sel_desc:
            st.caption(f"ℹ️ {sel_desc}")

        col1, col2 = st.columns(2)
        start_date = col1.date_input("Start", value=pd.Timestamp("2020-01-01"), key="cs")
        end_date = col2.date_input("End", value=pd.Timestamp.now(), key="ce")

        ts_data = fetch_timeseries(selected_id, start=str(start_date), end=str(end_date))
        if ts_data and ts_data.get("data"):
            df = pd.DataFrame(ts_data["data"])
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            fig = px.line(df, x="timestamp", y="value",
                          title=f"{ts_data['indicator_name']} ({ts_data['unit']})")
            fig.update_layout(xaxis_title="Date", yaxis_title=ts_data["unit"])
            st.plotly_chart(fig, use_container_width=True)

            analytics = summary_map.get(selected_id, {}) if summary_data else {}
            if analytics:
                ac1, ac2, ac3, ac4 = st.columns(4)
                pct = analytics.get("percentile")
                ac1.metric("Percentile", f"{pct:.0f}th" if pct is not None else "N/A")
                z = analytics.get("zscore")
                ac2.metric("Z-Score", f"{z:.2f}" if z is not None else "N/A")
                roc = analytics.get("roc_pct")
                ac3.metric("Rate of Change", f"{roc:+.1f}%" if roc is not None else "N/A")
                ac4.metric("Trend", analytics.get("trend", "N/A"))


# ── Tab 6: Compare ───────────────────────────────────────────────────────

with tabs[5]:
    st.header("Compare Indicators")
    all_indicators = fetch_indicators()
    if all_indicators:
        compare_map = {f"{ind['name']} ({ind['id']})": ind['id'] for ind in all_indicators}
        selected_compare = st.multiselect("Select indicators (max 5)",
                                           list(compare_map.keys()), max_selections=5)

        normalize_toggle = st.checkbox("Normalize to Z-scores", value=False)
        col1, col2 = st.columns(2)
        cmp_start = col1.date_input("Start", value=pd.Timestamp("2022-01-01"), key="cms")
        cmp_end = col2.date_input("End", value=pd.Timestamp.now(), key="cme")

        if selected_compare:
            fig = go.Figure()
            for label in selected_compare:
                ind_id = compare_map[label]
                ts = fetch_timeseries(ind_id, start=str(cmp_start), end=str(cmp_end))
                if ts and ts.get("data"):
                    df = pd.DataFrame(ts["data"])
                    df["timestamp"] = pd.to_datetime(df["timestamp"])
                    if normalize_toggle and len(df) > 1:
                        m, s = df["value"].mean(), df["value"].std()
                        if s > 0:
                            df["value"] = (df["value"] - m) / s
                    fig.add_trace(go.Scatter(x=df["timestamp"], y=df["value"],
                                              name=label, mode="lines"))

            fig.update_layout(
                title="Comparison" + (" (Z-Score)" if normalize_toggle else ""),
                xaxis_title="Date", yaxis_title="Z-Score" if normalize_toggle else "Value")
            st.plotly_chart(fig, use_container_width=True)

            # Rolling correlation
            if len(selected_compare) == 2:
                st.subheader("Rolling Correlation")
                window = st.slider("Window (trading days)", 20, 252, 60, key="rc_w")
                id1 = compare_map[selected_compare[0]]
                id2 = compare_map[selected_compare[1]]
                rc_data = fetch_rolling_corr(id1, id2, window)
                if rc_data:
                    df_rc = pd.DataFrame(rc_data)
                    df_rc["timestamp"] = pd.to_datetime(df_rc["timestamp"])
                    fig_rc = go.Figure()
                    fig_rc.add_trace(go.Scatter(x=df_rc["timestamp"], y=df_rc["rolling_corr"],
                                                 mode="lines", line=dict(color="purple")))
                    fig_rc.add_hline(y=0, line_dash="dash", line_color="gray")
                    fig_rc.update_layout(title=f"Rolling {window}-day Correlation",
                                         yaxis=dict(range=[-1, 1]), yaxis_title="Correlation")
                    st.plotly_chart(fig_rc, use_container_width=True)


# ── Tab 7: Cross-Correlation ─────────────────────────────────────────────

with tabs[6]:
    st.header("Cross-Correlation Explorer")
    st.caption("Positive lag = first indicator leads; negative = second leads.")

    all_indicators = fetch_indicators()
    if all_indicators:
        ind_map = {f"{ind['name']} ({ind['id']})": ind['id'] for ind in all_indicators}
        ind_labels = list(ind_map.keys())
        col1, col2 = st.columns(2)
        label1 = col1.selectbox("First indicator", ind_labels, key="cc1")
        label2 = col2.selectbox("Second indicator", ind_labels, index=min(1, len(ind_labels)-1), key="cc2")
        max_lag = st.slider("Max lag (months)", 6, 48, 24, key="ccl")

        cc_desc_map = get_description_map()
        cc_descs = []
        for lbl, key in [(label1, ind_map[label1]), (label2, ind_map[label2])]:
            d = cc_desc_map.get(key, "")
            if d:
                cc_descs.append(f"**{lbl}** — {d}")
        if cc_descs:
            with st.expander("About these indicators"):
                for line in cc_descs:
                    st.markdown(line)

        if st.button("Compute"):
            ind1, ind2 = ind_map[label1], ind_map[label2]
            with st.spinner("Computing..."):
                result = fetch_crosscorr(ind1, ind2, max_lag)
            if result and result.get("ccf"):
                ccf_df = pd.DataFrame(result["ccf"])
                cl = result.get("classification", {})

                rc1, rc2, rc3 = st.columns(3)
                rc1.metric("Best Lag", f"{cl.get('best_lag', 'N/A')} months")
                rc2.metric("Peak Corr.", f"{cl.get('best_corr', 'N/A')}")
                rc3.metric("Relationship", cl.get("classification", "N/A"))

                colors = ["crimson" if c is not None and abs(c) > 0.5 else "steelblue"
                          for c in ccf_df["correlation"]]
                fig = go.Figure(go.Bar(x=ccf_df["lag"], y=ccf_df["correlation"], marker_color=colors))
                fig.add_hline(y=0, line_color="black", line_width=1)
                fig.add_hline(y=0.5, line_dash="dash", line_color="gray")
                fig.add_hline(y=-0.5, line_dash="dash", line_color="gray")
                fig.update_layout(title=f"CCF: {label1} vs {label2}",
                                  xaxis_title="Lag (months)", yaxis=dict(range=[-1, 1]))
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("Insufficient data for cross-correlation.")


# ── Tab 8: Freshness ─────────────────────────────────────────────────────

with tabs[7]:
    st.header("Data Freshness")
    freshness = fetch_freshness()
    if freshness:
        st.dataframe(pd.DataFrame(freshness), use_container_width=True)
    else:
        st.info("No freshness data.")
