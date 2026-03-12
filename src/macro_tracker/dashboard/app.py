"""Streamlit dashboard consuming the FastAPI backend."""

import httpx
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

API_BASE = "http://localhost:8000"

st.set_page_config(page_title="US Macrofinance Tracker", layout="wide")
st.title("US Macrofinance Tracker")


@st.cache_data(ttl=300)
def fetch_indicators(category=None, source=None):
    params = {}
    if category:
        params["category"] = category
    if source:
        params["source"] = source
    try:
        r = httpx.get(f"{API_BASE}/indicators", params=params, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        st.error(f"Failed to fetch indicators: {e}")
        return []


@st.cache_data(ttl=300)
def fetch_snapshot(category=None):
    params = {}
    if category:
        params["category"] = category
    try:
        r = httpx.get(f"{API_BASE}/snapshot", params=params, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        st.error(f"Failed to fetch snapshot: {e}")
        return []


@st.cache_data(ttl=300)
def fetch_timeseries(indicator_id, start=None, end=None):
    params = {}
    if start:
        params["start"] = start
    if end:
        params["end"] = end
    try:
        r = httpx.get(f"{API_BASE}/timeseries/{indicator_id}", params=params, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        st.error(f"Failed to fetch timeseries: {e}")
        return None


@st.cache_data(ttl=300)
def fetch_freshness():
    try:
        r = httpx.get(f"{API_BASE}/freshness", timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        st.error(f"Failed to fetch freshness: {e}")
        return []


# ── Sidebar ─────────────────────────────────────────────────────────
st.sidebar.header("Filters")

CATEGORIES = {
    "All": None,
    "Macro & Labor": "macro_labor",
    "Rates & Credit": "rates_credit",
    "Markets & Commodities": "markets_commodities",
    "Equities": "equities",
    "Derivatives": "derivatives",
}

selected_cat_label = st.sidebar.selectbox("Category", list(CATEGORIES.keys()))
selected_category = CATEGORIES[selected_cat_label]

# ── Headline Snapshot Cards ─────────────────────────────────────────
st.header("Latest Readings")

snapshot = fetch_snapshot(category=selected_category)

if snapshot:
    cols = st.columns(min(len(snapshot), 4))
    for i, entry in enumerate(snapshot):
        col = cols[i % len(cols)]
        val = entry["latest_value"]
        display_val = f"{val:,.2f}" if val is not None else "N/A"
        col.metric(
            label=entry["indicator_name"],
            value=display_val,
            help=(
                f"{entry['indicator_id']} | {entry['unit']}"
                f" | {entry['latest_timestamp'] or 'N/A'}"
            ),
        )
else:
    st.info("No data available. Run ingestion first: `python scripts/ingest.py`")

# ── Category Charts ─────────────────────────────────────────────────
st.header("Timeseries Charts")

indicators = fetch_indicators(category=selected_category)
if indicators:
    indicator_map = {f"{ind['name']} ({ind['id']})": ind['id'] for ind in indicators}
    selected_label = st.selectbox("Select indicator", list(indicator_map.keys()))
    selected_id = indicator_map[selected_label]

    col1, col2 = st.columns(2)
    start_date = col1.date_input("Start date", value=pd.Timestamp("2022-01-01"))
    end_date = col2.date_input("End date", value=pd.Timestamp.now())

    ts_data = fetch_timeseries(selected_id, start=str(start_date), end=str(end_date))

    if ts_data and ts_data.get("data"):
        df = pd.DataFrame(ts_data["data"])
        df["timestamp"] = pd.to_datetime(df["timestamp"])

        fig = px.line(
            df,
            x="timestamp",
            y="value",
            title=f"{ts_data['indicator_name']} ({ts_data['unit']})",
        )
        fig.update_layout(xaxis_title="Date", yaxis_title=ts_data["unit"])
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No timeseries data available for this indicator.")

# ── Multi-indicator Comparison ──────────────────────────────────────
st.header("Compare Indicators")

all_indicators = fetch_indicators()
if all_indicators:
    compare_map = {f"{ind['name']} ({ind['id']})": ind['id'] for ind in all_indicators}
    selected_compare = st.multiselect(
        "Select indicators to compare (max 5)",
        list(compare_map.keys()),
        max_selections=5,
    )

    if selected_compare:
        fig = go.Figure()
        for label in selected_compare:
            ind_id = compare_map[label]
            ts = fetch_timeseries(ind_id, start=str(start_date), end=str(end_date))
            if ts and ts.get("data"):
                df = pd.DataFrame(ts["data"])
                df["timestamp"] = pd.to_datetime(df["timestamp"])
                fig.add_trace(go.Scatter(
                    x=df["timestamp"], y=df["value"], name=label, mode="lines"
                ))

        fig.update_layout(title="Indicator Comparison", xaxis_title="Date", yaxis_title="Value")
        st.plotly_chart(fig, use_container_width=True)

# ── Data Freshness ──────────────────────────────────────────────────
st.header("Data Freshness")

freshness = fetch_freshness()
if freshness:
    df_fresh = pd.DataFrame(freshness)
    st.dataframe(df_fresh, use_container_width=True)
else:
    st.info("No freshness data available.")
