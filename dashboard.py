"""Streamlit dashboard for visualizing macro tracker data."""

from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd
import plotly.express as px
import streamlit as st


PROJECT_ROOT = Path(__file__).resolve().parent
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from macro_tracker.analytics import INDICATOR_VIEWS, MacroAnalytics
from macro_tracker.config import MacroTrackerConfig


st.set_page_config(
    page_title="Macro Tracker Dashboard",
    page_icon="chart_with_upwards_trend",
    layout="wide",
)


@st.cache_data(show_spinner=False)
def load_data(database_path: str) -> pd.DataFrame:
    """Load cached macro data for the dashboard."""
    analytics = MacroAnalytics(database_path)
    return analytics.load_daily_frame()


def render_overview(frame: pd.DataFrame) -> None:
    """Render latest point-in-time cards for key indicators."""
    analytics = MacroAnalytics(MacroTrackerConfig().database_path)
    snapshot = analytics.latest_snapshot(frame)
    if snapshot.empty:
        st.warning("No data found in the SQLite database yet.")
        return

    st.subheader("Latest Snapshot")
    selected = snapshot[
        snapshot["label"].isin(
            [
                "CPI",
                "Core PCE",
                "Unemployment Rate",
                "Effective Fed Funds Rate",
                "10Y-2Y Treasury Spread",
                "Real GDP",
            ]
        )
    ]
    columns = st.columns(len(selected))
    for column_widget, (_, row) in zip(columns, selected.iterrows()):
        delta_parts = []
        if row["mom_column"] and row["mom_column"] in frame.columns:
            latest_delta = frame[row["mom_column"]].dropna()
            if not latest_delta.empty:
                delta_parts.append(f"MoM {latest_delta.iloc[-1]:.2f}%")
        if row["yoy_column"] and row["yoy_column"] in frame.columns:
            latest_delta = frame[row["yoy_column"]].dropna()
            if not latest_delta.empty:
                delta_parts.append(f"YoY {latest_delta.iloc[-1]:.2f}%")

        column_widget.metric(
            label=row["label"],
            value=f"{row['value']:.2f}",
            delta=" | ".join(delta_parts) if delta_parts else None,
        )


def render_category_trends(frame: pd.DataFrame) -> None:
    """Render multi-series trend charts by indicator category."""
    st.subheader("Category Trends")
    categories = sorted({indicator.category for indicator in INDICATOR_VIEWS})
    category = st.selectbox("Category", categories, index=0)

    columns = [
        indicator.column
        for indicator in INDICATOR_VIEWS
        if indicator.category == category and indicator.column in frame.columns
    ]
    if not columns:
        st.info("No columns available for the selected category.")
        return

    chart_frame = frame[["date", *columns]].dropna(how="all", subset=columns)
    melted = chart_frame.melt(id_vars="date", var_name="indicator", value_name="value")
    melted = melted.dropna(subset=["value"])

    label_map = {indicator.column: indicator.label for indicator in INDICATOR_VIEWS}
    melted["indicator"] = melted["indicator"].map(label_map)

    figure = px.line(
        melted,
        x="date",
        y="value",
        color="indicator",
        template="plotly_white",
        title=f"{category} indicators over time",
    )
    figure.update_layout(legend_title_text="")
    st.plotly_chart(figure, use_container_width=True)


def render_indicator_detail(frame: pd.DataFrame) -> None:
    """Render a focused indicator analysis section."""
    st.subheader("Indicator Detail")
    label_to_indicator = {indicator.label: indicator for indicator in INDICATOR_VIEWS}
    selected_label = st.selectbox("Indicator", list(label_to_indicator))
    indicator = label_to_indicator[selected_label]

    if indicator.column not in frame.columns:
        st.info("No data available for the selected indicator.")
        return

    detail_frame = frame[["date", indicator.column]].copy()
    series_figure = px.line(
        detail_frame.dropna(),
        x="date",
        y=indicator.column,
        template="plotly_white",
        title=selected_label,
    )
    series_figure.update_layout(yaxis_title=selected_label, xaxis_title="")
    st.plotly_chart(series_figure, use_container_width=True)

    change_columns = []
    if indicator.mom_column and indicator.mom_column in frame.columns:
        change_columns.append(indicator.mom_column)
    if indicator.yoy_column and indicator.yoy_column in frame.columns:
        change_columns.append(indicator.yoy_column)

    if change_columns:
        change_frame = frame[["date", *change_columns]].dropna(how="all", subset=change_columns)
        melted = change_frame.melt(
            id_vars="date",
            var_name="metric",
            value_name="value",
        ).dropna(subset=["value"])
        metric_labels = {
            indicator.mom_column: "MoM %",
            indicator.yoy_column: "YoY %",
        }
        melted["metric"] = melted["metric"].map(metric_labels)
        change_figure = px.bar(
            melted,
            x="date",
            y="value",
            color="metric",
            barmode="group",
            template="plotly_white",
            title=f"{selected_label} growth rates",
        )
        change_figure.update_layout(legend_title_text="")
        st.plotly_chart(change_figure, use_container_width=True)

    recent = frame[["date", indicator.column, *change_columns]].tail(30)
    st.dataframe(recent, use_container_width=True, hide_index=True)


def render_correlation(frame: pd.DataFrame) -> None:
    """Render a rolling correlation analysis for two selected indicators."""
    st.subheader("Rolling Correlation")
    label_to_indicator = {indicator.label: indicator for indicator in INDICATOR_VIEWS}
    labels = list(label_to_indicator)
    left_label = st.selectbox("Left indicator", labels, index=0, key="left_indicator")
    right_label = st.selectbox("Right indicator", labels, index=7, key="right_indicator")
    window = st.slider("Window (days)", min_value=30, max_value=365, value=90, step=30)

    left = label_to_indicator[left_label]
    right = label_to_indicator[right_label]
    if left.column not in frame.columns or right.column not in frame.columns:
        st.info("Selected indicators are not available.")
        return

    pair = frame[["date", left.column, right.column]].dropna()
    if pair.empty:
        st.info("No overlapping history for the selected pair.")
        return

    pair["rolling_correlation"] = (
        pair[left.column].rolling(window).corr(pair[right.column])
    )
    figure = px.line(
        pair.dropna(subset=["rolling_correlation"]),
        x="date",
        y="rolling_correlation",
        template="plotly_white",
        title=f"{left_label} vs {right_label}",
    )
    figure.update_layout(yaxis_title="Correlation", xaxis_title="")
    st.plotly_chart(figure, use_container_width=True)


def main() -> None:
    """Render the dashboard."""
    config = MacroTrackerConfig()
    st.title("US Macro Tracker")
    st.caption(
        "Daily ETL from FRED and Yahoo Finance with SQLite storage and indicator analytics."
    )

    frame = load_data(str(config.database_path))
    if frame.empty:
        st.error(
            "No macro data was found. Run `python run_tracker.py` first to populate "
            "the SQLite database."
        )
        return

    render_overview(frame)
    st.divider()
    render_category_trends(frame)
    st.divider()
    render_indicator_detail(frame)
    st.divider()
    render_correlation(frame)


if __name__ == "__main__":
    main()
