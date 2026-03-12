# US Macrofinance Tracker

[![CI](https://github.com/murzua7/macro-tracker/actions/workflows/ci.yml/badge.svg)](https://github.com/murzua7/macro-tracker/actions/workflows/ci.yml)

**[Live Dashboard](https://us-macro-tracker.streamlit.app/)**

Production-ready system that ingests, normalizes, stores, and serves near-real-time macroeconomic and financial indicators relevant to US economic health.

## Features

- **62 indicators** across 8 categories with hover-tooltip descriptions explaining what each measures and why it matters
- **Analytics engine**: percentiles, z-scores, rate-of-change, recession probability (Estrella-Mishkin), regime detection, composite leading index (OECD-style), financial stress index (PCA-based), diffusion index, cross-correlation
- **Plugin-style registry** (`registry.yaml`) — add indicators without code changes
- **Connectors** for FRED and Yahoo Finance with retry and rate-limit handling
- **SQLite storage** structured for easy migration to Postgres
- **FastAPI backend** — 18 endpoints for indicators, timeseries, analytics
- **Streamlit dashboard** — 8 tabs: Overview, Recession Risk, Heatmap, Composites, Charts, Compare, Cross-Correlation, Freshness
- **Automated daily ingestion** via GitHub Actions
- **Free deployment** on Streamlit Community Cloud

## Dashboard

The dashboard is live at **[us-macro-tracker.streamlit.app](https://us-macro-tracker.streamlit.app/)** and includes:

| Tab | Description |
|-----|-------------|
| Overview | Latest readings with trend arrows, rate-of-change deltas, and percentile tooltips |
| Recession Risk | Composite score from yield curve probability, Sahm Rule, NFCI, and HY OAS |
| Heatmap | Color-coded percentile/z-score table with momentum bar chart |
| Composites | Leading economic index, financial stress index, and diffusion index |
| Charts | Individual indicator timeseries with sparklines and analytics |
| Compare | Multi-indicator overlay with optional z-score normalization and rolling correlation |
| Cross-Correlation | Lead-lag analysis between any two indicators |
| Freshness | Data staleness monitor for all indicators |

## Quick Start (Local)

```bash
# Clone and install
python -m venv .venv && source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Configure
cp .env.example .env
# Edit .env with your FRED_API_KEY (get one at https://fred.stlouisfed.org/docs/api/api_key.html)

# Ingest data
PYTHONPATH=src python scripts/ingest.py

# Start API server (optional, for programmatic access)
uvicorn macro_tracker.api.main:app --port 8080

# Start dashboard
streamlit run src/macro_tracker/dashboard/app.py
```

## Indicator Categories

| Category | Count | Source | Examples |
|----------|-------|--------|----------|
| Macro Activity | 6 | FRED | GDP, Industrial Production, Retail Sales, WEI |
| Labor | 8 | FRED | Unemployment, Nonfarm Payrolls, Initial Claims, JOLTS |
| Inflation & Prices | 4 | FRED | CPI, Core PCE, PPI, 5Y Breakeven |
| Rates & Yield Curve | 9 | FRED | Fed Funds, SOFR, 2Y/5Y/10Y/30Y Treasury, Spreads, Mortgage |
| Credit Conditions | 6 | FRED | HY OAS, IG OAS, BAA Spread, NFCI, StL FSI, M2 |
| Housing | 3 | FRED | Housing Starts, Building Permits, Case-Shiller |
| Leading & Composite | 6 | FRED | LEI, Sahm Rule, Recession Prob, Consumer Sentiment, ISM |
| Markets | 20 | Yahoo Finance | S&P 500, Nasdaq 100, VIX, Gold, Oil, BTC, 8 Sector ETFs |

All 62 indicators include human-readable descriptions accessible via hover tooltips in the dashboard.

## Configuration

All secrets are managed via `.env` file (never committed):

- `FRED_API_KEY` — required for FRED data
- `DATABASE_URL` — defaults to `sqlite:///macro_data.db`
- `LOG_LEVEL` — defaults to `INFO`
- `START_DATE` — historical backfill start date

## Development

```bash
pip install -e ".[dev]"
ruff check src/ tests/
pytest tests/ -v
```
