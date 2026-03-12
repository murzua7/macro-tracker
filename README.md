# US Macrofinance Tracker

Production-ready system that ingests, normalizes, stores, and serves near-real-time and periodic macroeconomic and financial indicators relevant to US economic health.

## Features

- **68 indicators** across 8 categories: Macro Activity, Labor, Inflation & Prices, Rates & Yield Curve, Credit Conditions, Housing, Leading & Composite, Markets
- **Analytics engine**: percentiles, z-scores, rate-of-change, recession probability (Estrella-Mishkin), regime detection, composite leading index (OECD-style), financial stress index (PCA-based), diffusion index, cross-correlation
- **Plugin-style registry** (`registry.yaml`) — add indicators without code changes
- **Connectors** for FRED and Yahoo Finance with retry and rate-limit handling
- **SQLite storage** structured for easy migration to Postgres
- **FastAPI backend** — 18 endpoints for indicators, timeseries, analytics
- **Streamlit dashboard** — 8 tabs: Overview, Recession Risk, Heatmap, Composites, Charts, Compare, Cross-Correlation, Freshness
- **Automated daily ingestion** via GitHub Actions
- **Free deployment** on Streamlit Community Cloud

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
| Macro Activity | 6 | FRED | GDP, Industrial Production, Retail Sales |
| Labor | 8 | FRED | Unemployment, Nonfarm Payrolls, Initial Claims, Sahm Rule |
| Inflation & Prices | 4 | FRED | CPI, PCE, PPI, 5Y Breakeven |
| Rates & Yield Curve | 9 | FRED | Fed Funds, 2Y/10Y/30Y Treasury, Spreads, SOFR, Mortgage |
| Credit Conditions | 6 | FRED | HY OAS, IG OAS, NFCI, StL FSI, M2 |
| Housing | 3 | FRED | Building Permits, Case-Shiller, Mortgage Rate |
| Leading & Composite | 6 | FRED | LEI, Sahm Rule, Recession Prob, Consumer Sentiment, ISM |
| Markets | 20 | YFinance | S&P 500, VIX, Gold, Oil, BTC, 8 Sector ETFs |

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
