# US Macrofinance Tracker

Production-ready system that ingests, normalizes, stores, and serves near-real-time and periodic macroeconomic and financial indicators relevant to US economic health.

## Features

- **33 indicators** across 5 domains: Macro & Labor, Rates & Credit, Markets & Commodities, Equities, Derivatives
- **Plugin-style registry** (`registry.yaml`) — add indicators without code changes
- **Connectors** for FRED and Yahoo Finance with retry and rate-limit handling
- **Canonical schema** — every data point normalized to (timestamp, indicator_id, value, unit, frequency, source)
- **SQLite storage** structured for easy migration to Postgres
- **FastAPI backend** — list indicators, fetch timeseries, get latest snapshots
- **Streamlit dashboard** — headline cards, category charts, and alerts

## Quick Start

```bash
# Clone and install
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Configure
cp .env.example .env
# Edit .env with your FRED_API_KEY

# Ingest data
python scripts/ingest.py

# Start API server
uvicorn src.macro_tracker.api.main:app --reload

# Start dashboard (in another terminal)
streamlit run src/macro_tracker/dashboard/app.py
```

## Indicator Domains

| Domain | Count | Source |
|--------|-------|--------|
| Macro & Labor | 7 | FRED |
| Rates & Credit | 8 | FRED |
| Markets & Commodities | 10 | YFinance |
| Equities / Fundamentals | 6 | YFinance |
| Derivatives / Positioning | 2 | Placeholder |

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
