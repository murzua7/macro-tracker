# Macro Tracker

Daily Python ETL for US macroeconomic indicators using FRED and Yahoo Finance,
with SQLite storage and optional GitHub repository bootstrap through `gh`.

## Tracked series

- Inflation: CPI (`CPIAUCSL`), Core PCE (`PCEPILFE`)
- Employment: Non-Farm Payrolls (`PAYEMS`), Unemployment Rate (`UNRATE`)
- Rates: Effective Fed Funds Rate (`DFF`), 10Y-2Y Treasury spread (`T10Y2Y`)
- Growth: Real GDP (`GDPC1`)
- Market proxies: S&P 500 (`^GSPC`), DXY (`DX-Y.NYB`), 10Y Treasury yield (`^TNX`)

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
printf "FRED_API_KEY=your_fred_api_key\n" > .env
python run_tracker.py
```

You can also export `FRED_API_KEY` in your shell, but `.env` keeps the secret scoped
to this project and avoids putting it in chat history.

## Daily batch usage

Run the same command every day from cron, Task Scheduler, or any job runner:

```bash
python run_tracker.py
```

The ETL computes a rolling lookback window so it can safely upsert new rows and
refresh recent percentage-change calculations without duplicating data.

## Dashboard

Launch the local dashboard with:

```bash
streamlit run dashboard.py
```

The dashboard reads from `macro_data.db` and includes:

- latest macro snapshot cards
- category-level trend charts
- single-indicator drilldowns with MoM and YoY views
- rolling correlation analysis across indicators

## GitHub bootstrap

```bash
python run_tracker.py --setup-github your-repo-name
```

This assumes `gh auth login` has already been completed successfully.
