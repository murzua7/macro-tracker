"""Daily ETL entry point for a US macroeconomic tracking system.

This script orchestrates a production-style extract/transform/load pipeline:
1. Extract macroeconomic series from FRED and market proxies from Yahoo Finance.
2. Transform mixed-frequency series into a daily, analytics-ready dataset.
3. Load the result into SQLite with upsert semantics for daily batch execution.

Environment variables:
- FRED_API_KEY: API key used by fredapi.
- MACRO_DB_PATH: optional SQLite path, defaults to macro_data.db.
- MACRO_LOG_PATH: optional log file path, defaults to macro_tracker.log.
- MACRO_START_DATE: optional initial historical backfill start date.
- MACRO_LOOKBACK_DAYS: optional incremental refresh buffer, defaults to 400.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
import sys

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from macro_tracker.config import MacroTrackerConfig
from macro_tracker.github import setup_github_repo
from macro_tracker.tracker import MacroTracker


def configure_logging(log_path: Path) -> None:
    """Configure console and file logging for the ETL pipeline."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[
            logging.FileHandler(log_path),
            logging.StreamHandler(),
        ],
    )


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Run the macro tracker ETL.")
    parser.add_argument(
        "--setup-github",
        dest="repo_name",
        help="Optional GitHub repository name for one-time project bootstrap.",
    )
    return parser.parse_args()


def main() -> None:
    """Run the ETL job and optionally initialize a GitHub repository."""
    args = parse_args()
    load_dotenv(PROJECT_ROOT / ".env")
    config = MacroTrackerConfig()
    configure_logging(config.log_path)

    tracker = MacroTracker(config=config)
    tracker.run_daily_update()

    if args.repo_name:
        setup_github_repo(args.repo_name, repo_path=PROJECT_ROOT)


if __name__ == "__main__":
    main()
