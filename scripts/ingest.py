#!/usr/bin/env python3
"""CLI entry point for scheduled data ingestion."""

import argparse
import logging
import sys
from pathlib import Path

# Ensure src is on the path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from macro_tracker.config import LOG_LEVEL, START_DATE
from macro_tracker.ingestion import ingest_all, ingest_indicator


def main():
    parser = argparse.ArgumentParser(description="US Macrofinance Tracker - Data Ingestion")
    parser.add_argument("--start", default=START_DATE, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", default=None, help="End date (YYYY-MM-DD), defaults to today")
    parser.add_argument(
        "--indicator", default=None, help="Ingest a single indicator by ID"
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    if args.indicator:
        count = ingest_indicator(args.indicator, start_date=args.start, end_date=args.end)
        print(f"Ingested {count} data points for {args.indicator}")
    else:
        count = ingest_all(start_date=args.start, end_date=args.end)
        print(f"Ingestion complete. Total data points: {count}")


if __name__ == "__main__":
    main()
