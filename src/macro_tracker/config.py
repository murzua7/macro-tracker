"""Application configuration loaded from environment."""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent.parent

FRED_API_KEY: str = os.getenv("FRED_API_KEY", "")
DATABASE_URL: str = os.getenv("DATABASE_URL", f"sqlite:///{BASE_DIR / 'macro_data.db'}")
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
START_DATE: str = os.getenv("START_DATE", "2020-01-01")
REGISTRY_PATH: Path = BASE_DIR / "registry.yaml"


def get_sqlite_path() -> str:
    """Extract the file path from a sqlite:/// URL."""
    if DATABASE_URL.startswith("sqlite:///"):
        return DATABASE_URL[len("sqlite:///"):]
    return "macro_data.db"
