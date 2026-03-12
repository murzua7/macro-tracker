"""Base connector with retry and rate-limit handling."""

import logging
import time
from abc import ABC, abstractmethod

from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from macro_tracker.schema import DataPoint, IndicatorSpec

logger = logging.getLogger(__name__)

# Shared retry decorator for network calls
network_retry = retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception_type((ConnectionError, TimeoutError, OSError)),
    reraise=True,
)


class BaseConnector(ABC):
    """Abstract base for all data source connectors."""

    source_name: str = "base"

    # Simple rate limiter state
    _last_call_time: float = 0.0
    _min_interval: float = 0.5  # seconds between calls

    def _rate_limit(self) -> None:
        """Enforce minimum interval between API calls."""
        elapsed = time.time() - self._last_call_time
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_call_time = time.time()

    @abstractmethod
    def fetch(
        self, spec: IndicatorSpec, start_date: str, end_date: str
    ) -> list[DataPoint]:
        """Fetch data for a single indicator. Must be implemented by subclasses."""
        ...

    def fetch_safe(
        self, spec: IndicatorSpec, start_date: str, end_date: str
    ) -> list[DataPoint]:
        """Fetch with error handling — never raises, returns empty on failure."""
        try:
            self._rate_limit()
            return self.fetch(spec, start_date, end_date)
        except Exception:
            logger.exception("Failed to fetch %s (%s)", spec.id, spec.code)
            return []
