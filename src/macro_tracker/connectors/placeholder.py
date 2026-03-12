"""Placeholder connector for derivatives/positioning data.

Free real-time data for options volume and gamma exposure is limited.
This connector provides the schema and interface so real implementations
can be swapped in when a data source becomes available.
"""

import logging

from macro_tracker.connectors.base import BaseConnector
from macro_tracker.schema import DataPoint, IndicatorSpec

logger = logging.getLogger(__name__)


class PlaceholderConnector(BaseConnector):
    source_name = "placeholder"

    def fetch(
        self, spec: IndicatorSpec, start_date: str, end_date: str
    ) -> list[DataPoint]:
        logger.info(
            "Placeholder connector for %s (%s) — no free data source available. "
            "Implement a real connector when a data feed is available.",
            spec.id,
            spec.code,
        )
        return []
